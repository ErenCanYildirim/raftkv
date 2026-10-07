//! Node-to-node networking.
//!
//! Messages are JSON, framed with a 4-byte big-endian length prefix. Each node
//! keeps one outgoing connection per peer, owned by a [`peer_loop`] task, and
//! serves incoming connections with [`serve_rpc`].
//!
//! Every connection starts with a [`Hello`] handshake. The receiver refuses
//! senders from a different cluster (a different member list) and senders
//! that are not members, so stray traffic (a reused address, a misconfigured
//! node) can never change its term or its log.

use std::io;
use std::sync::mpsc::Sender;
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, warn};

use crate::raft::Event;
use crate::types::{Hello, HelloReply, NodeId, RpcRequest, RpcResponse};

/// Refuse frames larger than this, so a bad peer cannot exhaust memory.
const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_millis(300);
const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(2);

/// What a node needs to know to accept or refuse incoming connections.
#[derive(Debug, Clone)]
pub struct ClusterIdentity {
    /// Fingerprint of this node's member list.
    pub cluster_id: String,
    /// This node's id.
    pub self_id: NodeId,
    /// Every member's id, including this node.
    pub members: Vec<NodeId>,
}

impl ClusterIdentity {
    /// Check a peer's handshake; `Err` carries the reason for refusing it.
    ///
    /// # Errors
    /// Returns the reason when the peer belongs to another cluster, is not a
    /// member, or claims to be this node.
    pub fn check(&self, hello: &Hello) -> Result<(), String> {
        if hello.cluster_id != self.cluster_id {
            return Err(
                "cluster mismatch: the nodes were started with different member lists".to_string(),
            );
        }
        if hello.from == self.self_id {
            return Err(format!("sender claims to be this node ({})", self.self_id));
        }
        if !self.members.contains(&hello.from) {
            return Err(format!("node {} is not a member", hello.from));
        }
        Ok(())
    }

    /// The handshake this node sends to its peers.
    #[must_use]
    pub fn hello(&self) -> Hello {
        Hello {
            cluster_id: self.cluster_id.clone(),
            from: self.self_id,
        }
    }
}

/// Write one length-prefixed JSON frame.
///
/// # Errors
/// Fails on I/O errors or if the message is too large.
pub async fn write_frame<T: Serialize>(stream: &mut TcpStream, message: &T) -> io::Result<()> {
    let body = serde_json::to_vec(message).map_err(io::Error::other)?;
    let len = u32::try_from(body.len())
        .ok()
        .filter(|&len| len <= MAX_FRAME_BYTES)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "frame too large"))?;
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    stream.write_all(&frame).await
}

/// Read one length-prefixed JSON frame.
///
/// # Errors
/// Fails on I/O errors, oversized frames or invalid JSON.
pub async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> io::Result<T> {
    let len = stream.read_u32().await?;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut body = vec![0; len as usize];
    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Accept RPC connections from peers forever, forwarding requests to the core.
pub async fn serve_rpc(listener: TcpListener, events: Sender<Event>, identity: ClusterIdentity) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let events = events.clone();
                let identity = identity.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_rpc_connection(stream, events, &identity).await {
                        debug!(%addr, error = %e, "rpc connection closed");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "failed to accept rpc connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handle_rpc_connection(
    mut stream: TcpStream,
    events: Sender<Event>,
    identity: &ClusterIdentity,
) -> io::Result<()> {
    stream.set_nodelay(true)?;

    let hello: Hello = timeout(HANDSHAKE_TIMEOUT, read_frame(&mut stream))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no handshake"))??;
    if let Err(reason) = identity.check(&hello) {
        let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_default();
        warn!(%peer, claimed_id = hello.from, %reason, "refusing rpc connection");
        write_frame(&mut stream, &HelloReply::Rejected { reason }).await?;
        return Ok(());
    }
    write_frame(&mut stream, &HelloReply::Accepted).await?;

    loop {
        let request: RpcRequest = match read_frame(&mut stream).await {
            Ok(request) => request,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        let (reply, response) = oneshot::channel();
        if events.send(Event::Rpc { request, reply }).is_err() {
            return Ok(()); // the core has stopped
        }
        let Ok(response) = response.await else {
            return Ok(());
        };
        write_frame(&mut stream, &response).await?;
    }
}

/// Deliver requests from `outbox` to one peer and feed its responses back to
/// the core. Reconnects as needed; failed requests are simply dropped, since
/// Raft retries through heartbeats.
pub async fn peer_loop(
    peer: NodeId,
    addr: String,
    hello: Hello,
    mut outbox: mpsc::Receiver<RpcRequest>,
    events: Sender<Event>,
) {
    let mut connection: Option<TcpStream> = None;
    let mut last_rejection: Option<String> = None;
    while let Some(mut request) = outbox.recv().await {
        // If requests piled up (e.g. the peer was unreachable), only the newest
        // matters: it reflects the core's current state. Raft tolerates the loss.
        while let Ok(newer) = outbox.try_recv() {
            request = newer;
        }

        if connection.is_none() {
            match connect(&addr, &hello).await {
                Ok(stream) => {
                    last_rejection = None;
                    connection = Some(stream);
                }
                Err(ConnectError::Rejected(reason)) => {
                    // Warn once per distinct reason, not on every heartbeat.
                    if last_rejection.as_deref() != Some(reason.as_str()) {
                        warn!(peer, %addr, %reason, "peer refused our connection");
                        last_rejection = Some(reason);
                    }
                    continue;
                }
                Err(ConnectError::Io(e)) => {
                    debug!(peer, %addr, error = %e, "connect failed");
                    continue;
                }
            }
        }
        let Some(stream) = connection.as_mut() else {
            continue;
        };

        match timeout(RPC_TIMEOUT, exchange(stream, &request)).await {
            Ok(Ok(response)) => {
                if events.send(Event::PeerResponse { peer, response }).is_err() {
                    return; // the core has stopped
                }
            }
            Ok(Err(e)) => {
                debug!(peer, error = %e, "rpc failed; reconnecting");
                connection = None;
            }
            Err(_) => {
                debug!(peer, "rpc timed out; reconnecting");
                connection = None;
            }
        }
    }
}

enum ConnectError {
    Io(io::Error),
    Rejected(String),
}

/// Connect to a peer and complete the handshake.
async fn connect(addr: &str, hello: &Hello) -> Result<TcpStream, ConnectError> {
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| ConnectError::Io(io::Error::new(io::ErrorKind::TimedOut, "connect timed out")))?
        .map_err(ConnectError::Io)?;
    let _ = stream.set_nodelay(true);

    let handshake = async {
        write_frame(&mut stream, hello).await?;
        read_frame::<HelloReply>(&mut stream).await
    };
    match timeout(HANDSHAKE_TIMEOUT, handshake).await {
        Ok(Ok(HelloReply::Accepted)) => Ok(stream),
        Ok(Ok(HelloReply::Rejected { reason })) => Err(ConnectError::Rejected(reason)),
        Ok(Err(e)) => Err(ConnectError::Io(e)),
        Err(_) => Err(ConnectError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "handshake timed out",
        ))),
    }
}

async fn exchange(stream: &mut TcpStream, request: &RpcRequest) -> io::Result<RpcResponse> {
    write_frame(stream, request).await?;
    read_frame(stream).await
}