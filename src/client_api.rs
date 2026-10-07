//! The client-facing listener: reads text requests, hands them to the core,
//! //! and writes back the replies. See [`crate::protocol`] for the format.

use std::io;
use std::sync::mpsc::Sender;
use std::time::Duration;
 
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, warn};
 
use crate::protocol::{parse_request, render_reply};
use crate::raft::Event;
use crate::types::{ClientOp, ClientReply};

pub const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn serve_clients(listener: TcpListener, events: Sender<Event>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let events = events.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, events).await {
                        debug!(%addr, error = %e, "client connection closed");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "failed to accept client connection");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
 
async fn handle_connection(stream: TcpStream, events: Sender<Event>) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
 
    while let Some(line) = lines.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let reply = match parse_request(&line) {
            Ok(op) => submit(&events, op).await,
            Err(message) => ClientReply::Error(message),
        };
        let mut out = render_reply(&reply);
        out.push('\n');
        writer.write_all(out.as_bytes()).await?;
    }
    Ok(())
}

async fn submit(events: &Sender<Event>, op: ClientOp) -> ClientReply {
    let (reply, response) = oneshot::channel();
    if events.send(Event::Client { op, reply }).is_err() {
        return ClientReply::Error("node is shutting down".to_string());
    }
    match timeout(CLIENT_REQUEST_TIMEOUT, response).await {
        Ok(Ok(reply)) => reply,
        Ok(Err(_)) => ClientReply::Error("request was dropped".to_string()),
        Err(_) => ClientReply::Error(
            "timed out waiting for the cluster (is a majority of nodes up?)".to_string(),
        ),
    }
}