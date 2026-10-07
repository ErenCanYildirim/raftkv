// wire a node together, storage: Raft core thread, peer connections, two listeners

use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;
use std::io;
use std::path::PathBuf;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::client_api::serve_clients;
use crate::raft::{Raft, RaftConfig};
use crate::storage::Storage;
use crate::transport::{peer_loop, serve_rpc, ClusterIdentity};
use crate::types::NodeId;

/// Capacity of each per-peer outgoing queue. When full, new messages are
/// dropped (Raft retries through heartbeats).
const OUTBOX_CAPACITY: usize = 64;

/// The two addresses of a cluster member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAddrs {
    /// Address for node-to-node Raft traffic.
    pub raft: String,
    /// Address clients connect to.
    pub client: String,
}

/// Configuration of one node.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This node's id; must be a key of `members`.
    pub id: NodeId,
    /// Every member of the cluster, including this node.
    pub members: BTreeMap<NodeId, NodeAddrs>,
    /// Where this node keeps its durable state.
    pub data_dir: PathBuf,
}

/// Parse a member spec of the form `ID=RAFT_ADDR,CLIENT_ADDR`,
pub fn parse_member(spec: &str) -> Result<(NodeId, NodeAddrs), String> {
    let usage = || format!("invalid member '{spec}', expected ID=RAFT_ADDR,CLIENT_ADDR");

    let (id, addrs) = spec.split_once('=').ok_or_else(usage)?;
    let id: NodeId = id.trim().parse().map_err(|_| usage())?;
    let (raft, client) = addrs.split_once(',').ok_or_else(usage)?;
    let (raft, client) = (raft.trim(), client.trim());
    if raft.is_empty() || client.is_empty() {
        return Err(usage());
    }
    Ok((
        id,
        NodeAddrs {
            raft: raft.to_string(),
            client: client.to_string(),
        },
    ))
}

/// A fingerprint of the member list. Nodes only talk to peers with the same
/// fingerprint, so nodes started with different member lists (which could
/// disagree about what a majority is) refuse each other instead of
/// silently forming an unsafe cluster.
#[must_use]
pub fn cluster_fingerprint(members: &BTreeMap<NodeId, NodeAddrs>) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut canonical = String::new();
    for (id, addrs) in members {
        let _ = write!(canonical, "{id}={},{};", addrs.raft, addrs.client);
    }
    let hash = canonical.bytes().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    });
    format!("{hash:016x}")
}

/// Run a node until the process is stopped.
/// Fails if the configuration is invalid, the data directory cannot be
/// opened, or the listeners cannot be bound.
pub async fn run(config: NodeConfig) -> io::Result<()> {
    let id = config.id;
    let me = config.members.get(&id).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("node {id} is not in the member list"),
        )
    })?;

    let (storage, state) = Storage::open(&config.data_dir).map_err(|e| {
        io::Error::new(
            e.kind(),
            format!("opening data dir {}: {e}", config.data_dir.display()),
        )
    })?;
    let raft_listener = bind(&me.raft).await?;
    let client_listener = bind(&me.client).await?;

    let identity = ClusterIdentity {
        cluster_id: cluster_fingerprint(&config.members),
        self_id: id,
        members: config.members.keys().copied().collect(),
    };

    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let mut outboxes = HashMap::new();
    for (&peer, addrs) in &config.members {
        if peer == id {
            continue;
        }
        let (tx, rx) = mpsc::channel(OUTBOX_CAPACITY);
        outboxes.insert(peer, tx);

        tokio::spawn(peer_loop(
            peer,
            addrs.raft.clone(),
            identity.hello(),
            rx,
            events_tx.clone(),
        ));
    }

    let raft = Raft::new(
        RaftConfig {
            id,
            peers: config.members.keys().copied().filter(|&p| p != id).collect(),
            client_addrs: config
                .members
                .iter()
                .map(|(&peer, addrs)| (peer, addrs.client.clone()))
                .collect(),
        },
        storage,
        state,
        outboxes,
    );

        std::thread::Builder::new()
        .name("raft-core".to_string())
        .spawn(move || {
            if let Err(e) = raft.run(&events_rx) {
                // Without durable state the node could break Raft's guarantees,
                // so it stops instead of limping on.
                error!(error = %e, "raft core failed; exiting");
                std::process::exit(1);
            }
        })?;
 
    info!(
        id,
        raft = %me.raft,
        client = %me.client,
        cluster = %identity.cluster_id,
        "node listening"
    );
    tokio::join!(
        serve_rpc(raft_listener, events_tx.clone(), identity),
        serve_clients(client_listener, events_tx),
    );
    Ok(())
}

async fn bind(addr: &str) -> io::Result<TcpListener> {
    TcpListener::bind(addr)
        .await
        .map_err(|e| io::Error::new(e.kind(), format!("binding {addr}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
 
    #[test]
    fn parses_member_specs() {
        assert_eq!(
            parse_member("2=127.0.0.1:7002,127.0.0.1:8002"),
            Ok((
                2,
                NodeAddrs {
                    raft: "127.0.0.1:7002".into(),
                    client: "127.0.0.1:8002".into()
                }
            ))
        );
        assert!(parse_member("x=a,b").is_err());
        assert!(parse_member("1=a").is_err());
        assert!(parse_member("1=a,").is_err());
        assert!(parse_member("127.0.0.1:7001").is_err());
    }
 
    fn members(specs: &[&str]) -> BTreeMap<NodeId, NodeAddrs> {
        specs
            .iter()
            .map(|spec| parse_member(spec).expect("valid spec"))
            .collect()
    }
 
    #[test]
    fn fingerprint_depends_on_the_whole_member_list() {
        let base = members(&["1=a:1,a:2", "2=b:1,b:2"]);
        let same = members(&["2=b:1,b:2", "1=a:1,a:2"]); // order of flags is irrelevant
        let other = members(&["1=a:1,a:2", "2=b:1,b:3"]);
        let bigger = members(&["1=a:1,a:2", "2=b:1,b:2", "3=c:1,c:2"]);
        assert_eq!(cluster_fingerprint(&base), cluster_fingerprint(&same));
        assert_ne!(cluster_fingerprint(&base), cluster_fingerprint(&other));
        assert_ne!(cluster_fingerprint(&base), cluster_fingerprint(&bigger));
    }
}
