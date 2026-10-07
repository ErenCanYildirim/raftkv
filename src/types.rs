//! Shared types: replicated log entries, Raft RPC messages and client operations.

use serde::{Deserialize, Serialize};

/// Identifier of a cluster member.
pub type NodeId = u64;

/// A state-machine command stored in the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    /// Appended by every new leader so it can commit entries left over from
    /// earlier terms (Raft paper, section 5.4.2). Has no effect on the data.
    Noop,
    /// Store `value` under `key`.
    Set {
        /// Key to write.
        key: String,
        /// Value to store.
        value: String,
    },
    /// Remove `key`.
    Del {
        /// Key to remove.
        key: String,
    },
}

/// One entry of the replicated log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    /// Term of the leader that created the entry.
    pub term: u64,
    /// The command to apply once the entry is committed.
    pub command: Command,
}

/// A Raft RPC request sent from one node to another.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcRequest {
    /// Sent by candidates to gather votes.
    RequestVote {
        /// Candidate's term.
        term: u64,
        /// Candidate requesting the vote.
        candidate_id: NodeId,
        /// Index of the candidate's last log entry.
        last_log_index: u64,
        /// Term of the candidate's last log entry.
        last_log_term: u64,
    },
    /// Sent by the leader to replicate entries; empty `entries` is a heartbeat.
    AppendEntries {
        /// Leader's term.
        term: u64,
        /// Leader's id, so followers can redirect clients.
        leader_id: NodeId,
        /// Index of the entry immediately preceding the new ones.
        prev_log_index: u64,
        /// Term of the entry at `prev_log_index`.
        prev_log_term: u64,
        /// Entries to store (possibly empty).
        entries: Vec<Entry>,
        /// Leader's commit index.
        leader_commit: u64,
        /// Leader-local sequence number, echoed back in the response. Used to
        /// confirm leadership for linearizable reads (`ReadIndex`); not part of
        /// Raft's safety rules.
        seq: u64,
    },
}

/// First frame on every node-to-node connection, identifying the sender.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// Fingerprint of the sender's member list; must match the receiver's.
    pub cluster_id: String,
    /// The sender's node id; must be a member other than the receiver.
    pub from: NodeId,
}

/// The receiver's answer to a [`Hello`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum HelloReply {
    /// The connection may be used for RPCs.
    Accepted,
    /// The connection is refused and will be closed.
    Rejected {
        /// Why the sender was refused.
        reason: String,
    },
}

/// A response to an [`RpcRequest`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcResponse {
    /// Answer to [`RpcRequest::RequestVote`].
    Vote {
        /// Responder's current term.
        term: u64,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// Answer to [`RpcRequest::AppendEntries`].
    Append {
        /// Responder's current term.
        term: u64,
        /// Whether the entries matched and were stored.
        success: bool,
        /// On success: index of the last entry known to match the leader.
        match_index: u64,
        /// On failure: the index the leader should retry from.
        conflict_index: u64,
        /// Echo of the request's `seq`.
        seq: u64,
    },
}

/// An operation requested by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOp {
    /// Linearizable read of `key`.
    Get {
        /// Key to read.
        key: String,
    },
    /// Write `value` under `key`.
    Set {
        /// Key to write.
        key: String,
        /// Value to store.
        value: String,
    },
    /// Delete `key`.
    Del {
        /// Key to delete.
        key: String,
    },
    /// This node's local view of the cluster (not linearizable).
    Status,
}

/// The answer sent back to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientReply {
    /// The write was committed.
    Ok,
    /// Result of a read: the value, or `None` if the key does not exist.
    Value(Option<String>),
    /// Result of a delete: whether the key existed.
    Deleted(bool),
    /// This node is not the leader; retry at the given client address.
    Redirect(String),
    /// Local status line.
    Status(String),
    /// The request failed.
    Error(String),
}
