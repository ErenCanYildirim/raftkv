//! The Raft consensus core and the key-value state machine.
//!
//! The core runs on a dedicated OS thread and is the single owner of all Raft
//! state, so it needs no locks. Everything that happens to it arrives as an
//! [`Event`] on one channel: RPCs from other nodes, responses to RPCs it sent,
//! and client requests. Timeouts (elections, heartbeats) are driven by the
//! receive timeout of that channel.
//!
//! Because it runs on its own thread, the core writes to disk synchronously
//! and only replies after the write is durable, as Raft requires, without
//! blocking the async runtime that handles networking.
//!
//! Implemented: leader election, log replication with fast conflict backup,
//! the commit rule for the leader's own term (with a no-op entry per term),
//! linearizable reads via `ReadIndex`, and check-quorum (a leader that cannot
//! reach a majority steps down). Not implemented: snapshots / log compaction
//! and membership changes; see the README.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, info};

use crate::protocol::NO_LEADER;
use crate::storage::{PersistentState, Storage};
use crate::types::{ClientOp, ClientReply, Command, Entry, NodeId, RpcRequest, RpcResponse};

/// How often a leader sends heartbeats.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
/// Election timeouts are drawn uniformly from this range (milliseconds).
const ELECTION_TIMEOUT_MS: std::ops::Range<u64> = 400..800;
/// A leader steps down if it has not heard from a majority for this long.
const CHECK_QUORUM_TIMEOUT: Duration = Duration::from_millis(800);
/// Upper bound on entries per `AppendEntries` request.
const MAX_ENTRIES_PER_APPEND: usize = 256;

/// Everything the core reacts to.
#[derive(Debug)]
pub enum Event {
    /// An RPC from another node, with the channel to answer on.
    Rpc {
        /// The request.
        request: RpcRequest,
        /// Where to send the response.
        reply: oneshot::Sender<RpcResponse>,
    },
    /// A response to an RPC this node sent.
    PeerResponse {
        /// The node that answered.
        peer: NodeId,
        /// Its answer.
        response: RpcResponse,
    },
    /// A client request, with the channel to answer on.
    Client {
        /// The requested operation.
        op: ClientOp,
        /// Where to send the reply.
        reply: oneshot::Sender<ClientReply>,
    },
}

/// Static configuration of the core.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's id.
    pub id: NodeId,
    /// The other members of the cluster.
    pub peers: Vec<NodeId>,
    /// Client-facing address of every member, used for redirects.
    pub client_addrs: HashMap<NodeId, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
struct PendingWrite {
    term: u64,
    reply: oneshot::Sender<ClientReply>,
}

#[derive(Debug)]
struct PendingRead {
    /// Leadership is confirmed once a majority answered a request with at least this `seq`.
    required_seq: u64,
    /// The read may be served once this index has been applied.
    read_index: u64,
    key: String,
    reply: oneshot::Sender<ClientReply>,
}

/// The Raft node: consensus state plus the key-value state machine.
#[derive(Debug)]
pub struct Raft {
    id: NodeId,
    peers: Vec<NodeId>,
    client_addrs: HashMap<NodeId, String>,
    outboxes: HashMap<NodeId, mpsc::Sender<RpcRequest>>,
    storage: Storage,

    // Persistent state (always written to `storage` before it is relied on).
    term: u64,
    voted_for: Option<NodeId>,
    /// Log index `i` (1-based) is stored at `log[i - 1]`.
    log: Vec<Entry>,

    // Volatile state.
    commit_index: u64,
    last_applied: u64,
    role: Role,
    leader_id: Option<NodeId>,
    election_deadline: Instant,
    votes: Vec<NodeId>,

    // Leader-only state, reset by `become_leader`.
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,
    /// Index of this leader's no-op entry; reads wait until it is applied.
    term_start_index: u64,
    next_seq: u64,
    /// Highest `seq` each peer has answered in the current term.
    acked_seq: HashMap<NodeId, u64>,
    /// When each peer last answered in the current term (for check-quorum).
    last_contact: HashMap<NodeId, Instant>,
    heartbeat_deadline: Instant,
    pending_writes: BTreeMap<u64, PendingWrite>,
    pending_reads: VecDeque<PendingRead>,

    // The state machine.
    kv: HashMap<String, String>,
}

impl Raft {
    /// Build a core from its configuration and the state loaded from disk.
    #[must_use]
    pub fn new(
        config: RaftConfig,
        storage: Storage,
        state: PersistentState,
        outboxes: HashMap<NodeId, mpsc::Sender<RpcRequest>>,
    ) -> Self {
        let now = Instant::now();
        Self {
            id: config.id,
            peers: config.peers,
            client_addrs: config.client_addrs,
            outboxes,
            storage,
            term: state.term,
            voted_for: state.voted_for,
            log: state.log,
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            leader_id: None,
            election_deadline: now + random_election_timeout(),
            votes: Vec::new(),
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            term_start_index: 0,
            next_seq: 0,
            acked_seq: HashMap::new(),
            last_contact: HashMap::new(),
            heartbeat_deadline: now,
            pending_writes: BTreeMap::new(),
            pending_reads: VecDeque::new(),
            kv: HashMap::new(),
        }
    }

    /// Run the event loop until every event sender is gone.
    ///
    /// # Errors
    /// Returns an error if persisting state fails. The node must then stop:
    /// continuing without durable state could violate Raft's guarantees.
    pub fn run(mut self, events: &Receiver<Event>) -> io::Result<()> {
        info!(
            id = self.id,
            term = self.term,
            log_len = self.log.len(),
            "raft core started"
        );
        loop {
            let wait = self.next_deadline().saturating_duration_since(Instant::now());
            match events.recv_timeout(wait) {
                Ok(event) => self.handle_event(event)?,
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Ok(()),
            }
            // Checked after every event too, so a busy channel cannot starve timers.
            self.on_tick()?;
        }
    }

    // ---------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------

    fn majority(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn last_index(&self) -> u64 {
        self.log.len() as u64
    }

    /// Term of the entry at `index`; 0 for index 0 (the empty prefix).
    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            return 0;
        }
        self.log.get(to_usize(index) - 1).map_or(0, |entry| entry.term)
    }

    fn next_deadline(&self) -> Instant {
        if self.role == Role::Leader {
            self.heartbeat_deadline
        } else {
            self.election_deadline
        }
    }

    fn reset_election_timer(&mut self) {
        self.election_deadline = Instant::now() + random_election_timeout();
    }

    fn send(&self, peer: NodeId, request: RpcRequest) {
        if let Some(outbox) = self.outboxes.get(&peer)
            && let Err(e) = outbox.try_send(request)
        {
            // Raft tolerates lost messages; the next heartbeat retries.
            debug!(peer, error = %e, "dropping outgoing rpc");
        }
    }

    fn not_leader_reply(&self) -> ClientReply {
        match self.leader_id.and_then(|id| self.client_addrs.get(&id)) {
            Some(addr) => ClientReply::Redirect(addr.clone()),
            None => ClientReply::Error(NO_LEADER.to_string()),
        }
    }

    fn status_line(&self) -> String {
        let leader = self
            .leader_id
            .map_or_else(|| "none".to_string(), |id| id.to_string());
        format!(
            "id={} role={:?} term={} leader={} commit={} applied={} last_index={}",
            self.id,
            self.role,
            self.term,
            leader,
            self.commit_index,
            self.last_applied,
            self.last_index()
        )
    }

    // ---------------------------------------------------------------------
    // Role transitions
    // ---------------------------------------------------------------------

    /// Adopt a newer term seen in any message, stepping down if needed.
    fn observe_term(&mut self, term: u64) -> io::Result<()> {
        if term > self.term {
            debug!(old = self.term, new = term, "observed newer term");
            self.term = term;
            self.voted_for = None;
            self.storage.save_meta(self.term, self.voted_for)?;
            self.become_follower(None);
        }
        Ok(())
    }

    fn become_follower(&mut self, leader: Option<NodeId>) {
        if self.role == Role::Leader {
            info!(term = self.term, "stepping down as leader");
            self.fail_pending();
        }
        self.role = Role::Follower;
        self.leader_id = leader;
        self.votes.clear();
        self.reset_election_timer();
    }

    fn start_election(&mut self) -> io::Result<()> {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.storage.save_meta(self.term, self.voted_for)?;
        self.role = Role::Candidate;
        self.leader_id = None;
        self.votes = vec![self.id];
        self.reset_election_timer();
        info!(term = self.term, "starting election");

        if self.votes.len() >= self.majority() {
            return self.become_leader();
        }
        let request = RpcRequest::RequestVote {
            term: self.term,
            candidate_id: self.id,
            last_log_index: self.last_index(),
            last_log_term: self.term_at(self.last_index()),
        };
        for &peer in &self.peers {
            self.send(peer, request.clone());
        }
        Ok(())
    }

    fn become_leader(&mut self) -> io::Result<()> {
        info!(term = self.term, "became leader");
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
        self.votes.clear();

        let now = Instant::now();
        let next = self.last_index() + 1;
        self.next_index.clear();
        self.match_index.clear();
        self.acked_seq.clear();
        self.last_contact.clear();
        for &peer in &self.peers {
            self.next_index.insert(peer, next);
            self.match_index.insert(peer, 0);
            // Grace period: count every peer as reachable at the start of the term.
            self.last_contact.insert(peer, now);
        }

        self.term_start_index = self.append_local(Command::Noop)?;
        self.broadcast_append();
        self.heartbeat_deadline = now + HEARTBEAT_INTERVAL;
        self.advance_commit();
        Ok(())
    }

    /// Reply to every waiting client after losing leadership.
    fn fail_pending(&mut self) {
        for (_, write) in std::mem::take(&mut self.pending_writes) {
            let _ = write.reply.send(ClientReply::Error(
                "leadership lost before the write was confirmed; its outcome is unknown"
                    .to_string(),
            ));
        }
        for read in std::mem::take(&mut self.pending_reads) {
            let _ = read.reply.send(ClientReply::Error(NO_LEADER.to_string()));
        }
    }

    // ---------------------------------------------------------------------
    // Timers
    // ---------------------------------------------------------------------

    fn on_tick(&mut self) -> io::Result<()> {
        let now = Instant::now();
        match self.role {
            Role::Leader => {
                if now >= self.heartbeat_deadline {
                    if !self.has_recent_quorum(now) {
                        info!(term = self.term, "lost contact with a majority");
                        self.become_follower(None);
                        return Ok(());
                    }
                    self.broadcast_append();
                    self.heartbeat_deadline = now + HEARTBEAT_INTERVAL;
                }
            }
            Role::Follower | Role::Candidate => {
                if now >= self.election_deadline {
                    self.start_election()?;
                }
            }
        }
        Ok(())
    }

    fn has_recent_quorum(&self, now: Instant) -> bool {
        let reachable = self
            .peers
            .iter()
            .filter(|peer| {
                self.last_contact
                    .get(peer)
                    .is_some_and(|&at| now.duration_since(at) < CHECK_QUORUM_TIMEOUT)
            })
            .count();
        reachable + 1 >= self.majority()
    }

    // ---------------------------------------------------------------------
    // Event dispatch
    // ---------------------------------------------------------------------

    fn handle_event(&mut self, event: Event) -> io::Result<()> {
        match event {
            Event::Rpc { request, reply } => {
                let response = match request {
                    RpcRequest::RequestVote {
                        term,
                        candidate_id,
                        last_log_index,
                        last_log_term,
                    } => self.handle_request_vote(term, candidate_id, last_log_index, last_log_term)?,
                    RpcRequest::AppendEntries {
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        seq,
                    } => self.handle_append_entries(
                        term,
                        leader_id,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit,
                        seq,
                    )?,
                };
                // Sent only now, after any state change above is durable.
                let _ = reply.send(response);
            }
            Event::PeerResponse { peer, response } => match response {
                RpcResponse::Vote { term, granted } => {
                    self.handle_vote_response(peer, term, granted)?;
                }
                RpcResponse::Append {
                    term,
                    success,
                    match_index,
                    conflict_index,
                    seq,
                } => self.handle_append_response(
                    peer,
                    term,
                    success,
                    match_index,
                    conflict_index,
                    seq,
                )?,
            },
            Event::Client { op, reply } => self.handle_client(op, reply)?,
        }
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Elections
    // ---------------------------------------------------------------------

    fn handle_request_vote(
        &mut self,
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    ) -> io::Result<RpcResponse> {
        self.observe_term(term)?;

        let my_last_term = self.term_at(self.last_index());
        let candidate_up_to_date = last_log_term > my_last_term
            || (last_log_term == my_last_term && last_log_index >= self.last_index());
        let granted = term == self.term
            && self.voted_for.is_none_or(|v| v == candidate_id)
            && candidate_up_to_date;

        if granted {
            self.voted_for = Some(candidate_id);
            self.storage.save_meta(self.term, self.voted_for)?;
            self.reset_election_timer();
            debug!(term, candidate_id, "granted vote");
        }
        Ok(RpcResponse::Vote {
            term: self.term,
            granted,
        })
    }

    fn handle_vote_response(&mut self, peer: NodeId, term: u64, granted: bool) -> io::Result<()> {
        self.observe_term(term)?;
        if self.role != Role::Candidate || term != self.term || !granted {
            return Ok(());
        }
        if !self.votes.contains(&peer) {
            self.votes.push(peer);
        }
        if self.votes.len() >= self.majority() {
            self.become_leader()?;
        }
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Replication: follower side
    // ---------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)] // mirrors the fields of the RPC one to one
    fn handle_append_entries(
        &mut self,
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<Entry>,
        leader_commit: u64,
        seq: u64,
    ) -> io::Result<RpcResponse> {
        self.observe_term(term)?;
        let reject = |me: &Self, conflict_index: u64| RpcResponse::Append {
            term: me.term,
            success: false,
            match_index: 0,
            conflict_index,
            seq,
        };

        if term < self.term {
            return Ok(reject(self, 0));
        }

        // `term == self.term`: the sender is the legitimate leader of this term.
        if self.role == Role::Follower && self.leader_id == Some(leader_id) {
            self.reset_election_timer();
        } else {
            if self.role == Role::Leader {
                error!(term, leader_id, "two leaders in one term; this is a bug");
            }
            self.become_follower(Some(leader_id));
        }

        // Consistency check: our log must contain the entry before the new ones.
        if prev_log_index > self.last_index() {
            return Ok(reject(self, self.last_index() + 1));
        }
        let local_prev_term = self.term_at(prev_log_index);
        if local_prev_term != prev_log_term {
            // Skip back over the whole conflicting term in one step.
            let mut first = prev_log_index;
            while first > 1 && self.term_at(first - 1) == local_prev_term {
                first -= 1;
            }
            return Ok(reject(self, first));
        }

        // Merge: skip entries we already have, truncate on the first conflict.
        // Never truncate on a match: the request may be stale or reordered.
        let count = entries.len() as u64;
        let mut truncated = false;
        let mut first_new: Option<usize> = None;
        for (offset, entry) in (0_u64..).zip(entries) {
            let index = prev_log_index + 1 + offset;
            if index <= self.last_index() {
                if self.term_at(index) == entry.term {
                    continue;
                }
                if index <= self.commit_index {
                    return Err(io::Error::other(format!(
                        "leader tried to overwrite committed entry {index}; refusing"
                    )));
                }
                self.log.truncate(to_usize(index) - 1);
                truncated = true;
            }
            first_new.get_or_insert(self.log.len());
            self.log.push(entry);
        }
        if truncated {
            self.storage.rewrite_log(&self.log)?;
        } else if let Some(from) = first_new {
            self.storage.append(&self.log[from..])?;
        }

        let match_index = prev_log_index + count;
        let new_commit = leader_commit.min(match_index);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed();
        }

        Ok(RpcResponse::Append {
            term: self.term,
            success: true,
            match_index,
            conflict_index: 0,
            seq,
        })
    }

    // ---------------------------------------------------------------------
    // Replication: leader side
    // ---------------------------------------------------------------------

    /// Durably append a new entry to the leader's own log; returns its index.
    fn append_local(&mut self, command: Command) -> io::Result<u64> {
        let entry = Entry {
            term: self.term,
            command,
        };
        self.storage.append(std::slice::from_ref(&entry))?;
        self.log.push(entry);
        Ok(self.last_index())
    }

    fn broadcast_append(&mut self) {
        for i in 0..self.peers.len() {
            let peer = self.peers[i];
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: NodeId) {
        let next = self
            .next_index
            .get(&peer)
            .copied()
            .unwrap_or(1)
            .clamp(1, self.last_index() + 1);
        let prev_log_index = next - 1;
        let start = to_usize(prev_log_index);
        let end = (start + MAX_ENTRIES_PER_APPEND).min(self.log.len());

        self.next_seq += 1;
        let request = RpcRequest::AppendEntries {
            term: self.term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term: self.term_at(prev_log_index),
            entries: self.log[start..end].to_vec(),
            leader_commit: self.commit_index,
            seq: self.next_seq,
        };
        self.send(peer, request);
    }

    fn handle_append_response(
        &mut self,
        peer: NodeId,
        term: u64,
        success: bool,
        match_index: u64,
        conflict_index: u64,
        seq: u64,
    ) -> io::Result<()> {
        self.observe_term(term)?;
        if self.role != Role::Leader || term != self.term {
            return Ok(()); // stale response from an earlier term
        }

        // Any answer in our term confirms the peer still accepts us as leader.
        let acked = self.acked_seq.entry(peer).or_insert(0);
        *acked = (*acked).max(seq);
        self.last_contact.insert(peer, Instant::now());

        let matched = self.match_index.get(&peer).copied().unwrap_or(0);
        if success {
            // Responses can arrive out of order: only ever move forward.
            let matched = matched.max(match_index);
            self.match_index.insert(peer, matched);
            let next = self.next_index.entry(peer).or_insert(1);
            *next = (*next).max(matched + 1);
            self.advance_commit();
            if matched < self.last_index() {
                self.send_append(peer); // keep a lagging follower catching up
            }
        } else {
            // Back up, but never below what the peer is known to have.
            let next = conflict_index.max(matched + 1).min(self.last_index() + 1);
            self.next_index.insert(peer, next);
            self.send_append(peer);
        }
        self.process_reads();
        Ok(())
    }

    /// Commit the highest entry of the current term stored on a majority.
    fn advance_commit(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let majority = self.majority();
        let mut n = self.last_index();
        // Only entries from the current term are committed by counting
        // replicas (Raft paper, figure 8); earlier ones commit along with them.
        while n > self.commit_index && self.term_at(n) == self.term {
            let replicas = 1 + self
                .peers
                .iter()
                .filter(|peer| self.match_index.get(peer).copied().unwrap_or(0) >= n)
                .count();
            if replicas >= majority {
                self.commit_index = n;
                break;
            }
            n -= 1;
        }
        self.apply_committed();
        self.process_reads();
    }

    // ---------------------------------------------------------------------
    // State machine
    // ---------------------------------------------------------------------

    fn apply_committed(&mut self) {
        while self.last_applied < self.commit_index {
            let index = self.last_applied + 1;
            let entry = self.log[to_usize(index) - 1].clone();
            let result = match entry.command {
                Command::Noop => ClientReply::Ok,
                Command::Set { key, value } => {
                    self.kv.insert(key, value);
                    ClientReply::Ok
                }
                Command::Del { key } => ClientReply::Deleted(self.kv.remove(&key).is_some()),
            };
            self.last_applied = index;

            if let Some(pending) = self.pending_writes.remove(&index) {
                let reply = if pending.term == entry.term {
                    result
                } else {
                    ClientReply::Error(
                        "the write was replaced by a new leader before committing; retry"
                            .to_string(),
                    )
                };
                let _ = pending.reply.send(reply);
            }
        }
    }

    // ---------------------------------------------------------------------
    // Clients
    // ---------------------------------------------------------------------

    fn handle_client(&mut self, op: ClientOp, reply: oneshot::Sender<ClientReply>) -> io::Result<()> {
        match op {
            ClientOp::Status => {
                let _ = reply.send(ClientReply::Status(self.status_line()));
            }
            _ if self.role != Role::Leader => {
                let _ = reply.send(self.not_leader_reply());
            }
            ClientOp::Get { key } => {
                // ReadIndex: remember the commit index, confirm we are still
                // leader with one round of heartbeats, then serve the read
                // once everything up to that index is applied.
                self.pending_reads.push_back(PendingRead {
                    required_seq: self.next_seq + 1,
                    read_index: self.commit_index.max(self.term_start_index),
                    key,
                    reply,
                });
                self.broadcast_append();
                self.process_reads();
            }
            ClientOp::Set { key, value } => self.propose(Command::Set { key, value }, reply)?,
            ClientOp::Del { key } => self.propose(Command::Del { key }, reply)?,
        }
        Ok(())
    }

    fn propose(&mut self, command: Command, reply: oneshot::Sender<ClientReply>) -> io::Result<()> {
        let index = self.append_local(command)?;
        self.pending_writes.insert(
            index,
            PendingWrite {
                term: self.term,
                reply,
            },
        );
        self.broadcast_append();
        self.advance_commit();
        Ok(())
    }

    fn process_reads(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        let majority = self.majority();
        while let Some(read) = self.pending_reads.front() {
            let (required_seq, read_index) = (read.required_seq, read.read_index);
            let confirmations = 1 + self
                .peers
                .iter()
                .filter(|peer| self.acked_seq.get(peer).copied().unwrap_or(0) >= required_seq)
                .count();
            // Reads are queued in order of required_seq and read_index,
            // so the first one that is not ready blocks the rest.
            if confirmations < majority || self.last_applied < read_index {
                break;
            }
            if let Some(read) = self.pending_reads.pop_front() {
                let _ = read
                    .reply
                    .send(ClientReply::Value(self.kv.get(&read.key).cloned()));
            }
        }
    }
}

fn random_election_timeout() -> Duration {
    Duration::from_millis(rand::random_range(ELECTION_TIMEOUT_MS))
}

/// Log indexes always fit in memory, since the whole log is held in a `Vec`.
fn to_usize(index: u64) -> usize {
    usize::try_from(index).expect("log index exceeds usize; the log could not be in memory")
}