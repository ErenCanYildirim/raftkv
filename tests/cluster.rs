//! End-to-end tests: real `raftkv` processes talking over TCP, killed with
//! SIGKILL to simulate crashes.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::{Duration, Instant};

const WAIT: Duration = Duration::from_secs(15);

struct TestNode {
    id: u64,
    raft: String,
    client: String,
    dir: PathBuf,
    process: Option<Child>,
}

struct Cluster {
    root: PathBuf,
    nodes: Vec<TestNode>,
}

impl Cluster {
    fn start(size: usize) -> Self {
        Self::from_addrs(reserve_addrs(size))
    }

    /// Start a cluster whose node `i` uses `addrs[i]` as (raft, client) addresses.
    fn from_addrs(addrs: Vec<(String, String)>) -> Self {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "raftkv-it-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("create test dir");

        let nodes = addrs
            .into_iter()
            .enumerate()
            .map(|(i, (raft, client))| TestNode {
                id: i as u64 + 1,
                raft,
                client,
                dir: root.join(format!("node{}", i + 1)),
                process: None,
            })
            .collect::<Vec<_>>();
        let size = nodes.len();
        let mut cluster = Self { root, nodes };
        for i in 0..size {
            cluster.start_node(i);
        }
        cluster
    }

    fn start_node(&mut self, i: usize) {
        let members: Vec<String> = self
            .nodes
            .iter()
            .map(|n| format!("{}={},{}", n.id, n.raft, n.client))
            .collect();
        let node = &mut self.nodes[i];
        assert!(node.process.is_none(), "node {} already running", node.id);
        fs::create_dir_all(&node.dir).expect("node dir");
        let log = File::options()
            .create(true)
            .append(true)
            .open(self.root.join(format!("node{}.log", node.id)))
            .expect("log file");

        let mut command = Command::new(env!("CARGO_BIN_EXE_raftkv"));
        command
            .args(["node", "--id", &node.id.to_string(), "--data-dir"])
            .arg(&node.dir)
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(log);
        for member in &members {
            command.args(["--member", member]);
        }
        node.process = Some(command.spawn().expect("spawn node"));
    }

    fn kill(&mut self, i: usize) {
        if let Some(mut child) = self.nodes[i].process.take() {
            let _ = child.kill(); // SIGKILL: no chance to clean up
            let _ = child.wait();
        }
    }

    fn is_running(&self, i: usize) -> bool {
        self.nodes[i].process.is_some()
    }

    /// One request to one node, no redirects.
    fn request(&self, i: usize, line: &str) -> Option<String> {
        request_addr(&self.nodes[i].client, line)
    }

    fn index_of_client(&self, addr: &str) -> usize {
        self.nodes
            .iter()
            .position(|n| n.client == addr)
            .expect("redirect to a known node")
    }

    /// Send a request starting at node `start`, following redirects and
    /// retrying while there is no leader, until a definitive reply or timeout.
    fn exec_from(&self, start: usize, line: &str, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        let mut target = start;
        let mut last = String::new();
        while Instant::now() < deadline {
            if self.is_running(target) {
                match self.request(target, line) {
                    Some(reply) if reply.starts_with("REDIRECT ") => {
                        target = self.index_of_client(&reply["REDIRECT ".len()..]);
                        continue;
                    }
                    Some(reply) if reply.contains("no leader known") => last = reply,
                    Some(reply) => return Ok(reply),
                    None => last = format!("node {} unreachable", target + 1),
                }
            }
            target = (target + 1) % self.nodes.len();
            sleep(Duration::from_millis(100));
        }
        Err(format!("no definitive reply to '{line}': {last}"))
    }

    fn exec(&self, line: &str) -> String {
        let start = (0..self.nodes.len())
            .find(|&i| self.is_running(i))
            .expect("some node running");
        self.exec_from(start, line, WAIT)
            .expect("cluster should answer")
    }

    fn status(&self, i: usize) -> Option<HashMap<String, String>> {
        let reply = self.request(i, "STATUS")?;
        Some(
            reply
                .strip_prefix("STATUS ")?
                .split_whitespace()
                .filter_map(|pair| pair.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        )
    }

    /// Wait until exactly one running node reports itself leader; return its index.
    fn wait_for_leader(&self) -> usize {
        wait_until("a leader", || {
            let leaders: Vec<usize> = (0..self.nodes.len())
                .filter(|&i| self.is_running(i))
                .filter(|&i| self.status(i).is_some_and(|s| s["role"] == "Leader"))
                .collect();
            (leaders.len() == 1).then(|| leaders[0])
        })
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for i in 0..self.nodes.len() {
            self.kill(i);
        }
        if !std::thread::panicking() && std::env::var_os("RAFTKV_KEEP_LOGS").is_none() {
            let _ = fs::remove_dir_all(&self.root);
        } else {
            eprintln!("node logs kept in {}", self.root.display());
        }
    }
}

/// Hand out (raft, client) address pairs on ports below the OS's ephemeral
/// range, so they never collide with ports the OS assigns to outgoing
/// connections or to other `bind(0)` calls. Unique within this test process.
fn reserve_addrs(count: usize) -> Vec<(String, String)> {
    static NEXT_PORT: AtomicUsize = AtomicUsize::new(0);
    let base = 20_000 + (std::process::id() as usize % 100) * 100;
    let mut free = Vec::new();
    while free.len() < count * 2 {
        let port = base + NEXT_PORT.fetch_add(1, Ordering::SeqCst);
        assert!(port < 32_000, "ran out of test ports");
        let addr = format!("127.0.0.1:{port}");
        if TcpListener::bind(&addr).is_ok() {
            free.push(addr);
        }
    }
    free.chunks(2)
        .map(|pair| (pair[0].clone(), pair[1].clone()))
        .collect()
}

fn wait_until<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(value) = check() {
            return value;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(100));
    }
}

#[test]
fn elects_one_leader_that_everyone_follows() {
    let cluster = Cluster::start(3);
    // Leadership may legitimately change while we watch (e.g. a heartbeat
    // delayed under load), so check convergence on *a* leader, not on the
    // first one observed.
    wait_until("all nodes to agree on one leader", || {
        let statuses: Vec<_> = (0..3).map(|i| cluster.status(i)).collect::<Option<_>>()?;
        let leader = &statuses[0]["leader"];
        let agreed = leader != "none" && statuses.iter().all(|s| &s["leader"] == leader);
        let claims = statuses.iter().filter(|s| s["role"] == "Leader").count();
        let leader_claims_it = statuses
            .iter()
            .any(|s| &s["id"] == leader && s["role"] == "Leader");
        (agreed && claims == 1 && leader_claims_it).then_some(())
    });
}

#[test]
fn writes_are_readable_through_every_node() {
    let cluster = Cluster::start(3);
    cluster.wait_for_leader();

    assert_eq!(cluster.exec("SET greeting hello raft world"), "OK");
    assert_eq!(cluster.exec("SET counter 1"), "OK");
    assert_eq!(cluster.exec("DEL counter"), "DELETED 1");
    assert_eq!(cluster.exec("DEL counter"), "DELETED 0");

    for i in 0..3 {
        assert_eq!(
            cluster.exec_from(i, "GET greeting", WAIT),
            Ok("VALUE hello raft world".to_string())
        );
        assert_eq!(
            cluster.exec_from(i, "GET counter", WAIT),
            Ok("NIL".to_string())
        );
    }
}

#[test]
fn survives_leader_crash_without_losing_acknowledged_writes() {
    let mut cluster = Cluster::start(3);
    let old_leader = cluster.wait_for_leader();
    for i in 0..20 {
        assert_eq!(cluster.exec(&format!("SET key{i} value{i}")), "OK");
    }

    cluster.kill(old_leader);
    let new_leader = cluster.wait_for_leader();
    assert_ne!(new_leader, old_leader);

    for i in 0..20 {
        assert_eq!(
            cluster.exec(&format!("GET key{i}")),
            format!("VALUE value{i}")
        );
    }
    assert_eq!(cluster.exec("SET after crash"), "OK");

    // The old leader comes back, follows whoever leads now, and catches up.
    cluster.start_node(old_leader);
    wait_until("the old leader to rejoin", || {
        cluster
            .status(old_leader)
            .is_some_and(|s| s["role"] == "Follower" && s["leader"] != "none")
            .then_some(())
    });
    assert_eq!(
        cluster.exec_from(old_leader, "GET after", WAIT),
        Ok("VALUE crash".to_string())
    );
}

#[test]
fn recovers_all_data_after_full_cluster_restart() {
    let mut cluster = Cluster::start(3);
    cluster.wait_for_leader();
    for i in 0..10 {
        assert_eq!(cluster.exec(&format!("SET k{i} v{i}")), "OK");
    }
    assert_eq!(cluster.exec("DEL k3"), "DELETED 1");

    for i in 0..3 {
        cluster.kill(i);
    }
    for i in 0..3 {
        cluster.start_node(i);
    }
    cluster.wait_for_leader();

    for i in 0..10 {
        let expected = if i == 3 {
            "NIL".to_string()
        } else {
            format!("VALUE v{i}")
        };
        assert_eq!(cluster.exec(&format!("GET k{i}")), expected);
    }
}

#[test]
fn minority_cannot_accept_writes() {
    let mut cluster = Cluster::start(3);
    cluster.wait_for_leader();
    assert_eq!(cluster.exec("SET before outage"), "OK");

    // Leave only one node running: no majority, so nothing may commit.
    cluster.kill(1);
    cluster.kill(2);
    let result = cluster.exec_from(0, "SET during outage", Duration::from_secs(3));
    assert!(
        result.is_err() || result.as_deref().is_ok_and(|r| r.starts_with("ERR")),
        "a minority must not acknowledge writes, got {result:?}"
    );
    // Reads need a majority too (they are linearizable).
    let read = cluster.exec_from(0, "GET before", Duration::from_secs(3));
    assert!(
        read.is_err() || read.as_deref().is_ok_and(|r| r.starts_with("ERR")),
        "a minority must not serve reads, got {read:?}"
    );

    // Restoring a majority restores service, with the old data intact.
    cluster.start_node(1);
    assert_eq!(cluster.exec("GET before"), "VALUE outage");
    assert_eq!(cluster.exec("SET after recovery"), "OK");
    assert_eq!(cluster.exec("GET after"), "VALUE recovery");
}

#[test]
fn lagging_follower_catches_up_and_can_complete_a_majority() {
    let mut cluster = Cluster::start(3);
    let leader = cluster.wait_for_leader();
    let lagging = (leader + 1) % 3;
    let other = (leader + 2) % 3;

    cluster.kill(lagging);
    for i in 0..300 {
        assert_eq!(cluster.exec(&format!("SET bulk{i} {i}")), "OK");
    }
    cluster.start_node(lagging);

    let leader_commit: u64 = cluster.status(leader).expect("leader status")["commit"]
        .parse()
        .expect("number");
    wait_until("the lagging follower to catch up", || {
        cluster.status(lagging).and_then(|s| {
            let applied: u64 = s["applied"].parse().ok()?;
            (applied >= leader_commit).then_some(())
        })
    });

    // Now the caught-up node is required for a majority.
    cluster.kill(other);
    assert_eq!(cluster.exec("GET bulk299"), "VALUE 299");
    assert_eq!(cluster.exec("SET final write"), "OK");
}

/// Concurrent writers while the current leader is killed again and again:
/// every write the cluster acknowledged must still be readable afterwards.
#[test]
fn acknowledged_writes_survive_repeated_leader_crashes() {
    const WRITERS: usize = 4;
    const LEADER_KILLS: usize = 6;

    let mut cluster = Cluster::start(3);
    cluster.wait_for_leader();
    let addrs: Vec<String> = cluster.nodes.iter().map(|n| n.client.clone()).collect();

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let addrs = addrs.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut acknowledged = Vec::new();
                let mut attempted = 0;
                for i in 0.. {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    attempted += 1;
                    let key = format!("w{w}-{i}");
                    let line = format!("SET {key} {i}");
                    // Errors are allowed (e.g. leadership lost mid-write): outcome unknown.
                    if exec_addrs(&addrs, w, &line, Duration::from_secs(8)).as_deref() == Ok("OK") {
                        acknowledged.push((key, i));
                    }
                }
                (attempted, acknowledged)
            })
        })
        .collect();

    // Chaos: kill whoever is leader, let the cluster fail over, bring it back.
    let mut leader_kills = 0;
    while leader_kills < LEADER_KILLS {
        sleep(Duration::from_millis(300));
        let leader = (0..3)
            .filter(|&i| cluster.is_running(i))
            .find(|&i| cluster.status(i).is_some_and(|s| s["role"] == "Leader"));
        if let Some(leader) = leader {
            cluster.kill(leader);
            leader_kills += 1;
            sleep(Duration::from_millis(900));
            cluster.start_node(leader);
        }
    }
    stop.store(true, Ordering::SeqCst);

    let mut attempted = 0;
    let mut acknowledged: Vec<(String, usize)> = Vec::new();
    for writer in writers {
        let (tried, acked) = writer.join().expect("writer thread");
        attempted += tried;
        acknowledged.extend(acked);
    }
    eprintln!(
        "chaos: {} of {attempted} writes acknowledged across {leader_kills} leader kills",
        acknowledged.len()
    );
    assert!(
        acknowledged.len() > attempted / 2,
        "too few writes succeeded: {} of {attempted}",
        acknowledged.len()
    );
    for (key, value) in acknowledged {
        assert_eq!(
            cluster.exec(&format!("GET {key}")),
            format!("VALUE {value}"),
            "acknowledged write {key} was lost"
        );
    }
}

/// Like `Cluster::exec_from`, but usable from other threads: only needs the addresses.
fn exec_addrs(
    addrs: &[String],
    start: usize,
    line: &str,
    timeout: Duration,
) -> Result<String, String> {
    let deadline = Instant::now() + timeout;
    let mut target = addrs[start % addrs.len()].clone();
    let mut next = start;
    while Instant::now() < deadline {
        match request_addr(&target, line) {
            Some(reply) if reply.starts_with("REDIRECT ") => {
                target = reply["REDIRECT ".len()..].to_string();
                continue;
            }
            Some(reply) if !reply.contains("no leader known") => return Ok(reply),
            _ => {}
        }
        next = (next + 1) % addrs.len();
        target.clone_from(&addrs[next]);
        sleep(Duration::from_millis(100));
    }
    Err(format!("no definitive reply to '{line}'"))
}

fn request_addr(addr: &str, line: &str) -> Option<String> {
    let addr: SocketAddr = addr.parse().ok()?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(500)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(8))).ok()?;
    stream.write_all(format!("{line}\n").as_bytes()).ok()?;
    let mut reply = String::new();
    BufReader::new(stream).read_line(&mut reply).ok()?;
    (!reply.is_empty()).then(|| reply.trim_end().to_string())
}

/// Raft traffic from a different cluster can reach a node, e.g. through a
/// reused address or a misconfigured peer. It must be refused, not obeyed.
#[test]
fn ignores_traffic_from_a_different_cluster() {
    let mut a = Cluster::start(3);
    let leader = a.wait_for_leader();
    for i in 0..5 {
        assert_eq!(a.exec(&format!("SET a{i} x")), "OK");
    }

    // Cluster A's leader keeps sending heartbeats to a dead follower's
    // address. Start an unrelated single-node cluster B exactly there.
    let follower = (leader + 1) % 3;
    a.kill(follower);
    let stolen_raft_addr = a.nodes[follower].raft.clone();
    let (_, b_client) = reserve_addrs(1).remove(0);
    let b = Cluster::from_addrs(vec![(stolen_raft_addr, b_client)]);
    b.wait_for_leader();
    sleep(Duration::from_secs(2)); // let A's leader keep knocking

    let status = b.status(0).expect("B status");
    assert_eq!(status["role"], "Leader", "B was taken over: {status:?}");
    assert_eq!(
        b.exec("GET a0"),
        "NIL",
        "cluster A's data leaked into cluster B"
    );
    assert_eq!(b.exec("SET mine yes"), "OK");
    assert_eq!(a.exec("GET a0"), "VALUE x", "cluster A was disturbed");
}

#[test]
fn single_node_cluster_works() {
    let cluster = Cluster::start(1);
    cluster.wait_for_leader();
    assert_eq!(cluster.exec("SET solo yes"), "OK");
    assert_eq!(cluster.exec("GET solo"), "VALUE yes");
}
