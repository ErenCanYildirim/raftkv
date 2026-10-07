## raftkv

A distributed key-value store in Rust, replicated with the Raft consensur algorithm. 

- A write is acknowledged after it is fsycned on the majority of nodes. 

- Reads and writes behave as if they were a single copy of the data. Reads use the Raft's ReadIndex. The leader confirms it is still the leader with one round of heartbeats before answering.

- Safe under failure: A minority can neither accept writes nor serve reads. A leader that loses contact with a majority steps down.

## Quick start

```sh
cargo build --release
B=./target/release/raftkv
M="--member 1=127.0.0.1:7001,127.0.0.1:8001 \
   --member 2=127.0.0.1:7002,127.0.0.1:8002 \
   --member 3=127.0.0.1:7003,127.0.0.1:8003"

# three nodes, each in its own terminal (or with & at the end)
$B node --id 1 --data-dir data/1 $M
$B node --id 2 --data-dir data/2 $M
$B node --id 3 --data-dir data/3 $M
```

Every node must get the same member list. Each member is `ID=RAFT_ADDR, CLIENT_ADDR`.

```sh
N=127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003
$B client --nodes $N set greeting hello world   # OK
$B client --nodes $N get greeting               # hello world
$B client --nodes $N del greeting               # 1  (0 if it did not exist)
$B client --nodes $N get greeting               # (nil)
$B client --nodes $N status                     # every node's local view
```

The client finds the leader on its own, following redirects and while an election is in progress it retries other nodes until `--timeout-secs`.

You can talk to a node directly via `nc 127.0.0.1 8001`.

| Request | Replies |
| ------- | ------- | 
| SET <key> <value> | OK |
| GET <key> | VALUE <v> or NIL | 
| DEL <key> | DELETED 1 or DELETED 0 |
| STATUS | STATUS id=.. | 
| any request on a follower | REDIRECT <leader client addr> | 
| failures | ERR <message> |

## How it works

 clients ──text──▶ client_api ─┐                      ┌─▶ peer_loop ──TCP──▶ other nodes
                               ├─▶ Raft core thread ──┤
 other nodes ──TCP──▶ serve_rpc┘    (owns all state)  └─▶ storage (fsync)

Implemented Raft aspects:

- A new leader appends a no-op entry, entries from earlier get committed. Reads wait until no-op is applied
- Only entries of the leader's own term are committed by counting replicas
- Followers never truncate entries that match, delayed or reordered messages can't erase data, overwriting a commit stops the node
- Fast log backup, a rejecting follower tells the leader where to resume, skipping a whole conflicting term at once
- Check-quorum: a leader that hasn't heard from a majority within the election timeout steps down, clients in a minority partition get an error instead of waiting forever

## Cluster identity

Every node-to-node connection starts with a handshake carrying a fingerprint of the sender's member list and its node id.

A node refuses connections from non-members, and from nodes whose member list differs from its own. Without this, stray traffic (e.g. a node of another cluster reaching a reused address) could raise a node's term or even write into its log. Nodes with mismatched member lists could disagree about what a majority is. A refused peer is logged as a warning.

## Testing

```sh
cargo test
```

## Limitations

- No snapshots, no log compaction
- Static membership, node modification requires a full restart of the cluster, no join consensus
- No client deduplication
- No auth or TLS
- Text protocol limits
- Whole dataset is in memory
- Reads go through the leader (no follower reads)