# raftkv

A distributed, linearizable key-value store implemented from scratch in Java, built on a working implementation of the **Raft consensus algorithm**. No consensus libraries, no embedded databases — leader election, log replication, write-ahead logging, and snapshotting are all hand-written and independently verified.

This project was built as a deep dive into distributed systems fundamentals. Every component — RPC framing, persistence, log reconciliation — was implemented, tested, and debugged against real multi-process failure scenarios.

```
PUT foo bar  → OK
GET foo      → bar
[kill the leader process]
GET foo      → bar   (served by the newly elected leader, zero data loss)
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                          │
│              Interactive CLI (App.java)                      │
└───────────────────────────────┬──────────────────────────────┘
                                │ submitCommand()
┌───────────────────────────────▼──────────────────────────────┐
│                      RAFT CONSENSUS LAYER                    │
│                                                              │
│   RaftNode  — election, replication, commit logic            │
│   RaftState — term / votedFor / log, offset-aware after      │
│               snapshot compaction                            │
│   PeerReplicationState — per-follower nextIndex / matchIndex  │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                 NETWORK LAYER (Netty)                        │
│   RpcServer / RpcClient — length-field-framed JSON RPC        │
│   RequestVote + AppendEntries, the only two RPCs in Raft      │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│                    PERSISTENCE LAYER                          │
│   WriteAheadLog  — fsync'd, append-only, crash-safe replay    │
│   SnapshotStore  — atomic-rename snapshot + log compaction    │
└────────────────────────────────────────────────────────────────┘
```

**Package layout:**

```
src/main/java/com/raftkv/
├── raft/           RaftNode, RaftState, LogEntry, PeerReplicationState, NotLeaderException
├── network/        RpcServer, RpcClient, JsonCodec, RequestVote*/AppendEntries* messages
├── storage/        WriteAheadLog, SnapshotStore, WalRecord, Snapshot
├── statemachine/   KeyValueStateMachine (the actual HashMap)
├── config/         NodeConfig, PeerConfig
└── App.java        Node launcher + interactive CLI (client logic lives here,
                    not in a separate package — see "Known limitations")
```

---

## How it works

A 3-node cluster elects exactly one **leader** via randomized election timeouts (150–300ms), which prevents split votes. All client writes go through the leader, get appended to its log, and are replicated to followers via `AppendEntries`. Once a **majority** of nodes confirm an entry (tracked per-follower via `nextIndex`/`matchIndex`), it's committed and applied to the key-value store.

Reads (`GET`) are implemented as log entries too — **linearizable reads through the log** — guaranteeing no stale data is ever returned, at the cost of paying full replication latency per read. (See "Known limitations" below for the planned optimization.)

If the leader crashes, surviving nodes detect the missing heartbeat and elect a new leader automatically — no manual intervention, no data loss for any write that was already committed.

Every term change, vote, and log entry is written to a **Write-Ahead Log with fsync before acknowledgment** — meaning a write is only considered durable once it's physically on disk, not just in an OS buffer. Periodically, the state machine is snapshotted to disk and the WAL is compacted, so the log doesn't grow unboundedly.

---

## Verified behaviors

These weren't just implemented — they were tested against real running processes and confirmed via log output:

✅ **Leader election** — 3 independent JVM processes correctly elect exactly one leader, with randomized timeouts preventing split votes.

✅ **Failover** — killing the leader process (`Ctrl+C`, simulating a crash) results in automatic re-election within ~20ms of the election timeout firing. Cluster remains available throughout.

✅ **Replication correctness** — `PUT foo bar` on the leader, followed by killing the leader, followed by `GET foo` on the *newly elected* leader, correctly returns `bar`. The write survived the leader's death because it had already reached a majority before the crash.

✅ **Crash recovery (WAL)** — killing **all three** node processes simultaneously, then restarting all three, correctly restores `currentTerm`, `votedFor`, and the full log from disk. Data written before the total shutdown is fully intact after restart.

✅ **Snapshotting** — verified across three consecutive snapshot cycles (indexes 5, 10, 12 in testing), each followed by WAL truncation and a clean restart that correctly restores from `snapshot.json` + the (now much smaller) WAL.

---


## Performance

Benchmarked with JMH (JDK 21, single machine, single-node configurations where applicable):

| Benchmark | Result | What it measures |
|---|---|---|
| State machine `PUT` | ~9.3M ops/sec | Pure HashMap mutation, no consensus/disk involved |
| State machine `GET` | ~13.0M ops/sec | Pure HashMap read |
| State machine `DELETE` | ~13.5M ops/sec | Pure HashMap removal |
| WAL `append` (fsync'd) | ~355 µs/op (~2,800 ops/sec) | Cost of one durable, fsync'd disk write |
| Full commit pipeline | ~582 µs/op (~1,720 ops/sec) | `submitCommand()` → append → commit → apply, single-node |

**The honest conclusion:** disk durability dominates cost by roughly an order of magnitude over everything else. Pure in-memory state machine operations run in the millions per second; the moment a write must be durably fsync'd before acknowledgment, throughput drops to ~1,700–2,800 ops/sec — consistent with the WAL append benchmark alone accounting for the large majority of the full pipeline's latency. This is the expected, correct cost of choosing real crash-safety over speed, not an implementation inefficiency.

---

## Design decisions & tradeoffs

- **Reads go through the log (linearizable reads):** every `GET` is replicated and committed exactly like a write, guaranteeing it never returns stale data. The tradeoff is read latency — a future optimization would be **lease-based reads**, where the leader serves reads locally as long as it can prove via recent heartbeats that it's still the leader (the approach etcd uses).
- **Count-based snapshot trigger:** snapshots are taken every N committed entries (configurable, default 100) rather than a size- or time-based trigger. Simpler to reason about and test; a production system might prefer a byte-size threshold for more predictable WAL growth.
- **Single tagged `WalRecord` class** instead of three separate record types: keeps the JSON format and deserialization logic simple, at a small cost of unused fields per record type. Fewer moving parts in the code path responsible for durability felt like the right tradeoff.
- **`nextIndex` backoff by exactly 1 on conflict:** the simplest version proven correct in the original Raft paper. Production systems (etcd) optimize this by having the follower's rejection response include its actual last log index, letting the leader skip directly to the right point rather than retrying one entry at a time.
- **Lombok used selectively:** applied only to classes that are pure data carriers (`LogEntry`, RPC messages, `NodeConfig`, `PeerConfig`). Classes with real behavior attached to their fields (`RaftState`'s `AtomicLong`-backed term/log, anything touching the WAL) keep hand-written accessors, since Lombok's generated setters would bypass the persistence and thread-safety logic those fields require.

## Known limitations

- Linearizable-but-slow reads (see above) — lease reads not yet implemented.
- `nextIndex` backoff is one-at-a-time, not optimized for followers that are far behind.
- Snapshotting runs synchronously on the same thread that sends heartbeats, occasionally causing a brief, harmless leadership handover during a snapshot (observed directly in testing — a leader taking a snapshot can miss a heartbeat window, triggering a clean re-election). A production version would snapshot asynchronously off the consensus thread.
- No cluster membership changes (adding/removing nodes) — the peer set is fixed at startup.
- No standalone client library — the CLI in `App.java` talks directly to a `RaftNode` in-process. A real client (one that connects over the network to submit commands without being a cluster member itself) was scoped early on but not built; everything currently runs the client and the node in the same JVM process.

---

## Running it

```cmd
build.bat            # compiles + resolves dependencies
start-cluster.bat    # launches a 3-node cluster, each in its own terminal window
```

Once a leader is elected (watch for `*** Node X became LEADER ***`), type commands directly into any node's terminal:

```
PUT foo bar
GET foo
DELETE foo
```

Commands submitted to a non-leader node return a redirect:
```
>> NOT LEADER. Try node: node2
```

To test crash recovery: kill any node's terminal (`Ctrl+C`) and watch the remaining nodes elect a new leader within a few hundred milliseconds. To test full persistence: kill **all** node terminals, then run `start-cluster.bat` again — previously committed data will be restored from each node's Write-Ahead Log (and snapshot, if one was taken).

```cmd
reset-data.bat       # wipes all WAL/snapshot data for a clean slate
```

### Running tests

```cmd
mvn test
```

29 tests covering Raft state mutation (including snapshot-offset arithmetic), WAL crash-recovery (including a simulated corrupt final line from a mid-write crash), snapshot atomic-replace semantics, state machine command parsing, and a dedicated regression test for the constructor argument-swap bug described above.

### Running benchmarks

```cmd
mvn clean package -DskipTests
java -jar target\benchmarks.jar
```

---

## Tech stack

Java 21 · Maven · Netty (RPC transport) · Jackson (JSON serialization) · SLF4J + Logback (logging) · JUnit 5 (testing) · JMH (benchmarking) · Lombok (selectively, for pure data classes)
