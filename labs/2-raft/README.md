# Lab 2: Raft

A Raft implementation in Go, following Figure 2 of the
[Raft paper](https://raft.github.io/raft.pdf) and the MIT 6.5840 lab.

```sh
go run . 2                          # demo: election, replication, crashes
go test -race ./labs/2-raft/        # full test suite (~1 min)
go test -race -run Election ./labs/2-raft/
```

## Files

| File | What it is |
|------|------------|
| `raft.go` | The algorithm: election, `AppendEntries`, commit, persistence |
| `network.go` | Fake network: disconnect servers, drop and delay RPCs |
| `persister.go` | Fake disk that survives a crash |
| `cluster.go` | Test harness: runs N servers, checks they all apply the same log |
| `demo.go` | The `go run . 2` walkthrough |
| `raft_test.go` | Tests modeled on the MIT lab's |

## How it maps to the paper

**Leader election (§5.2).** `ticker()` is lab 1's periodic loop. A follower
that hears nothing for a random 300–600 ms calls `startElection()`: it bumps
its term, votes for itself, and sends `RequestVote` to everyone. A majority of
votes makes it leader. The randomness keeps two servers from splitting the
vote forever.

**Heartbeats.** A leader sends `AppendEntries` every 100 ms, even with nothing
to replicate. Followers reset their election timer on each one.

**Log replication (§5.3).** `Start(cmd)` appends to the leader's log. The
leader tracks `nextIndex[p]` for each follower and sends everything from
there. A follower rejects the entries if its log doesn't match at
`PrevLogIndex`. The reply then says where its log diverges
(`ConflictTerm`/`ConflictIndex`), so the leader can back up a whole term at a
time.

**Commit (§5.4).** An entry is committed once it's on a majority
(`matchIndex`). The leader only counts replicas for entries from its *own*
term; older entries commit along with them. This is the Figure 8 rule, and
step 5 of the demo shows it.

**Election restriction (§5.4.1).** A server only votes for a candidate whose
log is at least as up-to-date as its own, so a new leader already has every
committed entry.

**Persistence (§5.1).** `currentTerm`, `votedFor`, and `log` are saved before
replying to any RPC. A restarted server reloads them and catches up from the
leader.

## Locking rules

- One mutex `rf.mu` guards all of a server's state.
- Never hold it while sending an RPC or writing to `applyCh`. Both can block,
  and the other side may need your lock (deadlock).
- After any RPC returns, re-check `state` and `currentTerm` before using the
  reply. The world may have changed while you waited.

## Not implemented

- Snapshots / log compaction (6.5840 lab 3D)
- PreVote. In the demo, servers that were cut off come back with an inflated
  term and force an unnecessary election.
