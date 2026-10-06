# Lab 3: Raft

A Raft implementation in Go, following Figure 2 of the
[Raft paper](https://raft.github.io/raft.pdf) and the MIT 6.5840 lab.

```sh
go run . 3                          # demo: election, replication, crashes
go test -race ./labs/3-raft/        # full test suite (~3 min)
go test -race -run 3D ./labs/3-raft/  # one part: 3A, 3B, 3C, or 3D
```

## Files

| File | What it is |
|------|------------|
| `raft.go` | The algorithm: election, `AppendEntries`, commit, persistence, snapshots |
| `cluster.go` | Test harness: runs N servers, checks they all apply the same log |
| `demo.go` | The `go run . 3` walkthrough |
| `raft_test.go` | Tests for parts 3A–3C, modeled on the MIT lab's |
| `snapshot_test.go` | Tests for part 3D |

Servers talk through `internal/labrpc` (the simulated network) and save
state to `internal/tester`'s `Persister` (the simulated disk).

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

**Snapshots (§7, part 3D).** The service calls `Snapshot(index, data)` once
it has saved its state up to `index`. Raft drops the log up to there and
keeps the snapshot instead. `log[0]` then stands for the snapshot's last
entry, and every index is offset by `lastIncludedIndex`. A follower so far
behind that the leader has already discarded the entries it needs gets
`InstallSnapshot` instead of `AppendEntries`. Raft passes the snapshot up to
the service on `applyCh`.

## Not implemented

- PreVote. In the demo, servers that were cut off come back with an inflated
  term and force an unnecessary election.
