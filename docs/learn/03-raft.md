# 3. Raft

Code: [`labs/3-raft/raft.go`](../../labs/3-raft/raft.go) · Run: `go run . 3` ·
Test: `go test -race ./labs/3-raft/` (`-run 3A` for one part)

Paper: [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf).
Keep **Figure 2** open while reading the code: it's the whole algorithm on
one page, and the code follows it closely.

## The idea

Several servers keep identical copies of a **log** of commands. If every
server applies the same commands in the same order, they all end up in the
same state, even though some servers crash and the network loses
messages.

One server is the **leader**. Clients send commands to it; it appends them
to its log and copies them to the **followers**. Once a **majority** has an
entry, it's **committed**: it will never be lost, and every server can
apply it.

Time is divided into numbered **terms**. Each term has at most one leader.
Whenever a server sees a higher term, it knows it's behind and becomes a
follower.

| Part | What it adds | Points |
|------|--------------|--------|
| 3A | Leader election, heartbeats | 1–4 |
| 3B | Log replication, commit | 5–9 |
| 3C | Persistence across crashes | 10 |
| 3D | Snapshots / log compaction | 11–12 |

## Point 1: the server state

```go
type Raft struct {
    mu        sync.Mutex
    peers     []*labrpc.ClientEnd   // peers[i] reaches server i
    persister *tester.Persister
    me        int
    dead      int32

    // Persistent state on all servers (saved before answering RPCs).
    currentTerm int
    votedFor    int        // -1 if none in currentTerm
    log         []LogEntry
    lastIncludedIndex int
    snapshot          []byte

    // Volatile state on all servers.
    commitIndex int
    lastApplied int

    // Volatile state on leaders, reset after each election.
    nextIndex  []int // next log index to send to each server
    matchIndex []int // highest index known replicated on each server

    state           State
    lastHeard       time.Time
    electionTimeout time.Duration
    // ...
}
```

**How it works:** the fields are grouped the way Figure 2 groups them.
**Persistent** fields must survive a crash (point 10). **Volatile** fields
can be rebuilt. One mutex guards all of them (chapter 0, section 2).

## Point 2: the ticker drives everything time-based

```go
func (rf *Raft) ticker() {
    for !rf.killed() {
        rf.mu.Lock()
        switch {
        case rf.state == Leader && time.Since(rf.lastBroadcast) >= HeartbeatInterval:
            rf.broadcastAppendEntries()
        case rf.state != Leader && time.Since(rf.lastHeard) >= rf.electionTimeout:
            rf.startElection()
        }
        rf.mu.Unlock()
        time.Sleep(tickInterval)   // 10ms
    }
}
```

**How it works:** this is the periodic loop from the lecture example. Every
10 ms it asks one question:

- **Leader:** has it been 100 ms since I last sent anything? Then send
  heartbeats, so followers know I'm alive.
- **Follower/candidate:** have I gone a whole election timeout without
  hearing from a leader? Then the leader is probably dead: start an
  election.

`switch { case cond: ... }` with no value after `switch` is Go's tidy
if/else-if chain.

```go
func (rf *Raft) resetElectionTimer() {
    rf.lastHeard = time.Now()
    spread := int64(electionTimeoutMax - electionTimeoutMin)
    rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(spread))
}
```

The timeout is **random** (300–600 ms). If every follower used the same
timeout, they'd all become candidates at once, split the vote, time out
together again, and repeat forever. With random timeouts, one usually
starts first and wins.

## Point 3: starting an election and counting votes

```go
func (rf *Raft) startElection() {
    rf.state = Candidate
    rf.currentTerm++
    rf.votedFor = rf.me
    rf.persist()
    rf.resetElectionTimer()

    term := rf.currentTerm
    args := RequestVoteArgs{
        Term:         term,
        CandidateId:  rf.me,
        LastLogIndex: rf.lastLogIndex(),
        LastLogTerm:  rf.termAt(rf.lastLogIndex()),
    }
    votes := 1 // our own; guarded by rf.mu

    for p := range rf.peers {
        if p == rf.me {
            continue
        }
        go func(p int) {
            var reply RequestVoteReply
            if !rf.peers[p].Call("Raft.RequestVote", &args, &reply) {
                return
            }
            rf.mu.Lock()
            defer rf.mu.Unlock()
            if reply.Term > rf.currentTerm {
                rf.becomeFollower(reply.Term)
                return
            }
            if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
                return
            }
            votes++
            if votes > len(rf.peers)/2 {
                rf.becomeLeader()
            }
        }(p)
    }
}
```

**How it works:**

1. Move to a new term and vote for yourself.
2. Ask every peer for a vote, **in parallel**, one goroutine each.
   `startElection` returns right away, and the ticker keeps running.
3. Each reply is handled under the lock:
   - Peer has a higher term → we're out of date; step down.
   - **Stale reply check:** if we're no longer a candidate in *this* term
     (we already won, or a newer election started), ignore it. Replies can
     arrive very late.
   - Otherwise count the vote. `votes > len/2` is a majority. The goroutine
     that gets the deciding vote calls `becomeLeader`; later replies fail
     the `rf.state != Candidate` check.

`votes` is a local variable shared by all the closures. It's safe because
it's only touched while holding `rf.mu`.

## Point 4: granting a vote (and the election restriction)

```go
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term > rf.currentTerm {
        rf.becomeFollower(args.Term)
    }
    reply.Term = rf.currentTerm
    if args.Term < rf.currentTerm {
        return // stale candidate
    }

    lastIndex := rf.lastLogIndex()
    lastTerm := rf.termAt(lastIndex)
    upToDate := args.LastLogTerm > lastTerm ||
        (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

    if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
        rf.votedFor = args.CandidateId
        rf.persist()
        reply.VoteGranted = true
        rf.resetElectionTimer()
    }
}
```

**How it works:** a server grants its vote only if both hold:

- **It hasn't voted for someone else this term.** One vote per term means
  at most one candidate can collect a majority, so at most one leader per
  term.
- **The candidate's log is at least as up-to-date as its own**: a higher
  last term, or the same last term and at least as long. This is the
  *election restriction*. A committed entry is on a majority, and any
  winning candidate needs votes from a majority. Those two majorities
  overlap in at least one server, which refuses to vote for a candidate
  missing the entry. So **a new leader always has every committed entry.**

Granting a vote also resets the election timer, so a server doesn't start
a competing election right after helping someone else.

## Point 5: the leader replicates with `nextIndex`

```go
func (rf *Raft) becomeLeader() {
    rf.state = Leader
    for i := range rf.nextIndex {
        rf.nextIndex[i] = rf.lastLogIndex() + 1   // optimistic: assume they're up to date
        rf.matchIndex[i] = 0                       // pessimistic: nothing confirmed yet
    }
    rf.matchIndex[rf.me] = rf.lastLogIndex()
    rf.broadcastAppendEntries()
}
```

```go
// replicateTo(p, term)
prev := rf.nextIndex[p] - 1
args := AppendEntriesArgs{
    Term:         term,
    LeaderId:     rf.me,
    PrevLogIndex: prev,
    PrevLogTerm:  rf.termAt(prev),
    Entries:      append([]LogEntry(nil), rf.log[prev+1-rf.lastIncludedIndex:]...),
    LeaderCommit: rf.commitIndex,
}
```

**How it works:** for each follower, the leader keeps two numbers:

- `nextIndex[p]`: where to start sending. A **guess**, which starts
  optimistic and backs up on rejection (point 7).
- `matchIndex[p]`: how far the follower is **confirmed** to match. Used to
  decide commits (point 8).

Every AppendEntries carries "here are the entries after index `prev`, and
my entry at `prev` has term `PrevLogTerm`". A heartbeat is the same message
with no new entries.

## Point 6: the follower's consistency check

```go
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    if args.Term < rf.currentTerm {
        reply.Term = rf.currentTerm
        return // stale leader
    }
    rf.becomeFollower(args.Term)
    rf.resetElectionTimer()
    reply.Term = rf.currentTerm

    // (snapshot handling skipped here, see point 12)

    if args.PrevLogIndex > rf.lastLogIndex() {
        reply.ConflictTerm = -1
        reply.ConflictIndex = rf.lastLogIndex() + 1
        return
    }
    if t := rf.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
        reply.ConflictTerm = t
        i := args.PrevLogIndex
        for i > rf.lastIncludedIndex+1 && rf.termAt(i-1) == t {
            i--
        }
        reply.ConflictIndex = i
        return
    }

    for i, e := range args.Entries {
        idx := args.PrevLogIndex + 1 + i
        if idx <= rf.lastLogIndex() {
            if rf.termAt(idx) == e.Term {
                continue
            }
            rf.log = rf.log[:idx-rf.lastIncludedIndex]
        }
        rf.log = append(rf.log, args.Entries[i:]...)
        rf.persist()
        break
    }
    reply.Success = true
    // ... update commitIndex, point 8
}
```

**How it works:** the follower accepts new entries only if its log
**contains the leader's entry at `PrevLogIndex`** (same index, same term).
Raft's *Log Matching Property* says: if two logs have an entry with the
same index and term, the logs are identical up to that point. So passing
this one check proves the whole prefix matches.

When the check passes, the append loop:

- **skips** entries the follower already has (same term at that index),
- **truncates** at the first real conflict and appends everything from
  there.

Truncate only on a real conflict. AppendEntries messages can arrive out
of order. If an old, short message arrived after a newer, longer one and
the follower blindly cut its log to the old length, it would lose entries
it had already acknowledged.

## Point 7: backing up fast after a rejection

```go
// replicateTo(), after a rejection
if rf.nextIndex[p] != args.PrevLogIndex+1 {
    return   // another reply already moved nextIndex; this one is stale
}
next := reply.ConflictIndex
if reply.ConflictTerm != -1 {
    for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
        if rf.termAt(i) == reply.ConflictTerm {
            next = i + 1
            break
        }
        if rf.termAt(i) < reply.ConflictTerm {
            break
        }
    }
}
rf.nextIndex[p] = max(1, min(next, rf.lastLogIndex()+1))
go rf.replicateTo(p, term)
```

**How it works:** the simplest version is "on rejection, `nextIndex--` and
try again", but a follower 1000 entries behind would need 1000 round trips.
Instead, the follower says *where* the mismatch is:

- `ConflictTerm = -1`: "my log is too short; it ends before
  `ConflictIndex`." Jump there.
- Otherwise: "at that index I have term `ConflictTerm`, and my entries of
  that term start at `ConflictIndex`." If the leader also has entries from
  that term, it resumes right after its last one. If not, the follower's
  whole term is wrong, so the leader skips back to `ConflictIndex`.

So the leader backs up **one term per round trip** instead of one entry.
`TestBackup3B` builds logs with hundreds of conflicting entries to check
this. Then `go rf.replicateTo(p, term)` retries right away instead of
waiting for the next heartbeat.

## Point 8: committing

```go
// leader side
func (rf *Raft) advanceCommitIndex() {
    for idx := rf.lastLogIndex(); idx > rf.commitIndex; idx-- {
        if rf.termAt(idx) != rf.currentTerm {
            break
        }
        count := 0
        for _, m := range rf.matchIndex {
            if m >= idx {
                count++
            }
        }
        if count > len(rf.peers)/2 {
            rf.commitIndex = idx
            rf.applyCond.Broadcast()
            return
        }
    }
}
```

```go
// follower side, end of AppendEntries
if args.LeaderCommit > rf.commitIndex {
    newCommit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
    if newCommit > rf.commitIndex {
        rf.commitIndex = newCommit
        rf.applyCond.Broadcast()
    }
}
```

**How it works:**

- **Leader:** find the highest index that a majority's `matchIndex` has
  reached. That entry, and everything before it, is committed.
- **The current-term rule** (`termAt(idx) != currentTerm → break`): a
  leader only commits by counting copies of entries **from its own term**.
  Older entries become committed along with them. Figure 8 of the paper
  shows how an old-term entry on a majority can still be overwritten by a
  later leader. Step 5 of the demo (`go run . 3`) shows this rule at work.
- **Follower:** learns the commit point from `LeaderCommit`. It caps it at
  the last entry *this message* confirmed matches the leader. Entries past
  that point might be stale ones from an old leader.

## Point 9: applying committed entries in order

```go
func (rf *Raft) applier() {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    for !rf.killed() {
        var msg ApplyMsg
        switch {
        case rf.snapshotPending:
            // ... point 12
        case rf.lastApplied < rf.commitIndex:
            rf.lastApplied++
            msg = ApplyMsg{
                CommandValid: true,
                Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
                CommandIndex: rf.lastApplied,
            }
        default:
            rf.applyCond.Wait()
            continue
        }
        rf.mu.Unlock()
        rf.applyCh <- msg
        rf.mu.Lock()
    }
}
```

**How it works:** one goroutine, the only one that sends on `applyCh`,
walks `lastApplied` up to `commitIndex` one entry at a time. Having a
single sender guarantees order. When there's nothing to do it sleeps on
`applyCond`, and whoever advances `commitIndex` wakes it (chapter 0,
section 5). It releases the lock during the send because the service may
call back into Raft, for example `Snapshot()`, before it reads the next
message.

## Point 10: persistence (3C)

```go
func (rf *Raft) persist() {
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    for _, v := range []any{rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.log} {
        if err := e.Encode(v); err != nil {
            log.Fatalf("persist: %v", err)
        }
    }
    rf.persister.Save(w.Bytes(), rf.snapshot)
}
```

**How it works:** `persist()` is called right after any change to
`currentTerm`, `votedFor`, or `log`, and **before replying** to the RPC
that caused it. Why these three:

- **`votedFor`**: without it, a server could vote, crash, restart, and vote
  for a different candidate in the same term. Two leaders.
- **`currentTerm`**: without it, a restarted server would go back to an
  old term and could vote in a term it already voted in.
- **`log`**: a follower that said "I have entry 5" helped commit it. If it
  forgot entry 5 after a crash, the majority holding it might no longer
  exist.

`commitIndex` and `lastApplied` aren't saved. After a restart, the leader
re-teaches the commit point.

`Make()` calls `readPersist()` to load everything back, in the same order.

## Point 11: snapshots and the shifted log (3D)

Without compaction, the log grows forever. The service (lab 4's KV store)
periodically saves its whole state and tells Raft it can drop the log up to
that point:

```go
func (rf *Raft) Snapshot(index int, snapshot []byte) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if index <= rf.lastIncludedIndex || index > rf.lastApplied {
        return
    }
    rf.compactLog(index, rf.termAt(index))
    rf.snapshot = snapshot
    rf.persist()
}
```

After compaction, slice position 0 is no longer log index 0. So all access
goes through helpers that shift by `lastIncludedIndex`:

```go
// log[0] stands for the last entry covered by the snapshot: it sits at
// index lastIncludedIndex and holds its term.
func (rf *Raft) lastLogIndex() int {
    return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
    return rf.log[index-rf.lastIncludedIndex].Term
}
```

**How it works:** log indexes stay **absolute** everywhere (in RPCs, in
`commitIndex`, in `nextIndex`). Only these helpers know about the offset.
Keeping a placeholder `log[0]` with the snapshot's last term means
`termAt(PrevLogIndex)` still works for the entry just before the first real
one, which the consistency check needs. Before any snapshot,
`lastIncludedIndex` is 0 and `log[0]` is just a dummy, so the same code
works for both cases.

## Point 12: InstallSnapshot for followers that fell too far behind (3D)

```go
// replicateTo()
if rf.nextIndex[p] <= rf.lastIncludedIndex {
    rf.sendSnapshotTo(p, term)
    return
}
```

```go
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
    // ... term checks, same as AppendEntries ...
    if args.LastIncludedIndex <= rf.commitIndex {
        return // we already have everything it covers
    }
    if args.LastIncludedIndex <= rf.lastLogIndex() &&
        rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
        rf.compactLog(args.LastIncludedIndex, args.LastIncludedTerm)  // keep the rest
    } else {
        rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
        rf.lastIncludedIndex = args.LastIncludedIndex
    }
    rf.snapshot = args.Data
    rf.commitIndex = args.LastIncludedIndex
    rf.lastApplied = args.LastIncludedIndex
    rf.persist()

    rf.snapshotPending = true
    rf.applyCond.Broadcast()
}
```

**How it works:** if the leader has already thrown away the entries a
follower needs, it sends its snapshot instead. The follower:

- ignores it if it's not newer than what it has committed (it might be
  delayed),
- keeps any log entries *after* the snapshot if they match, otherwise
  starts with an empty log,
- marks the snapshot pending. The applier delivers it to the service on
  `applyCh` (`SnapshotValid: true`) before any later entries, so the
  service replaces its state at the right moment in the sequence.

---

## Check yourself

<details>
<summary>1. Why are election timeouts random?</summary>

With equal timeouts, all followers would become candidates at the same
moment, each vote for itself, nobody would get a majority, and they'd all
time out together again: a livelock. Randomness lets one start first and
collect votes before the others wake up.
</details>

<details>
<summary>2. Why does a vote reply check <code>rf.currentTerm != term</code>?</summary>

The reply might belong to an old election. Counting it toward the current
one could let a candidate "win" with votes that were cast for a different
term.
</details>

<details>
<summary>3. A follower has entries 1–10. A delayed AppendEntries arrives with PrevLogIndex 3 and entries 4–5 (all matching). What happens to 6–10?</summary>

Nothing. The loop sees 4 and 5 already present with the same terms and
skips them; there's no conflict, so no truncation. Blindly cutting to index
5 would delete entries the follower may already have acknowledged.
</details>

<details>
<summary>4. Why can't a leader commit an entry from an older term just because it's on a majority?</summary>

That entry can still be overwritten. A server from another term with a more
up-to-date last entry could win an election and replace it (Figure 8).
Committing a current-term entry on a majority prevents that: any future
leader must have it, and the old entries come with it.
</details>

<details>
<summary>5. What could go wrong if <code>votedFor</code> weren't persisted?</summary>

A server votes for A in term 5, crashes, restarts with no memory of that
vote, and votes for B in term 5. A and B might both get a majority: two
leaders in one term.
</details>

## Try it

1. Remove the current-term check in `advanceCommitIndex`:
   `if rf.termAt(idx) != rf.currentTerm { break }`. The tests here may
   still pass, because the Figure 8 scenario is rare. Read §5.4.2 of the
   paper and explain the scenario where this breaks safety.
2. Make the election timeout fixed: replace
   `time.Duration(rand.Int63n(spread))` with `0`. Run
   `go test -run ReElection -v ./labs/3-raft/` and watch how long elections
   take, or whether they finish at all.
3. Replace the fast backup with `rf.nextIndex[p]--` and time
   `go test -run Backup3B ./labs/3-raft/` before and after.
