# 4. Fault-tolerant key/value service

Code: [`labs/4-kvraft`](../../labs/4-kvraft), especially
[`rsm/rsm.go`](../../labs/4-kvraft/rsm/rsm.go) · Run: `go run . 4` ·
Test: `go test -race ./labs/4-kvraft/...`

## The idea

Lab 2's key/value server dies with its machine. Here, five servers each run
a copy of it, and Raft keeps them in sync. Every request goes into the Raft
log; every replica applies the log in order. As long as three of five
servers are up and connected, the service works.

The code is split in two layers:

```
KVServer            "what the service does"   Get / Put / Append, dedup
   │ Submit(op)        ▲ DoOp(op)
   ▼                   │
rsm.RSM             "how to replicate it"     Start, wait, apply in order, snapshot
   │ Start(op)         ▲ applyCh
   ▼                   │
raft.Raft           consensus (lab 3)
```

`rsm` knows nothing about keys and values, so labs 5A and 5B reuse it
unchanged.

## Point 1: the state machine interface

```go
// rsm/rsm.go
type StateMachine interface {
    DoOp(req any) any
    Snapshot() []byte
    Restore(data []byte)
}
```

```go
// server.go
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int) *KVServer {
    kv := &KVServer{
        me:      me,
        data:    map[string]string{},
        lastSeq: map[int64]int64{},
    }
    kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
    return kv
}
```

**How it works:** `KVServer` has the three methods, so it *is* a
`StateMachine` (Go interfaces are satisfied implicitly) and passes itself
into `MakeRSM`. From then on, `rsm` calls `kv.DoOp(...)` for every
committed request.

**`DoOp` must be deterministic.** Every replica calls it with the same
inputs in the same order and must reach the same state. So: no
`time.Now()`, no random numbers, and no iterating over a map when the order
affects the result.

## Point 2: an RPC handler is just "submit and wait"

```go
// server.go
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    err, result := kv.rsm.Submit(*args)
    if err != rsm.OK {
        reply.Err = ErrWrongLeader
        return
    }
    *reply = result.(GetReply)
}
```

**How it works:** the handler doesn't touch `kv.data` at all. It submits
the request, waits for `rsm` to say "committed and applied, here's the
result", and copies that into the reply. If this server isn't the leader,
the client tries another one.

Note `Submit(*args)`: it puts a **value** in the log, not a pointer.
Everything in the log gets gob-encoded (to send to followers and to save to
disk), and gob encodes the value anyway, so passing values keeps it simple.

`Get` goes through the log too. Why not read `kv.data` directly on the
leader? A leader that's been cut off from the others doesn't know it has
been replaced. It would happily return a value that the new leader has
since overwritten. A `Get` that has to commit proves this server was
leader at that point in the log.

## Point 3: `Submit`: start, then wait for *your* entry

```go
// rsm/rsm.go
func (rsm *RSM) Submit(req any) (Err, any) {
    op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}

    // Hold the lock across Start so the reader can't apply this index
    // before we've registered to wait for it.
    rsm.mu.Lock()
    index, term, isLeader := rsm.rf.Start(op)
    if !isLeader {
        rsm.mu.Unlock()
        return ErrWrongLeader, nil
    }
    w := &waiter{id: op.Id, done: make(chan result, 1)}
    if old := rsm.waiting[index]; old != nil {
        old.done <- result{err: ErrWrongLeader}
    }
    rsm.waiting[index] = w
    rsm.mu.Unlock()

    // ... wait on w.done with a timeout (chapter 0, section 4)
}
```

**How it works:**

1. Wrap the request in an `Op` with a random `Id`.
2. `rf.Start(op)` returns the log **index** the entry will occupy *if* it
   commits.
3. Register a `waiter` for that index: a channel the applier will send the
   result on.
4. Wait for the result, a leadership change, or a timeout.

**Why hold the lock across `Start`?** Without it, the entry could commit
and be applied in the gap between `Start` returning and the waiter being
registered. Nobody would be waiting, and `Submit` would wait until it timed
out.

**Why the `Id`?** Index 7 might be filled by a *different* entry: this
server was leader, accepted our op at index 7, lost leadership before it
committed, and the new leader put something else at index 7. The applier
compares ids to tell (point 4).

## Point 4: the reader applies everything, on every replica

```go
// rsm/rsm.go
func (rsm *RSM) reader() {
    for m := range rsm.applyCh {
        rsm.mu.Lock()
        switch {
        case m.SnapshotValid:
            if m.SnapshotIndex > rsm.lastApplied {
                rsm.restore(m.Snapshot)
            }
        case m.CommandValid && m.CommandIndex > rsm.lastApplied:
            rsm.lastApplied = m.CommandIndex
            op := m.Command.(Op)
            value := rsm.sm.DoOp(op.Req)

            if w := rsm.waiting[m.CommandIndex]; w != nil {
                delete(rsm.waiting, m.CommandIndex)
                if op.Me == rsm.me && op.Id == w.id {
                    w.done <- result{err: OK, value: value}
                } else {
                    w.done <- result{err: ErrWrongLeader}
                }
            }

            if rsm.maxraftstate != -1 && rsm.persister.RaftStateSize() >= rsm.maxraftstate {
                rsm.rf.Snapshot(m.CommandIndex, rsm.snapshot())
            }
        }
        rsm.mu.Unlock()
    }
}
```

**How it works:** one goroutine per server reads `applyCh` forever
(`for m := range ch` loops until the channel is closed).

- **Every replica applies every op**, leader or not. That's what keeps
  them identical. Only the replica that submitted it has a waiter.
- If the op at this index **isn't ours** (wrong `Me` or `Id`), the waiter
  is told `ErrWrongLeader`, and the client retries elsewhere.
- `m.CommandIndex > rsm.lastApplied` skips anything already covered by a
  snapshot.

`w.done` has a buffer of 1, so this send never blocks, even if the waiter
already timed out and left.

## Point 5: deduplication, now inside the replicated state

```go
// server.go
func (kv *KVServer) DoOp(req any) any {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    switch r := req.(type) {
    case GetArgs:
        v, ok := kv.data[r.Key]
        if !ok {
            return GetReply{Err: ErrNoKey}
        }
        return GetReply{Err: OK, Value: v}
    case PutAppendArgs:
        if r.Seq <= kv.lastSeq[r.ClientId] {
            return PutAppendReply{Err: OK}
        }
        kv.lastSeq[r.ClientId] = r.Seq
        if r.Append {
            kv.data[r.Key] += r.Value
        } else {
            kv.data[r.Key] = r.Value
        }
        return PutAppendReply{Err: OK}
    }
    // ...
}
```

**How it works:** the same idea as lab 2, with one new twist. With Raft, a
request can be **committed twice**: the client sends `Append` to leader A,
A commits it but the reply is lost, the client retries at the new leader
B, and B appends it to the log *again*. Both copies end up in the log. So
dedup has to happen **when applying**, and `lastSeq` must be part of the
replicated state. Then every replica skips the second copy the same way.

This version only stores the last `Seq` per client, not the old value,
because lab 4's `Append` doesn't return anything.

## Point 6: the client looks for the leader

```go
// client.go
func (ck *Clerk) Get(key string) string {
    args := GetArgs{Key: key}
    for tried := 0; ; tried++ {
        var reply GetReply
        ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
            return reply.Value
        }
        ck.nextServer(tried)
    }
}

func (ck *Clerk) nextServer(tried int) {
    ck.leader = (ck.leader + 1) % len(ck.servers)
    if (tried+1)%len(ck.servers) == 0 {
        time.Sleep(100 * time.Millisecond)
    }
}
```

**How it works:** the client remembers the last server that worked
(`ck.leader`) and tries it first. On failure it moves to the next server,
and after a full lap with no luck it sleeps briefly, since an election is
probably in progress.

## Point 7: snapshots include the dedup table

```go
// server.go
func (kv *KVServer) Snapshot() []byte {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    w := new(bytes.Buffer)
    e := gob.NewEncoder(w)
    if e.Encode(kv.data) != nil || e.Encode(kv.lastSeq) != nil {
        log.Fatalf("kvraft: encode snapshot")
    }
    return w.Bytes()
}
```

```go
// rsm/rsm.go
func (rsm *RSM) snapshot() []byte {
    // ...
    e.Encode(rsm.lastApplied)
    e.Encode(rsm.sm.Snapshot())
    // ...
}
```

**How it works:** when Raft's saved state passes `maxraftstate` bytes,
`rsm` asks the server for its state and calls `rf.Snapshot(index, ...)`,
and Raft drops the log up to `index` (chapter 3, point 11).

The snapshot must contain **everything that the dropped log entries
produced**, and that includes `lastSeq`. Leave it out, and a server
restored from a snapshot would accept a retried `Append` that the log had
already applied. `rsm` adds its own `lastApplied` so a restarted server
knows where the snapshot ends.

---

## Check yourself

<details>
<summary>1. Why does <code>Get</code> go through the Raft log?</summary>

A leader cut off from the majority may not know it's been replaced, and
would serve stale data. Committing the Get proves this server was leader at
that log position, so the value it reads is the latest one.
</details>

<details>
<summary>2. Submit got index 7. The reader applies an op at index 7 with a different Id. What does that mean, and what happens?</summary>

This server lost leadership before our entry committed, and another
leader's entry took index 7. Ours will never commit at 7. The waiter gets
ErrWrongLeader, and the client retries, probably at the new leader.
</details>

<details>
<summary>3. Why is <code>lastSeq</code> updated in <code>DoOp</code> and not in the <code>PutAppend</code> handler?</summary>

Only the leader runs the handler, but every replica must make the same
dedup decision. And the same request can be committed twice (a retry after
a lost reply), so the check has to happen when each copy is applied.
</details>

<details>
<summary>4. What would break if <code>Snapshot()</code> saved only <code>kv.data</code>?</summary>

After a restart from that snapshot, <code>lastSeq</code> would be empty. A
delayed duplicate of an Append already in the snapshot would look new and
be applied a second time.
</details>

## Try it

1. Make `Get` read `kv.data` directly on the leader instead of calling
   `Submit`. (Check `kv.rsm.Raft().GetState()` and read under `kv.mu`.) Run
   `go test -race -run Partition -count=3 ./labs/4-kvraft/`. Can you get a
   stale read? Think about when it could happen even if the tests don't
   catch it.
2. In `TestOnePartition4A`, the Put sent to the minority completes only
   after the partition heals. Trace why: which line keeps it waiting, and
   which line finally answers it?
