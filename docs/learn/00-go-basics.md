# 0. Go basics used in every lab

Distributed systems code in Go is mostly about **many things happening at
once**: servers handling RPCs, timers firing, background loops, replies
arriving late. Go gives you a small toolkit for that. This chapter covers
each tool with a real example from the labs, so later chapters can focus on
the distributed-systems ideas.

| # | Concept | Where you'll see it |
|---|---------|---------------------|
| 1 | Goroutines | every RPC send, every background loop |
| 2 | `sync.Mutex` and the "one big lock" pattern | every server struct |
| 3 | `defer` | unlocking, closing files |
| 4 | Channels and `select` | `applyCh`, waiting with a timeout |
| 5 | `sync.Cond` | Raft's applier |
| 6 | `sync/atomic` | the `dead` flag |
| 7 | Structs, methods, pointer receivers | everything |
| 8 | Interfaces and `any` | `StateMachine`, log commands |
| 9 | Type switches | applying different operations |
| 10 | Closures and loop variables | sending RPCs in a loop |
| 11 | Slices share memory | copying log entries |
| 12 | Maps: nil maps and copying | snapshots, shard data |
| 13 | `encoding/gob` and `gob.Register` | RPCs, persistence |
| 14 | Reflection | how `labrpc` finds RPC handlers |
| 15 | Generics | `copyMap` |
| 16 | Tests and the race detector | every `_test.go` |

---

## 1. Goroutines

A goroutine is a function running concurrently with the rest of the
program. You start one by putting `go` in front of a call. They're cheap
(a few KB each), so the labs start thousands.

```go
// labs/3-raft/raft.go — Make()
go rf.ticker()
go rf.applier()
return rf
```

`Make` starts two loops that run for the server's whole life, then returns
right away. `ticker` checks timeouts; `applier` delivers committed entries.

The most important use: **never wait on the network in a goroutine that
other work depends on.** Raft sends each RPC in its own goroutine:

```go
// labs/3-raft/raft.go — broadcastAppendEntries()
for p := range rf.peers {
    if p != rf.me {
        go rf.replicateTo(p, rf.currentTerm)
    }
}
```

If peer 2 is down and its RPC takes 100 ms to time out, peers 3 and 4 still
get their messages immediately.

> A goroutine has no handle: you can't kill it from outside. It stops only
> when its function returns. That's why every long-running loop checks a
> "should I stop?" flag (see section 6).

## 2. `sync.Mutex` and the "one big lock" pattern

When two goroutines touch the same variable and at least one writes, you
have a **data race**: the result is undefined. A `sync.Mutex` makes
goroutines take turns.

```go
// labs/3-raft/raft.go
type Raft struct {
    mu sync.Mutex
    // ... every field below is guarded by mu
    currentTerm int
    votedFor    int
    log         []LogEntry
}

func (rf *Raft) GetState() (int, bool) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    return rf.currentTerm, rf.state == Leader
}
```

All the labs use **one mutex per server, guarding all of its state**. It's
simpler than many small locks, and it makes each locked section atomic: no
other goroutine can see the state half-updated.

Two rules that matter in every lab:

1. **Never hold the lock while waiting for something slow**, like an RPC
   or a channel send. Others would block for the whole wait, and if the
   thing you're waiting on needs your lock, you deadlock.
2. **After you re-take the lock, re-check your assumptions.** While you
   were unlocked, anything could have changed.

Both rules in one function:

```go
// labs/3-raft/raft.go — replicateTo()
rf.mu.Lock()
// ... build args from rf's state ...
rf.mu.Unlock()                       // rule 1: unlock before the RPC

var reply AppendEntriesReply
if !rf.peers[p].Call("Raft.AppendEntries", &args, &reply) {
    return
}

rf.mu.Lock()
defer rf.mu.Unlock()
if rf.state != Leader || rf.currentTerm != term {
    return                           // rule 2: are we still leader of that term?
}
```

## 3. `defer`

`defer f()` runs `f` when the surrounding function returns, however it
returns (normal return, early return, or panic). Use it to pair setup with
cleanup so you can't forget the cleanup on some path:

```go
rf.mu.Lock()
defer rf.mu.Unlock()
```

Deferred calls run in reverse order. They're evaluated when the function
*returns*, not at the end of a loop body, so don't `defer` inside a long
loop.

## 4. Channels and `select`

A channel passes values between goroutines. An **unbuffered** channel
(`make(chan T)`) makes the sender wait until a receiver takes the value.
That's how Raft hands committed commands to the service:

```go
// labs/3-raft/raft.go — applier()
rf.mu.Unlock()
rf.applyCh <- msg   // blocks until the service reads it
rf.mu.Lock()
```

Notice the unlock around the send (rule 1 from section 2): the service may
be slow, or may call back into Raft before reading the next message.

A **buffered** channel (`make(chan T, 1)`) lets the sender continue
without waiting, up to the buffer size. `rsm` uses a buffer of 1 so the
goroutine that applies entries never blocks on a waiter that has already
given up:

```go
// labs/4-kvraft/rsm/rsm.go — Submit()
w := &waiter{id: op.Id, done: make(chan result, 1)}
```

`select` waits on several channels at once and takes whichever is ready
first. That's how you wait **with a timeout**:

```go
// labs/4-kvraft/rsm/rsm.go — Submit()
ticker := time.NewTicker(50 * time.Millisecond)
defer ticker.Stop()
timeout := time.After(submitTimeout)
for {
    select {
    case r := <-w.done:          // our op was applied
        return r.err, r.value
    case <-ticker.C:             // every 50ms: did we lose leadership?
        if t, isLeader := rsm.rf.GetState(); t != term || !isLeader {
            rsm.stopWaiting(index, w)
            return ErrWrongLeader, nil
        }
    case <-timeout:              // give up after 2s
        rsm.stopWaiting(index, w)
        return ErrWrongLeader, nil
    }
}
```

## 5. `sync.Cond`: wait until a condition is true

A goroutine that has nothing to do should sleep, not spin. `sync.Cond`
lets it sleep until another goroutine says "something changed":

```go
// labs/3-raft/raft.go
rf.applyCond = sync.NewCond(&rf.mu)   // tied to the server's mutex

// applier(): the waiting side
for !rf.killed() {
    if rf.lastApplied < rf.commitIndex {
        // ... deliver one entry ...
    } else {
        rf.applyCond.Wait()   // atomically: unlock, sleep, re-lock on wake
    }
}

// advanceCommitIndex(): the signalling side (lock held)
rf.commitIndex = idx
rf.applyCond.Broadcast()
```

`Wait()` must be inside a loop that re-checks the condition. A wakeup only
means "something changed", not "your condition is now true".

## 6. `sync/atomic` for a simple flag

For a single flag read by many goroutines, an atomic integer is simpler
than a lock:

```go
// labs/3-raft/raft.go
func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    // ...
}

func (rf *Raft) killed() bool {
    return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) ticker() {
    for !rf.killed() {
        // ...
    }
}
```

This is the same idea as the `done` flag + mutex from the lecture example
you started with. Every background loop checks it so a "crashed" server's
goroutines actually stop.

## 7. Structs, methods, pointer receivers

A method is a function attached to a type. With a **pointer receiver**
(`*Raft`), the method works on the original struct. With a value receiver
it would get a copy, including a copy of the mutex, which is a bug.

```go
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
```

Rule of thumb: if a struct has a mutex or is modified by its methods, use
pointer receivers everywhere. `go vet` warns if you copy a mutex.

Note the **named results** `(index int, term int, isLeader bool)`. They
document what's returned, and you can `return` them by name.

## 8. Interfaces and `any`

An interface is a set of methods. Any type that has those methods satisfies
it automatically; there's no `implements` keyword.

```go
// labs/4-kvraft/rsm/rsm.go
type StateMachine interface {
    DoOp(req any) any
    Snapshot() []byte
    Restore(data []byte)
}
```

`KVServer` (lab 4), `ShardCtrler` (5A), and `ShardKV` (5B) each have these
three methods, so `rsm` can replicate any of them without knowing which
one it has. That's why one `rsm` package serves three labs.

`any` is the empty interface (`interface{}`): it holds a value of any type.
Raft's log stores commands as `any` because Raft doesn't care what they
mean:

```go
type LogEntry struct {
    Term    int
    Command any
}
```

## 9. Type switches

To get the concrete value back out of an `any`, use a type switch:

```go
// labs/4-kvraft/server.go — DoOp()
switch r := req.(type) {
case GetArgs:
    v, ok := kv.data[r.Key]       // r is a GetArgs here
    // ...
case PutAppendArgs:
    if r.Seq <= kv.lastSeq[r.ClientId] {   // r is a PutAppendArgs here
        // ...
    }
}
```

The single-type form is `op := m.Command.(Op)`. It panics if the value
isn't an `Op`; use `op, ok := m.Command.(Op)` if you're not sure.

## 10. Closures and loop variables

A closure is a function literal that uses variables from the surrounding
scope. Starting a goroutine per peer looks like this:

```go
// labs/3-raft/raft.go — startElection()
votes := 1
for p := range rf.peers {
    if p == rf.me {
        continue
    }
    go func(p int) {
        // ...
        votes++                    // shared with the other goroutines, guarded by rf.mu
        if votes > len(rf.peers)/2 {
            rf.becomeLeader()
        }
    }(p)                           // p passed as an argument
}
```

Two things to see:

- `votes` is **shared** by all the goroutines. That's intentional: they're
  counting together. It's only touched while holding `rf.mu`.
- `p` is **passed as an argument**, so each goroutine gets its own copy.
  Before Go 1.22, a closure that used the loop variable directly would see
  it change under its feet, and every goroutine might send to the last
  peer. Passing it in is correct in every Go version.

## 11. Slices share memory

A slice is a view into an array. `s[2:5]` doesn't copy; it points into the
same array. So if a sender puts part of its log in an RPC and then keeps
appending, the receiver could see the data change. The fix is to copy:

```go
// labs/3-raft/raft.go — replicateTo()
Entries: append([]LogEntry(nil), rf.log[prev+1-rf.lastIncludedIndex:]...),
```

`append([]T(nil), s...)` is the standard idiom for "a fresh copy of s".

The same thing matters when trimming the log for a snapshot: re-slicing
keeps the whole old array (and every old command) in memory. Copying into a
new slice lets the garbage collector free it:

```go
// labs/3-raft/raft.go — compactLog()
rf.log = append([]LogEntry{{Term: term}}, rest...)
```

## 12. Maps: nil maps and copying

Reading a nil map returns zero values; **writing to one panics**. gob
decodes an empty map as `nil`, so after loading a snapshot you have to
make the maps writable:

```go
// labs/5-shardkv/shardkv/server.go — Restore()
for s := range shards {
    if shards[s].Data == nil {
        shards[s].Data = map[string]string{}
    }
}
```

Maps are references, like slices: assigning a map to another variable
doesn't copy it. When a shard's data is sent to another group, the code
copies it first (section 15).

## 13. `encoding/gob` and `gob.Register`

gob turns Go values into bytes and back. The labs use it for every RPC and
for saving state to the persister:

```go
// labs/3-raft/raft.go — persist()
w := new(bytes.Buffer)
e := gob.NewEncoder(w)
for _, v := range []any{rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.log} {
    if err := e.Encode(v); err != nil {
        log.Fatalf("persist: %v", err)
    }
}
rf.persister.Save(w.Bytes(), rf.snapshot)
```

Decoding must read the values back **in the same order**.

The catch: when a value is stored in an `any` field (like
`LogEntry.Command`), gob writes its type name and needs to know that type
on decode. You tell it with `gob.Register`, usually in `init()`, which runs
automatically when the package loads:

```go
// labs/4-kvraft/common.go
func init() {
    gob.Register(PutAppendArgs{})
    gob.Register(GetArgs{})
}
```

Forget this and you get `gob: type not registered for interface`.

gob only encodes **exported** (capitalized) struct fields. A field named
`term` would silently arrive as zero.

## 14. Reflection: how `labrpc` finds your handlers

You never register RPC handlers by hand. `labrpc` inspects the object's
methods at runtime with the `reflect` package and keeps those that look
like `func (r *T) Name(args *A, reply *R)`:

```go
// internal/labrpc/labrpc.go — MakeService()
typ := reflect.TypeOf(rcvr)
for m := 0; m < typ.NumMethod(); m++ {
    method := typ.Method(m)
    mtype := method.Type
    if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.NumOut() != 0 ||
        mtype.In(1).Kind() != reflect.Ptr || mtype.In(2).Kind() != reflect.Ptr {
        continue // not an RPC handler
    }
    svc.methods[method.Name] = method
}
```

`NumIn() != 3` because the receiver counts as an input. So `Raft.RequestVote`
qualifies, while `Raft.GetState()` (no args) and `Raft.Snapshot(int, []byte)`
(not pointers) are skipped. A call like `"Raft.RequestVote"` is split at the
dot: the service name is the struct's type name, the rest is the method.

Go's standard `net/rpc` (used in lab 1) works the same way, but its handlers
also return an `error`.

## 15. Generics

Since Go 1.18, a function can take type parameters. This one copies any
map:

```go
// labs/5-shardkv/shardkv/server.go
func copyMap[K comparable, V any](m map[K]V) map[K]V {
    c := make(map[K]V, len(m))
    for k, v := range m {
        c[k] = v
    }
    return c
}
```

`K comparable` means K must support `==` (required for map keys). The same
function copies `map[string]string` (shard data) and `map[int64]int64`
(dedup tables).

## 16. Tests and the race detector

Tests live in `_test.go` files, in functions named `TestXxx(t *testing.T)`.

```go
// labs/3-raft/raft_test.go
func newCluster(t *testing.T, n int, reliable bool) *Cluster {
    t.Helper()                 // failures point at the caller's line
    c := NewCluster(n, reliable, false)
    t.Cleanup(c.Cleanup)       // runs when the test ends, pass or fail
    return c
}
```

Useful commands:

```sh
go test ./labs/3-raft/               # all tests in a package
go test -run 3A ./labs/3-raft/       # tests whose name matches a regexp
go test -v -run TestBackup3B ./labs/3-raft/   # verbose, one test
go test -race ./...                  # with the race detector
go test -count=3 ./labs/3-raft/      # run 3 times (no caching): find flaky tests
```

**Always use `-race`.** It watches memory accesses while the program runs
and reports any two goroutines that touched the same variable without
synchronization. It only catches races that actually happen during the
run, so run concurrent tests more than once.

One gotcha: `t.Fatal` only works in the test's own goroutine. From a
goroutine you started, send the error back on a channel or use `t.Error`
(see `TestUnaffected5B` in `labs/5-shardkv/shardkv/shardkv_test.go`).

---

## Check yourself

<details>
<summary>1. Why does <code>replicateTo</code> unlock before calling <code>Call</code>?</summary>

The RPC can take up to ~100 ms (or forever, if a handler blocks). Holding
the lock would block every other goroutine of this server for that long,
including the RPC handlers. And if the peer's handler somehow needed our
lock, it would deadlock.
</details>

<details>
<summary>2. After the RPC returns, why check <code>rf.currentTerm != term</code>?</summary>

While unlocked, this server may have seen a higher term and stepped down, or
lost and won another election. A reply to an old term's request must not
update the new term's state.
</details>

<details>
<summary>3. What goes wrong if you forget <code>gob.Register(GetArgs{})</code>?</summary>

`GetArgs` travels through the Raft log inside `rsm.Op.Req`, an `any`. gob
can't encode an interface value whose concrete type isn't registered, so
`persist()` fails with "type not registered for interface".
</details>

<details>
<summary>4. Why must <code>Wait()</code> be in a loop?</summary>

`Broadcast` wakes every waiter, and another goroutine can change the state
between the wakeup and when this one re-takes the lock. The condition has to
be re-checked after every wakeup.
</details>

## Try it

Remove the lock from `GetState` in `labs/3-raft/raft.go`:

```go
func (rf *Raft) GetState() (int, bool) {
    return rf.currentTerm, rf.state == Leader
}
```

Run `go test -race -run InitialElection ./labs/3-raft/`. Read the race report:
it names the two goroutines and the exact lines where they collided. Put the
lock back.
