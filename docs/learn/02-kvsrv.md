# 2. Key/value server

Code: [`labs/2-kvsrv`](../../labs/2-kvsrv) · Run: `go run . 2` ·
Test: `go test -race ./labs/2-kvsrv/`

## The idea

One server stores `key → value` and supports `Get`, `Put`, and `Append`.
Clients reach it over a network that **loses messages**. This lab is small,
but it introduces the most important trick of the course: making retries
safe.

## Point 1: the simulated network (`internal/labrpc`)

From this lab on, servers don't use real sockets. They use
`internal/labrpc`, a fake network that lives inside the program:

```go
// labs/2-kvsrv/cluster.go
c := &Cluster{net: labrpc.MakeNetwork(), Server: StartKVServer()}
c.net.Reliable(reliable)
srv := labrpc.MakeServer()
srv.AddService(labrpc.MakeService(c.Server))   // exposes KVServer's RPC methods
c.net.AddServer("kvserver", srv)

// a client's handle to the server
end := tester.MakeEnd(c.net, "kvserver")
ok := end.Call("KVServer.Get", &args, &reply)
```

**How it works:** `Call` gob-encodes `args`, finds the server by name, runs
the handler in a goroutine, and gob-decodes the reply into `reply`. Because
everything is encoded and decoded, client and server never share memory,
just as with a real network. In unreliable mode, the network sometimes:

```go
// internal/labrpc/labrpc.go — call()
if !reliable {
    time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
    if rand.Intn(1000) < 100 {
        return false // request lost
    }
}
// ... run the handler ...
if !reliable && rand.Intn(1000) < 100 {
    return false // reply lost
}
```

Look at the second case carefully: **the handler ran**, but the client gets
`false`. From the client's side, a lost request and a lost reply look
exactly the same.

## Point 2: the client retries until it gets an answer

```go
// client.go
func (ck *Clerk) Get(key string) string {
    args := GetArgs{Key: key}
    for {
        var reply GetReply
        if ck.server.Call("KVServer.Get", &args, &reply) {
            return reply.Value
        }
    }
}
```

**How it works:** the only way to make progress over a lossy network is to
try again. Note `var reply GetReply` **inside** the loop: each attempt gets
a fresh, zero-valued reply. gob skips zero-valued fields when decoding, so
reusing a reply could leave stale data from an earlier attempt in it.

For `Get`, retrying is harmless: reading twice changes nothing. Retrying
`Append` is a problem:

```
client                        server             data["k"]
Append("k","x") ──────────>   apply               "x"
            <──── reply LOST
Append("k","x") ──────────>   apply AGAIN         "xx"   ← wrong
```

## Point 3: client ID + sequence number

```go
// client.go
type Clerk struct {
    server   *labrpc.ClientEnd
    clientId int64   // random, picked once
    seq      int64   // incremented for each new Put/Append
}

func (ck *Clerk) putAppend(op, key, value string) string {
    ck.seq++
    // Same Seq on every retry, so the server can spot duplicates.
    args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, Seq: ck.seq}
    for {
        var reply PutAppendReply
        if ck.server.Call("KVServer."+op, &args, &reply) {
            return reply.Value
        }
    }
}
```

**How it works:** `ck.seq++` happens **once, outside the loop**. Every retry
of this request carries the same `(ClientId, Seq)` pair, which works as the
request's unique name.

## Point 4: the server remembers the last request per client

```go
// server.go
type KVServer struct {
    mu      sync.Mutex
    data    map[string]string
    clients map[int64]*lastReply // client -> its most recent Put/Append
}

type lastReply struct {
    seq   int64
    value string
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if last, ok := kv.duplicate(args); ok {
        reply.Value = last // the same old value the lost reply carried
        return
    }
    old := kv.data[args.Key]
    kv.data[args.Key] = old + args.Value
    kv.remember(args, old)
    reply.Value = old
}

func (kv *KVServer) duplicate(args *PutAppendArgs) (string, bool) {
    if last := kv.clients[args.ClientId]; last != nil && args.Seq <= last.seq {
        return last.value, true
    }
    return "", false
}
```

**How it works:**

- First arrival of `Seq 5`: not a duplicate. Apply it and remember
  `{seq: 5, value: old}`.
- Retry of `Seq 5`: `5 <= 5`, so it's a duplicate. Return the **saved**
  reply without applying it again.

The duplicate must return the *same* answer as the original. `Append`
returns the old value, so the server saves it. Otherwise the client could
see a different result depending on which copy got through.

## Point 5: why one record per client is enough

**How it works:** a clerk sends one request at a time and doesn't move on
until it gets a reply. So when the server sees `Seq 6` from a client, that
client must have received the reply to `Seq 5`, and the server will never
need it again. It overwrites the record:

```go
func (kv *KVServer) remember(args *PutAppendArgs, value string) {
    kv.clients[args.ClientId] = &lastReply{seq: args.Seq, value: value}
}
```

Memory is O(number of clients), not O(number of requests). `TestMemory`
checks this.

This assumes each clerk is used by **one goroutine at a time**. Two
goroutines sharing a clerk would have two requests outstanding at once.

## Point 6: `Get` doesn't need deduplication

```go
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    reply.Value = kv.data[args.Key]
}
```

**How it works:** reading has no side effect, so applying it twice is
fine. `kv.data[missing]` returns `""` (the zero value), which is exactly
the "missing key" answer the lab wants.

---

## Check yourself

<details>
<summary>1. Why does the client increment <code>seq</code> outside the retry loop?</summary>

All retries must share one Seq so the server recognizes them as the same
request. Incrementing inside the loop would make each retry look new, and
the server would apply it again.
</details>

<details>
<summary>2. Request Seq 7 arrives after Seq 8 (it was delayed in the network). What does the server do?</summary>

<code>7 &lt;= 8</code>, so it's treated as a duplicate and not applied. That's
correct: the client only sent Seq 8 after getting a reply to Seq 7, so 7
was already applied. The client is no longer waiting for this reply.
</details>

<details>
<summary>3. Lab 4 replicates this server on Raft. Where must the <code>clients</code> table live then?</summary>

In the replicated state, applied through the log, and saved in snapshots.
Otherwise a new leader wouldn't know which requests the old leader already
applied, and would apply retries again. (See chapter 4, point 5.)
</details>

## Try it

Make `duplicate` always return `false`:

```go
if false && last != nil && args.Seq <= last.seq {
```

Run `go test -run Unreliable ./labs/2-kvsrv/` and read the failure: it names
an append that shows up twice. Then put it back.
