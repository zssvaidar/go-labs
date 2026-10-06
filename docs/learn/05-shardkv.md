# 5. Sharded key/value service

Code: [`labs/5-shardkv`](../../labs/5-shardkv) · Run: `go run . 5` ·
Test: `go test -race ./labs/5-shardkv/...`

## The idea

In lab 4 every request goes through one leader, so adding servers adds
fault tolerance but not speed. To scale, split the keys into **shards** and
give each shard to one of several **replica groups**, each its own Raft
cluster:

```
                 shard controller (a Raft group): "config 3: shards 0-3 → G100,
                                                   5-7 → G101, 4,8,9 → G102"
                   ▲ Query                  ▲ Join / Leave / Move (admin)
                   │
client ── key "apple" → shard 7 → G101 ──> G101 leader ──> G101 Raft log
```

Two parts:

- **5A, the shard controller:** decides which group owns which shard, as a
  numbered list of configs.
- **5B, the sharded KV servers:** serve their shards, and **move shard data
  between groups** when the config changes, without losing or duplicating
  anything.

Both are built on the `rsm` layer from lab 4.

## Part 5A: the shard controller

### Point 1: a config is a numbered, immutable snapshot

```go
// shardctrler/common.go
const NShards = 10

type Config struct {
    Num    int              // config number
    Shards [NShards]int     // shard -> gid
    Groups map[int][]string // gid -> server names
}

func (c Config) Copy() Config {
    nc := Config{Num: c.Num, Shards: c.Shards, Groups: map[int][]string{}}
    for gid, servers := range c.Groups {
        nc.Groups[gid] = append([]string(nil), servers...)
    }
    return nc
}
```

**How it works:** every Join, Leave, or Move creates config `Num+1`. Old
configs are never changed, so anyone can ask "what was config 4?" and get
a stable answer.

`Shards` is an **array** (`[NShards]int`), so assigning it copies it. But
`Groups` is a **map**, and assigning a map shares it (chapter 0, section
12). `Copy()` makes a deep copy, so changing the new config can't change an
old one.

### Point 2: applying Join/Leave/Move

```go
// shardctrler/server.go — DoOp()
next := sc.configs[len(sc.configs)-1].Copy()
next.Num++
switch r := req.(type) {
case JoinArgs:
    for gid, servers := range r.Servers {
        next.Groups[gid] = append([]string(nil), servers...)
    }
    rebalance(&next)
case LeaveArgs:
    for _, gid := range r.GIDs {
        delete(next.Groups, gid)
        for s, g := range next.Shards {
            if g == gid {
                next.Shards[s] = 0       // orphaned; rebalance reassigns it
            }
        }
    }
    rebalance(&next)
case MoveArgs:
    next.Shards[r.Shard] = r.GID
}
sc.configs = append(sc.configs, next)
```

**How it works:** copy the latest config, change it, rebalance, append.
`Move` skips the rebalance because an admin asked for that exact
placement. Join/Leave/Move use the same `ClientId`/`Seq` dedup as lab 4
(not shown), since a retried Join must not create two configs.

### Point 3: rebalancing must be deterministic and minimal

```go
// shardctrler/server.go — rebalance()
gids := make([]int, 0, len(c.Groups))
for gid := range c.Groups {
    gids = append(gids, gid)
}
// ...
sort.Slice(gids, func(i, j int) bool {
    if len(owned[gids[i]]) != len(owned[gids[j]]) {
        return len(owned[gids[i]]) > len(owned[gids[j]])
    }
    return gids[i] < gids[j]
})
target := map[int]int{}
for i, gid := range gids {
    target[gid] = NShards / len(gids)
    if i < NShards%len(gids) {
        target[gid]++
    }
}
// take shards away from groups over target, give them to groups under it
```

**How it works:**

- **Deterministic.** Each controller replica runs `rebalance` separately
  in `DoOp`, and Go randomizes map iteration order on purpose. Iterating
  `c.Groups` directly could give different replicas different configs. So
  the code collects the gids into a slice and **sorts** it.
- **Even.** 10 shards over 3 groups gives targets 4, 3, 3
  (`NShards / len` each, plus one extra for the first `NShards % len`).
- **Minimal movement.** Groups that already have the most shards come
  first, so they get the "+1" targets and keep what they have. Only the
  excess moves. `TestMinimalTransfers5A` checks that a Join moves shards
  only *to* the new group.

## Part 5B: sharded KV servers

### Point 4: routing a key, and `ErrWrongGroup`

```go
// shardkv/common.go
func key2shard(key string) int {
    shard := 0
    if len(key) > 0 {
        shard = int(key[0])
    }
    return shard % NShards
}
```

```go
// shardkv/client.go
func (ck *Clerk) Get(key string) string {
    args := GetArgs{Key: key}
    for {
        gid := ck.config.Shards[key2shard(key)]
        for _, name := range ck.config.Groups[gid] {
            var reply GetReply
            ok := ck.end(name).Call("ShardKV.Get", &args, &reply)
            if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
                return reply.Value
            }
            if ok && reply.Err == ErrWrongGroup {
                break
            }
        }
        time.Sleep(100 * time.Millisecond)
        ck.config = ck.sm.Query(-1)
    }
}
```

**How it works:** the client keeps a cached config. It maps the key to a
shard, the shard to a group, and tries that group's servers. If the group
says `ErrWrongGroup` (the shard has moved) or none of them answer, it
fetches the latest config and tries again. Servers are addressed by
**name** (`"shardkv-101-0"`), because configs list names, and `ck.end`
caches one connection per name.

### Point 5: the server also checks ownership, at apply time

```go
// shardkv/server.go — DoOp()
case PutAppendArgs:
    s := key2shard(op.Key)
    if !kv.canServe(s) {
        return PutAppendReply{Err: ErrWrongGroup}
    }
    if op.Seq <= kv.lastSeq[op.ClientId] {
        return PutAppendReply{Err: OK}
    }
    kv.lastSeq[op.ClientId] = op.Seq
    // ... apply to kv.shards[s].Data
```

```go
func (kv *ShardKV) canServe(s int) bool {
    st := kv.shards[s].State
    return kv.cur.Shards[s] == kv.gid && (st == Serving || st == GCing)
}
```

**How it works:** the check happens in `DoOp`, **when the op is applied**,
not when the RPC arrives. A request can sit in the Raft log while a config
change commits ahead of it. Checking at the RPC handler would let it
through, and the write would land in a shard this group no longer owns,
after the data was already handed off. The Raft log decides the order, so
only checks made at apply time are reliable.

Also note: a rejected request does **not** update `lastSeq`. The client
will retry it at the new owner, which must apply it.

### Point 6: config changes go through the log too

```go
// shardkv/server.go
func (kv *ShardKV) leaderLoop(work func()) {
    for !kv.killed() {
        if _, isLeader := kv.rsm.Raft().GetState(); isLeader {
            work()
        }
        time.Sleep(pollInterval)
    }
}

func (kv *ShardKV) pollConfig() {
    kv.mu.Lock()
    idle := kv.allServing()
    num := kv.cur.Num
    kv.mu.Unlock()
    if !idle {
        return
    }
    if next := kv.mck.Query(num + 1); next.Num == num+1 {
        kv.rsm.Submit(ConfigOp{Config: next})
    }
}
```

**How it works:** only the leader polls the controller, every 100 ms.
Instead of switching configs directly, it **submits a `ConfigOp`** to its
group's log. Every replica then applies the switch at the same log
position, between the same client requests. The same is true for
installing and deleting shard data: everything that changes state is a log
entry.

It asks for exactly `num + 1`, and only when idle (all shards `Serving`),
so a group goes through configs **one at a time**, finishing each move
before starting the next.

`leaderLoop(work func())` takes the work as a function value. Three
goroutines share the same loop: `go kv.leaderLoop(kv.pollConfig)`,
`kv.pullShards`, and `kv.gcShards`. Here `kv.pollConfig` is a *method
value*: a function bound to `kv`.

### Point 7: the shard state machine

```go
// shardkv/server.go
const (
    Serving   ShardState = iota // nothing pending: ours and served, or not ours
    Pulling                     // ours in the new config; waiting for the data
    BePulling                   // no longer ours; keeping the data for the new owner
    GCing                       // ours and served; old owner hasn't deleted its copy yet
)

func (kv *ShardKV) applyConfig(next shardctrler.Config) {
    if next.Num != kv.cur.Num+1 || !kv.allServing() {
        return
    }
    for s := 0; s < NShards; s++ {
        oldGid, newGid := kv.cur.Shards[s], next.Shards[s]
        switch {
        case oldGid != kv.gid && newGid == kv.gid:
            if oldGid != 0 {
                kv.shards[s].State = Pulling
            } // else: a brand-new shard, nothing to fetch
        case oldGid == kv.gid && newGid != kv.gid:
            if newGid != 0 {
                kv.shards[s].State = BePulling
            } else {
                kv.shards[s] = Shard{Data: map[string]string{}}
            }
        }
    }
    kv.prev = kv.cur
    kv.cur = next.Copy()
}
```

**How it works:** a shard moving from group A to group B goes through these
states:

```
                  A (old owner)              B (new owner)
config applied:   Serving → BePulling        Serving → Pulling
B fetches data:                              Pulling → GCing      (serving again)
B tells A delete: BePulling → Serving*       GCing → Serving
                  (* data deleted, not owned)
```

- **`Pulling`**: B owns the shard but doesn't have the data. It can't
  serve it (`canServe` is false), so clients get `ErrWrongGroup` and retry.
- **`BePulling`**: A no longer serves it, but keeps the data frozen for B.
- **`GCing`**: B has the data and serves it, but still has to tell A to
  delete its copy.

`iota` numbers the constants 0, 1, 2, 3. Since `Serving` is 0 (the zero
value), a fresh `Shard{}` starts as `Serving`.

`kv.prev` remembers the previous config, which is how B knows who to fetch
each shard from.

### Point 8: pulling the data

```go
// shardkv/server.go — pullShards() (leader only)
byGid, num, prev := kv.shardsFromPrevOwners(Pulling)
var wg sync.WaitGroup
for gid, shards := range byGid {
    wg.Add(1)
    go func(servers []string, shards []int) {
        defer wg.Done()
        args := FetchShardsArgs{ConfigNum: num, Shards: shards}
        for _, name := range servers {
            var reply FetchShardsReply
            if kv.end(name).Call("ShardKV.FetchShards", &args, &reply) && reply.Err == OK {
                kv.rsm.Submit(InsertShardsOp{ConfigNum: num, Shards: reply.Shards, LastSeq: reply.LastSeq})
                return
            }
        }
    }(prev.Groups[gid], shards)
}
wg.Wait()
```

```go
// the old owner's handler
func (kv *ShardKV) FetchShards(args *FetchShardsArgs, reply *FetchShardsReply) {
    kv.mu.Lock()
    defer kv.mu.Unlock()
    if kv.cur.Num < args.ConfigNum {
        reply.Err = ErrNotReady
        return
    }
    reply.Shards = map[int]map[string]string{}
    for _, s := range args.Shards {
        reply.Shards[s] = copyMap(kv.shards[s].Data)
    }
    reply.LastSeq = copyMap(kv.lastSeq)
    reply.Err = OK
}
```

**How it works:**

- B groups its `Pulling` shards by previous owner and fetches from each
  owner in parallel. `sync.WaitGroup` waits for all the goroutines to
  finish: `Add(1)` before each one starts, `Done()` (deferred) when it
  ends, `Wait()` blocks until the count is back to zero.
- A answers only once it has **also applied that config**
  (`cur.Num >= ConfigNum`). Before that, A might still accept writes to the
  shard. After that, the shard is frozen, so any replica of A can answer.
- B installs the data through its own log (`InsertShardsOp`), so all of
  B's replicas get it at the same point.

**The dedup table travels with the data** (`LastSeq`). Suppose a client's
`Append` was applied by A, but the reply was lost. The client retries at
B. Without A's `lastSeq`, B would apply it a second time. When applying,
B merges the tables by taking the max:

```go
// DoOp — InsertShardsOp
for client, seq := range op.LastSeq {
    kv.lastSeq[client] = max(kv.lastSeq[client], seq)
}
```

### Point 9: garbage collection, and why configs wait for it

```go
// gcShards(): for each GCing shard, ask the old owner to delete it
if kv.end(name).Call("ShardKV.DeleteShards", &args, &reply) && reply.Err == OK {
    kv.rsm.Submit(GCDoneOp{ConfigNum: num, Shards: shards})
}
```

```go
// old owner, DoOp — DeleteShardsOp
for _, s := range op.Shards {
    if kv.shards[s].State == BePulling {
        kv.shards[s] = Shard{Data: map[string]string{}, State: Serving}
    }
}
```

**How it works:** without this step, every group would keep a copy of
every shard it ever owned. A deletes only when B confirms it installed the
data, so the data is never lost: at every moment at least one group holds
it. `TestDeleteShards5B` checks the total stored bytes stay close to the
real data size.

`pollConfig` only moves on when every shard is `Serving` again. So a shard
is never in two moves at once, and `prev` always names the right owner to
fetch from.

---

## Check yourself

<details>
<summary>1. Why does <code>rebalance</code> sort the gids?</summary>

Every controller replica runs it separately, and Go's map iteration order is
random. Without sorting, replicas could compute different configs from the
same Join, and the controller's replicas would disagree.
</details>

<details>
<summary>2. Why is the ownership check in <code>DoOp</code> and not in the <code>PutAppend</code> RPC handler?</summary>

A config change can commit between when the request arrives and when it's
applied. Only the log order is the same on every replica, so only a check
made at apply time sees the right config.
</details>

<details>
<summary>3. A client's Append is applied by group A, the reply is lost, and the shard moves to B. The client retries at B. Why isn't it applied twice?</summary>

B gets A's <code>lastSeq</code> table along with the shard data and merges
it in. When the retry is applied at B, its Seq is ≤ the recorded one, so it's
skipped.
</details>

<details>
<summary>4. Why does A refuse <code>FetchShards</code> until <code>cur.Num &gt;= ConfigNum</code>?</summary>

Before A applies that config, it still owns the shard and may accept more
writes to it. Data fetched then could miss those writes. After it applies the
config, it rejects writes to the shard, so the data is final.
</details>

<details>
<summary>5. While a shard is <code>Pulling</code> into B, can clients use other shards on B?</summary>

Yes. <code>canServe</code> looks at each shard separately, so shards that
didn't move stay <code>Serving</code>. <code>TestUnaffected5B</code> checks
exactly this.
</details>

## Try it

1. In `rebalance`, remove the `sort.Slice` and iterate the map directly.
   Run `go test -run Deterministic -count=5 ./labs/5-shardkv/shardctrler/`.
2. Move the `canServe` check from `DoOp` to the start of the `PutAppend`
   handler (and remove it from `DoOp`). Run
   `go test -race -run Concurrent -count=3 ./labs/5-shardkv/shardkv/`.
   Reason about a write that slips in after the shard went `BePulling`:
   where does it end up?
3. Run `go run . 5` and match each line of output to a step in the table in
   point 7.
