# Lab 4: Fault-tolerant key/value service

Lab 2's key/value service, replicated on a Raft cluster (lab 3). It keeps
working as long as a majority of servers are up and can talk to each other.

```sh
go run . 4
go test -race ./labs/4-kvraft/...       # ~2 minutes
```

## How it works

Every `Get`, `Put`, and `Append` goes into the Raft log. Each replica
applies the committed log in order, so every replica goes through the same
states. The server that received the request replies once its entry is
applied.

That logic lives in a separate package, [`rsm`](rsm/rsm.go) ("replicated
state machine"), which labs 5A and 5B reuse:

```
client ──RPC──> KVServer.Get ──> rsm.Submit(op)
                                   │ raft.Start(op)
                                   ▼
                     Raft log, replicated to a majority
                                   │ applyCh (on every replica)
                                   ▼
                     rsm.reader ──> KVServer.DoOp(op) ──> result
                                   │
                     (only on the replica that submitted it)
                                   ▼
                     Submit returns result ──> reply to client
```

Things that make this tricky:

- **Losing leadership.** A leader may accept a request and then lose an
  election before it commits. Another leader's entry ends up at that log
  index. `rsm` tags each op with a unique id, and only reports success if
  the op applied at that index is its own.
- **Partitioned leaders.** A leader cut off from the majority doesn't know
  it lost. `Submit` gives up after a timeout, so the client tries another
  server.
- **Reads go through the log too.** A stale leader could otherwise return
  old data. Putting `Get` in the log makes reads linearizable.
- **Duplicates.** Retries can commit the same `Put` twice. Like lab 2, the
  server keeps the last `Seq` per client. The table is part of the
  replicated state, so every replica dedups the same way.
- **Snapshots (4B).** When Raft's persisted state reaches `maxraftstate`
  bytes, `rsm` asks the server for a snapshot (its data *and* its dedup
  table) and hands it to `raft.Snapshot`, which trims the log.
