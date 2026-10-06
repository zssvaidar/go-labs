# Lab 5: Sharded key/value service

One Raft group can only go as fast as its leader. To scale, split the keys
into shards (here 10, by first byte of the key) and give each shard to one
of several replica groups.

```sh
go run . 5
go test -race ./labs/5-shardkv/...      # ~1.5 minutes
```

## 5A: Shard controller ([`shardctrler`](shardctrler))

A small replicated service (on `rsm`) that holds a numbered sequence of
configs. Each config says which group serves each shard, and which servers
are in each group.

- `Join(groups)` / `Leave(gids)`: add or remove groups, then rebalance so
  shard counts differ by at most one, **moving as few shards as possible**.
- `Move(shard, gid)`: put one shard on a specific group.
- `Query(num)`: get config `num` (or the latest with -1).

Rebalancing has to be deterministic, since every replica runs it
separately. Go randomizes map iteration order, so it works on sorted gids.

## 5B: Sharded KV servers ([`shardkv`](shardkv))

Each group is a Raft cluster running the KV service for the shards it owns.
The group's leader polls the controller for the next config. When a shard
changes owner, it moves in three steps, each committed through Raft so all
replicas agree:

| Step | Old owner A | New owner B |
|------|-------------|-------------|
| 1. Apply new config | shard → `BePulling`: stop serving it, keep the data | shard → `Pulling` |
| 2. B fetches the data from A and installs it | | shard → `GCing`: serving it again |
| 3. B tells A to delete its copy | shard deleted | shard → `Serving` |

Rules that keep it correct:

- **Check ownership when applying, not when receiving.** A request can sit
  in the log while a config change commits ahead of it.
- **Move the dedup table with the shard**, so a client that retries its
  `Append` at the new owner doesn't get it applied twice.
- **One config at a time.** A group only moves to the next config when all
  its shards are `Serving`.
- **Shards that don't move keep working.** Only `Pulling` shards are
  unavailable while a move is in progress.

Clients look up the key's shard in their cached config, send the request to
that group, and on `ErrWrongGroup` fetch the latest config and retry.
