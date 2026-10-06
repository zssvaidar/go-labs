# Learning guide

How the Go code in these labs works: first the Go concepts the labs
rely on, then the important ideas of each lab. Every point shows the real
code and explains how it works.

| Chapter | Read it for |
|---------|-------------|
| [0. Go basics](00-go-basics.md) | Goroutines, mutexes, channels, `select`, `sync.Cond`, interfaces, gob, reflection, tests and `-race`, each shown in the lab code |
| [1. MapReduce](01-mapreduce.md) | Coordinator/worker over `net/rpc`, phases, timeouts, atomic writes, simulating crashes |
| [2. Key/value server](02-kvsrv.md) | Lossy networks, retries, client ID + sequence numbers for at-most-once |
| [3. Raft](03-raft.md) | Election, replication, commit rules, persistence, snapshots |
| [4. Fault-tolerant KV](04-kvraft.md) | Building a service on Raft: submit-and-wait, applying in order, dedup in the replicated state |
| [5. Sharded KV](05-shardkv.md) | Configs, deterministic rebalancing, moving shards between groups |

## How to use it

- Read chapter 0 first, then the labs in order. Each lab uses ideas from the
  ones before it.
- Keep the code open next to the guide. The snippets are trimmed (`// ...`
  marks cut lines); the files have the full versions and more comments.
- Run the demo (`go run . N`) before reading a lab's chapter, and again
  after. The second time, you should be able to explain every line it
  prints.
- Each chapter ends with **Check yourself** questions. Answer them before
  opening the hidden answers.
- Each chapter also has **Try it** experiments that break something on
  purpose. Use `git stash` or `git checkout -- <file>` to undo them.
