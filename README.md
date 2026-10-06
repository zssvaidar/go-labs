# go-labs

My solutions to the labs of MIT 6.5840 (Distributed Systems, formerly
6.824), in Go. Each lab builds on the previous ones.

| # | Lab | What it is | Key ideas |
|---|-----|------------|-----------|
| 1 | [MapReduce](labs/1-mapreduce) | Coordinator hands map/reduce tasks to workers over RPC | Re-issuing tasks from crashed or slow workers, atomic output files |
| 2 | [Key/value server](labs/2-kvsrv) | One server, clients on a lossy network | At-most-once semantics: a retried request applies only once |
| 3 | [Raft](labs/3-raft) | Consensus: election, replication, persistence, snapshots | The heart of the course |
| 4 | [Fault-tolerant KV](labs/4-kvraft) | Lab 2's service replicated with Raft | Replicated state machines, linearizable reads |
| 5 | [Sharded KV](labs/5-shardkv) | Keys split across many Raft groups | Moving shards between groups while serving |

**Learning the code?** Start with the [learning guide](docs/learn/README.md):
Go concepts first, then each lab's key ideas with the code explained.

## Run

```sh
go run .            # list the labs
go run . 3          # demo of lab 3 (Raft)
go test -race ./... # every lab's tests (~8 minutes)
go test -race ./labs/3-raft/ -run 3A   # one part of a lab
```

## Layout

```
internal/labrpc   simulated network: drops, delays, partitions (labs 2-5)
internal/tester   starts, crashes, restarts, and partitions server groups
labs/1-mapreduce  lab 1 (real net/rpc over a Unix socket)
labs/2-kvsrv      lab 2
labs/3-raft       lab 3
labs/4-kvraft     lab 4, plus rsm/: generic "replicate any state machine" layer
labs/5-shardkv    lab 5: shardctrler/ (5A) and shardkv/ (5B)
```

Everything runs in one process: servers are goroutines, and the "network"
is `internal/labrpc`, which copies every message through gob (so servers
never share memory) and can lose or delay any of them.
