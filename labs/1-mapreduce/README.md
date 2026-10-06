# Lab 1: MapReduce

A coordinator splits a job into tasks and hands them to workers over RPC,
like the [MapReduce paper](https://research.google/pubs/pub62/).

```sh
go run . 1
go test -race ./labs/1-mapreduce/
```

## How it works

1. **Map phase.** Each input file is one map task. The worker runs the map
   function and splits its output into `nReduce` buckets by `hash(key)`,
   writing `mr-<map>-<bucket>`.
2. **Reduce phase.** Starts only when every map task is done. Reduce task
   `r` reads bucket `r` from every map output, sorts by key, calls reduce
   once per key, and writes `mr-out-<r>`.
3. **Failures.** If a worker doesn't report back within the timeout (10 s,
   1 s in tests), the coordinator gives its task to someone else. The
   coordinator can't tell a crashed worker from a slow one, so a task may
   run twice. That's safe because each output is written to a temp file
   and renamed into place: the rename is atomic, and both runs produce the
   same output.

Workers ask for work (`RequestTask`) and report it (`ReportTask`). Between
the phases they get `WaitTask`; once the job is done they get `ExitTask`.

| File | What it is |
|------|------------|
| `coordinator.go` | Task bookkeeping and timeouts |
| `worker.go` | Worker loop, map and reduce, atomic writes |
| `rpc.go` | RPC argument types |
| `apps.go` | Word count and inverted index |
| `sequential.go` | Reference single-threaded run, input generator |
| `mr_test.go` | Correctness, parallelism, crash tests |

The crash test makes a third of tasks "kill" their worker mid-task (a panic
the worker recovers from by exiting without reporting) and a third stall
for up to 2 s.
