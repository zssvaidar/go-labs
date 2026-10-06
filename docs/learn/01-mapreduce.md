# 1. MapReduce

Code: [`labs/1-mapreduce`](../../labs/1-mapreduce) · Run: `go run . 1` ·
Test: `go test -race ./labs/1-mapreduce/`

## The idea

You want to count words in many big files. Split the work:

```
            map tasks (one per input file)          reduce tasks (one per bucket)
input-0 ──> map ──> mr-0-0  mr-0-1  mr-0-2  mr-0-3
input-1 ──> map ──> mr-1-0  mr-1-1  mr-1-2  mr-1-3
input-2 ──> map ──> mr-2-0  mr-2-1  mr-2-2  mr-2-3
                      │       │       │       │
                      ▼       ▼       ▼       ▼
                   reduce  reduce  reduce  reduce
                      │       │       │       │
                  mr-out-0 mr-out-1 mr-out-2 mr-out-3
```

- **Map** reads one file and emits `(word, "1")` pairs. Each pair goes into
  one of `nReduce` buckets, chosen by hashing the word.
- **Reduce** for bucket `r` collects every pair in bucket `r` from every
  map, groups them by word, and outputs `word count`.

Hashing guarantees that all copies of one word land in the same bucket, so
one reduce task sees every count for that word.

A **coordinator** hands out tasks; **workers** do them. Workers may crash
or be slow, and the coordinator has to cope.

## Point 1: map and reduce are just function types

```go
// worker.go
type KeyValue struct {
    Key   string
    Value string
}

type MapFunc func(filename string, contents string) []KeyValue
type ReduceFunc func(key string, values []string) string
```

```go
// apps.go
func WcMap(filename string, contents string) []KeyValue {
    words := strings.FieldsFunc(contents, func(r rune) bool { return !unicode.IsLetter(r) })
    kvs := make([]KeyValue, 0, len(words))
    for _, w := range words {
        kvs = append(kvs, KeyValue{Key: w, Value: "1"})
    }
    return kvs
}

func WcReduce(key string, values []string) string {
    return strconv.Itoa(len(values))
}
```

**How it works:** the framework doesn't know what job it runs. Anything
with the right signature works: `WcMap`/`WcReduce` count words,
`IndexerMap`/`IndexerReduce` build a word → files index. In Go, functions
are values, so the worker takes them as parameters:
`Worker(sockname, dir, mapf, reducef)`.

`strings.FieldsFunc` splits wherever the function returns true, here at
every non-letter.

## Point 2: the coordinator is a server with a mutex

```go
// coordinator.go
type task struct {
    state   taskState   // idle, inProgress, or done
    started time.Time
}

type Coordinator struct {
    mu          sync.Mutex
    files       []string
    nReduce     int
    mapTasks    []task
    reduceTasks []task
    timeout     time.Duration
    // ...
}
```

**How it works:** the coordinator's whole state is two slices of tasks.
Many workers call it at once, and `net/rpc` runs each call in its own
goroutine, so every handler starts with `c.mu.Lock()`.

## Point 3: real RPC with `net/rpc`

```go
// coordinator.go — MakeCoordinator()
server := rpc.NewServer()
if err := server.Register(c); err != nil {
    return nil, err
}
l, err := net.Listen("unix", c.sockname)
// ...
go func() {
    for {
        conn, err := l.Accept()
        if err != nil {
            return            // listener closed: stop
        }
        go server.ServeConn(conn)
    }
}()
```

```go
// worker.go
func call(sockname, rpcname string, args, reply any) bool {
    c, err := rpc.Dial("unix", sockname)
    if err != nil {
        return false
    }
    defer c.Close()
    return c.Call(rpcname, args, reply) == nil
}
```

**How it works:** `server.Register(c)` uses reflection to find the
coordinator's methods shaped like `func (c *Coordinator) Name(args *A,
reply *R) error`: `RequestTask` and `ReportTask`. The accept loop runs in
a goroutine and gives each connection its own goroutine. A worker calls
`call(sock, "Coordinator.RequestTask", &args, &reply)`.

It uses a Unix socket (a file path), not TCP, because everything runs on
one machine. Labs 2–5 use the simulated network instead, which can also
lose messages.

## Point 4: handing out tasks, and the barrier between phases

```go
// coordinator.go
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    reply.NMap = len(c.files)
    reply.NReduce = c.nReduce

    if id, ok := c.pick(c.mapTasks); ok {
        reply.Type = MapTask
        reply.TaskId = id
        reply.File = c.files[id]
        return nil
    }
    if !allDone(c.mapTasks) {
        // Reduce needs every map's output: wait for stragglers.
        reply.Type = WaitTask
        return nil
    }
    if id, ok := c.pick(c.reduceTasks); ok {
        reply.Type = ReduceTask
        reply.TaskId = id
        return nil
    }
    if allDone(c.reduceTasks) {
        reply.Type = ExitTask
    } else {
        reply.Type = WaitTask
    }
    return nil
}
```

**How it works:** the order of the checks is the scheduling policy:

1. Any map task available? Hand it out.
2. Maps handed out but not all finished? **Wait.** A reduce task needs
   bucket `r` from *every* map; starting early would miss data.
3. Any reduce task available? Hand it out.
4. Everything done? Tell the worker to exit. Otherwise wait.

The worker side is a loop with a `switch` on the reply:

```go
// worker.go — Worker()
for {
    var reply RequestTaskReply
    if !call(sockname, "Coordinator.RequestTask", &RequestTaskArgs{}, &reply) {
        return // coordinator is gone: the job is over
    }
    switch reply.Type {
    case MapTask, ReduceTask:
        if crashed := runTask(dir, &reply, mapf, reducef); crashed {
            return
        }
        call(sockname, "Coordinator.ReportTask", &ReportTaskArgs{Type: reply.Type, TaskId: reply.TaskId}, &ReportTaskReply{})
    case WaitTask:
        time.Sleep(100 * time.Millisecond)
    case ExitTask:
        return
    }
}
```

## Point 5: fault tolerance by timeout

```go
// coordinator.go
func (c *Coordinator) pick(tasks []task) (int, bool) {
    for i := range tasks {
        t := &tasks[i]
        expired := t.state == inProgress && time.Since(t.started) > c.timeout
        if t.state == idle || expired {
            if expired {
                c.reissued++
            }
            t.state = inProgress
            t.started = time.Now()
            c.assigned++
            return i, true
        }
    }
    return 0, false
}
```

**How it works:** the coordinator never learns that a worker crashed; it
only notices that a task has been in progress too long. Then it treats the
task as idle and gives it to the next worker who asks.

Notice `t := &tasks[i]`. Writing `for _, t := range tasks` would give a
**copy** of each element, and `t.state = inProgress` would change the copy,
not the slice.

The coordinator can't tell crashed from slow. A slow worker may still
finish, so **a task can run twice**. The next point makes that safe.

## Point 6: atomic file writes make duplicate tasks harmless

```go
// worker.go
func writeAtomic(path string, write func(io.Writer) error) error {
    tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
    if err != nil {
        return err
    }
    if err := write(tmp); err != nil {
        tmp.Close()
        os.Remove(tmp.Name())
        return err
    }
    if err := tmp.Close(); err != nil {
        return err
    }
    return os.Rename(tmp.Name(), path)
}
```

**How it works:** the worker writes to a unique temp file, then renames it
to the real name. On Unix, rename is atomic: other processes see either the
old file or the complete new one, never half a file.

- A worker that crashes mid-write leaves only a stray temp file, never a
  partial `mr-out-3`.
- Two workers running the same task both produce identical output; the
  second rename just replaces the file with the same content.

`write func(io.Writer) error` is a callback. `writeAtomic` handles the
file juggling, and the caller supplies only what to write. `*os.File`
satisfies `io.Writer`, so the callback can use `fmt.Fprintf` or a JSON
encoder on it.

## Point 7: the map side

```go
// worker.go — doMap()
buckets := make([][]KeyValue, nReduce)
for _, kv := range mapf(filename, string(content)) {
    r := ihash(kv.Key) % nReduce
    buckets[r] = append(buckets[r], kv)
}
for r, kvs := range buckets {
    err := writeAtomic(filepath.Join(dir, fmt.Sprintf("mr-%d-%d", mapId, r)), func(w io.Writer) error {
        enc := json.NewEncoder(w)
        for _, kv := range kvs {
            if err := enc.Encode(&kv); err != nil {
                return err
            }
        }
        return nil
    })
    // ...
}
```

**How it works:** `ihash` (FNV hash) maps every key to a stable number, so
`"raft"` always goes to the same bucket in every map task. The worker
writes one JSON line per pair into `mr-<map>-<bucket>`.

## Point 8: the reduce side

```go
// worker.go — doReduce()
var kvs []KeyValue
for m := 0; m < nMap; m++ {
    // read every pair from mr-<m>-<reduceId> ...
}
sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })

return writeAtomic(filepath.Join(dir, fmt.Sprintf("mr-out-%d", reduceId)), func(w io.Writer) error {
    for i := 0; i < len(kvs); {
        j := i
        var values []string
        for j < len(kvs) && kvs[j].Key == kvs[i].Key {
            values = append(values, kvs[j].Value)
            j++
        }
        fmt.Fprintf(w, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values))
        i = j
    }
    return nil
})
```

**How it works:** after sorting, equal keys sit next to each other. The
outer loop starts at the first pair of a key (`i`). The inner loop walks
`j` forward over all pairs with that key, collecting their values. Then
the worker calls reduce once with all of them and jumps `i` to `j`, the
next key.

## Point 9: simulating crashes in tests with `panic`/`recover`

The tests can't kill a goroutine, so a test map function **panics** to
pretend the worker died:

```go
// mr_test.go — TestCrash
maybeCrash := func() {
    switch r := rand.Intn(1000); {
    case r < 330:
        panic(errCrash) // the worker dies
    case r < 660:
        time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond) // a straggler
    }
}
```

```go
// worker.go
func runTask(dir string, t *RequestTaskReply, mapf MapFunc, reducef ReduceFunc) (crashed bool) {
    defer func() {
        if r := recover(); r != nil {
            if r != errCrash {
                panic(r)       // a real bug: don't hide it
            }
            crashed = true
        }
    }()
    // ... doMap or doReduce ...
}
```

**How it works:** `panic` unwinds the stack, running deferred functions on
the way. A deferred function can call `recover()` to stop the unwinding
and get the panic value. Here it sets the **named result** `crashed`, which
a deferred function is allowed to change, so `runTask` returns `true`. The
worker then exits without reporting, exactly like a dead process. The test
keeps starting new workers, and with a 1 s timeout the coordinator
re-issues the abandoned tasks.

---

## Check yourself

<details>
<summary>1. Why can't reduce tasks start while one map task is still running?</summary>

Reduce task <code>r</code> needs bucket <code>r</code> from <em>every</em> map
task. If one map hasn't finished, its <code>mr-m-r</code> file doesn't exist
yet, and the reduce would produce wrong counts.
</details>

<details>
<summary>2. A worker is slow, its task is re-issued, and then both workers finish. What happens?</summary>

Both write identical output via temp file + rename. Whichever renames second
replaces the file with the same contents. Both report done; marking done
twice is harmless.
</details>

<details>
<summary>3. What would break if <code>pick</code> used <code>for _, t := range tasks</code>?</summary>

<code>t</code> would be a copy. Setting <code>t.state = inProgress</code>
wouldn't change the slice, so the same task would be handed to every worker
that asks.
</details>

## Try it

1. In `TestCrash`, the reduce function stalls or "crashes" when it reaches
   the key `"the"`. That happens *in the middle of* writing `mr-out-*`,
   because `doReduce` calls reduce while writing. Predict what happens if
   `writeAtomic` wrote straight to the final path (replace its body with
   `f, _ := os.Create(path); defer f.Close(); return write(f)`). Think about
   a straggler that wakes up after another worker has already finished the
   same task. Then run `go test -race -run Crash -count=5 ./labs/1-mapreduce/`
   to see if you were right, and put `writeAtomic` back.
2. In `demo.go`, change the timeout passed to `MakeCoordinator` from
   `time.Second` to `time.Millisecond` and run `go run . 1`. The "re-issued"
   count goes way up: tasks are re-issued while they're still running. The
   output is still correct. Why? (Two reasons: point 4 and point 6.)
