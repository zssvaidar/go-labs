package mr

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type job struct {
	nReduce  int
	nWorkers int
	timeout  time.Duration
	respawn  bool // start a new worker whenever one exits, until the job is done
	mapf     MapFunc
	reducef  ReduceFunc
}

// run runs the job on files and returns the directory holding the output.
// firstExit, if set, is called as soon as the first worker exits.
func (j job) run(t *testing.T, files []string, firstExit func(dir string)) string {
	t.Helper()
	dir := t.TempDir()
	c, err := MakeCoordinator(files, j.nReduce, j.timeout)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var wg sync.WaitGroup
	var once sync.Once
	var start func()
	start = func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			Worker(c.Sock(), dir, j.mapf, j.reducef)
			if firstExit != nil {
				once.Do(func() { firstExit(dir) })
			}
			if j.respawn && !c.Done() {
				start()
			}
		}()
	}
	for i := 0; i < j.nWorkers; i++ {
		start()
	}

	deadline := time.Now().Add(60 * time.Second)
	for !c.Done() {
		if time.Now().After(deadline) {
			t.Fatal("job didn't finish in 60s")
		}
		time.Sleep(50 * time.Millisecond)
	}
	wg.Wait() // workers see ExitTask and stop
	return dir
}

func inputs(t *testing.T) []string {
	t.Helper()
	files, err := GenerateInputs(t.TempDir(), 8, 2000, 1)
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func checkOutput(t *testing.T, dir string, files []string, mapf MapFunc, reducef ReduceFunc) {
	t.Helper()
	want, err := Sequential(files, mapf, reducef)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ReadOutput(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Compare(got, want); err != nil {
		t.Fatal(err)
	}
}

func TestWordCount(t *testing.T) {
	files := inputs(t)
	dir := job{nReduce: 10, nWorkers: 3, timeout: DefaultTaskTimeout, mapf: WcMap, reducef: WcReduce}.run(t, files, nil)
	checkOutput(t, dir, files, WcMap, WcReduce)
}

func TestIndexer(t *testing.T) {
	files := inputs(t)
	dir := job{nReduce: 10, nWorkers: 2, timeout: DefaultTaskTimeout, mapf: IndexerMap, reducef: IndexerReduce}.run(t, files, nil)
	checkOutput(t, dir, files, IndexerMap, IndexerReduce)
}

func TestMapParallelism(t *testing.T) {
	var running, peak int32
	mapf := func(filename, contents string) []KeyValue {
		n := atomic.AddInt32(&running, 1)
		for {
			p := atomic.LoadInt32(&peak)
			if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
		atomic.AddInt32(&running, -1)
		return WcMap(filename, contents)
	}
	files := inputs(t)
	job{nReduce: 2, nWorkers: 2, timeout: DefaultTaskTimeout, mapf: mapf, reducef: WcReduce}.run(t, files, nil)
	if peak < 2 {
		t.Fatalf("map tasks never ran in parallel (peak %d)", peak)
	}
}

func TestReduceParallelism(t *testing.T) {
	var running, peak int32
	reducef := func(key string, values []string) string {
		// "the" and "go" are in different reduce tasks (checked below):
		// stall on both so the two tasks overlap if they run in parallel.
		if key == "the" || key == "go" {
			n := atomic.AddInt32(&running, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(500 * time.Millisecond)
			atomic.AddInt32(&running, -1)
		}
		return WcReduce(key, values)
	}
	files := inputs(t)
	// Find two keys that land in different reduce buckets.
	if ihash("the")%2 == ihash("go")%2 {
		t.Skip("test keys hash to the same bucket")
	}
	job{nReduce: 2, nWorkers: 2, timeout: DefaultTaskTimeout, mapf: WcMap, reducef: reducef}.run(t, files, nil)
	if peak < 2 {
		t.Fatalf("reduce tasks never ran in parallel (peak %d)", peak)
	}
}

// TestJobCount checks that, without failures, each map task runs once.
func TestJobCount(t *testing.T) {
	var calls int32
	mapf := func(filename, contents string) []KeyValue {
		atomic.AddInt32(&calls, 1)
		return WcMap(filename, contents)
	}
	files := inputs(t)
	job{nReduce: 4, nWorkers: 4, timeout: DefaultTaskTimeout, mapf: mapf, reducef: WcReduce}.run(t, files, nil)
	if int(calls) != len(files) {
		t.Fatalf("map ran %d times for %d files", calls, len(files))
	}
}

// TestEarlyExit checks that no worker exits before the whole job is done:
// by the time the first one exits, all output must be there.
func TestEarlyExit(t *testing.T) {
	files := inputs(t)
	var early error
	job{nReduce: 4, nWorkers: 3, timeout: DefaultTaskTimeout, mapf: WcMap, reducef: WcReduce}.run(t, files, func(dir string) {
		want, _ := Sequential(files, WcMap, WcReduce)
		got, err := ReadOutput(dir)
		if err == nil {
			err = Compare(got, want)
		}
		early = err
	})
	if early != nil {
		t.Fatalf("output incomplete when the first worker exited: %v", early)
	}
}

// TestCrash makes workers die mid-task or stall, so the coordinator must
// re-issue their tasks. Output must still be exactly right.
func TestCrash(t *testing.T) {
	maybeCrash := func() {
		switch r := rand.Intn(1000); {
		case r < 330:
			panic(errCrash) // the worker dies
		case r < 660:
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond) // a straggler
		}
	}
	mapf := func(filename, contents string) []KeyValue {
		maybeCrash()
		return WcMap(filename, contents)
	}
	reducef := func(key string, values []string) string {
		if key == "the" { // once per reduce task that has this key
			maybeCrash()
		}
		return WcReduce(key, values)
	}
	files := inputs(t)
	j := job{nReduce: 10, nWorkers: 3, timeout: time.Second, respawn: true, mapf: mapf, reducef: reducef}
	dir := j.run(t, files, nil)
	checkOutput(t, dir, files, WcMap, WcReduce)
}
