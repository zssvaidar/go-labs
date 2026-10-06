// Package mr is lab 1: MapReduce. A coordinator splits a job into map and
// reduce tasks and hands them to workers over RPC; workers can crash or be
// slow, and the coordinator re-issues their tasks to others.
//
// See the MapReduce paper: https://research.google/pubs/pub62/
package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

// MapFunc turns one input file into key/value pairs.
type MapFunc func(filename string, contents string) []KeyValue

// ReduceFunc combines all the values emitted for one key.
type ReduceFunc func(key string, values []string) string

// ihash picks the reduce bucket for a key.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// errCrash is what a test's map or reduce function panics with to simulate
// the worker process dying mid-task.
var errCrash = fmt.Errorf("simulated worker crash")

// Worker asks the coordinator at sockname for tasks and runs them until the
// job is done. Intermediate and output files go in dir.
func Worker(sockname, dir string, mapf MapFunc, reducef ReduceFunc) {
	for {
		var reply RequestTaskReply
		if !call(sockname, "Coordinator.RequestTask", &RequestTaskArgs{}, &reply) {
			return // coordinator is gone: the job is over
		}
		switch reply.Type {
		case MapTask, ReduceTask:
			if crashed := runTask(dir, &reply, mapf, reducef); crashed {
				return // a dead worker doesn't report anything
			}
			call(sockname, "Coordinator.ReportTask", &ReportTaskArgs{Type: reply.Type, TaskId: reply.TaskId}, &ReportTaskReply{})
		case WaitTask:
			time.Sleep(100 * time.Millisecond)
		case ExitTask:
			return
		}
	}
}

// runTask runs one task. A panic with errCrash means the test wants this
// worker to die here.
func runTask(dir string, t *RequestTaskReply, mapf MapFunc, reducef ReduceFunc) (crashed bool) {
	defer func() {
		if r := recover(); r != nil {
			if r != errCrash {
				panic(r)
			}
			crashed = true
		}
	}()
	var err error
	if t.Type == MapTask {
		err = doMap(dir, t.TaskId, t.File, t.NReduce, mapf)
	} else {
		err = doReduce(dir, t.TaskId, t.NMap, reducef)
	}
	if err != nil {
		panic(err)
	}
	return false
}

// doMap reads one input file, runs mapf, and splits the output into
// nReduce intermediate files mr-<map>-<reduce>, one per reduce bucket.
func doMap(dir string, mapId int, filename string, nReduce int, mapf MapFunc) error {
	content, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
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
		if err != nil {
			return err
		}
	}
	return nil
}

// doReduce reads bucket reduceId from every map task's output, sorts by
// key, runs reducef once per key, and writes mr-out-<reduceId>.
func doReduce(dir string, reduceId, nMap int, reducef ReduceFunc) error {
	var kvs []KeyValue
	for m := 0; m < nMap; m++ {
		f, err := os.Open(filepath.Join(dir, fmt.Sprintf("mr-%d-%d", m, reduceId)))
		if err != nil {
			return err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		f.Close()
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
			if _, err := fmt.Fprintf(w, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values)); err != nil {
				return err
			}
			i = j
		}
		return nil
	})
}

// writeAtomic writes to a temp file and renames it into place, so a worker
// that crashes halfway never leaves a partial file that looks finished.
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

// call sends one RPC to the coordinator, returning false if it's gone.
func call(sockname, rpcname string, args, reply any) bool {
	c, err := rpc.Dial("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()
	return c.Call(rpcname, args, reply) == nil
}
