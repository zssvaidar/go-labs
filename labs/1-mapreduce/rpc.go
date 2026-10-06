package mr

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask // nothing to hand out right now; ask again soon
	ExitTask // the job is done
)

type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Type    TaskType
	TaskId  int
	File    string // map tasks: the input file
	NMap    int    // reduce tasks read one intermediate file per map task
	NReduce int    // map tasks split their output into NReduce buckets
}

type ReportTaskArgs struct {
	Type   TaskType
	TaskId int
}

type ReportTaskReply struct {
	OK bool
}

var sockCount int64

// coordinatorSock returns a unique Unix socket path for a coordinator.
// Socket paths are limited to ~100 bytes, so it lives in the temp dir.
func coordinatorSock() string {
	n := atomic.AddInt64(&sockCount, 1)
	return filepath.Join(os.TempDir(), fmt.Sprintf("mr-%d-%d.sock", os.Getpid(), n))
}
