package mr

import (
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// DefaultTaskTimeout is how long the coordinator waits for a worker before
// assuming it crashed and giving its task to someone else.
const DefaultTaskTimeout = 10 * time.Second

type taskState int

const (
	idle taskState = iota
	inProgress
	done
)

type task struct {
	state   taskState
	started time.Time
}

// Coordinator hands out map tasks (one per input file), then, once every
// map task is done, reduce tasks (one per output bucket). It re-issues any
// task that isn't finished within the timeout.
type Coordinator struct {
	mu          sync.Mutex
	files       []string
	nReduce     int
	mapTasks    []task
	reduceTasks []task
	timeout     time.Duration
	sockname    string
	listener    net.Listener
	assigned    int // tasks handed out, including re-issues
	reissued    int // tasks re-issued after a timeout
}

// MakeCoordinator starts a coordinator for the given input files and
// listens for workers on a Unix socket.
func MakeCoordinator(files []string, nReduce int, timeout time.Duration) (*Coordinator, error) {
	c := &Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]task, len(files)),
		reduceTasks: make([]task, nReduce),
		timeout:     timeout,
		sockname:    coordinatorSock(),
	}
	// A fresh rpc.Server rather than the global one, so several
	// coordinators (e.g. in tests) don't collide.
	server := rpc.NewServer()
	if err := server.Register(c); err != nil {
		return nil, err
	}
	os.Remove(c.sockname)
	l, err := net.Listen("unix", c.sockname)
	if err != nil {
		return nil, err
	}
	c.listener = l
	go func() {
		// Like server.Accept, but quietly stops when Close is called.
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return c, nil
}

// Sock is the address workers connect to.
func (c *Coordinator) Sock() string { return c.sockname }

// RequestTask is called by a worker that wants something to do.
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

// pick finds a task that is idle, or whose worker has taken too long, and
// marks it in progress. Called with c.mu held.
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

// ReportTask is called by a worker when it finishes a task. A slow worker
// may report a task that was already re-issued and finished by another;
// that's fine, because outputs are written atomically and are identical.
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.Type {
	case MapTask:
		c.mapTasks[args.TaskId].state = done
	case ReduceTask:
		c.reduceTasks[args.TaskId].state = done
	}
	reply.OK = true
	return nil
}

// Done reports whether the whole job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return allDone(c.mapTasks) && allDone(c.reduceTasks)
}

// Stats returns how many tasks were handed out, and how many of those
// were re-issued because a worker took too long.
func (c *Coordinator) Stats() (assigned, reissued int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.assigned, c.reissued
}

// Close stops listening. Workers asking for tasks after this will exit.
func (c *Coordinator) Close() {
	c.listener.Close()
	os.Remove(c.sockname)
}

func allDone(tasks []task) bool {
	for _, t := range tasks {
		if t.state != done {
			return false
		}
	}
	return true
}
