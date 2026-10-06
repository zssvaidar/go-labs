package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
)

// SnapshotInterval is how often (in log entries) a cluster created with
// snapshots asks each server to snapshot.
const SnapshotInterval = 10

// Cluster runs n Raft servers on a labrpc network and records what each one
// applies, so the demo and the tests can crash, disconnect, and restart
// servers and then check that they all agree.
type Cluster struct {
	mu        sync.Mutex
	n         int
	net       *labrpc.Network
	group     *tester.Group
	rafts     []*Raft
	applied   []map[int]any // per server: log index -> command
	applyErr  string        // first safety violation seen, if any
	snapshots bool
}

// NewCluster starts n connected servers. With snapshots, each server acts
// like a service that snapshots its state every SnapshotInterval entries.
func NewCluster(n int, reliable, snapshots bool) *Cluster {
	c := &Cluster{
		n:         n,
		net:       labrpc.MakeNetwork(),
		rafts:     make([]*Raft, n),
		applied:   make([]map[int]any, n),
		snapshots: snapshots,
	}
	for i := range c.applied {
		c.applied[i] = map[int]any{}
	}
	c.net.Reliable(reliable)
	c.group = tester.NewGroup(c.net, "raft", n, c.startServer)
	c.group.StartServers()
	return c
}

func (c *Cluster) startServer(ends []*labrpc.ClientEnd, me int, persister *tester.Persister) []tester.IService {
	lastApplied := 0
	if c.snapshots {
		if snap := persister.ReadSnapshot(); len(snap) > 0 {
			lastApplied = c.ingestSnapshot(me, snap)
		}
	}
	applyCh := make(chan ApplyMsg)
	rf := Make(ends, me, persister, applyCh)
	c.mu.Lock()
	c.rafts[me] = rf
	c.mu.Unlock()
	go c.collect(me, rf, applyCh, lastApplied)
	return []tester.IService{rf}
}

func (c *Cluster) N() int { return c.n }

func (c *Cluster) Raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *Cluster) Connect(i int)          { c.group.Connect(i) }
func (c *Cluster) Disconnect(i int)       { c.group.Disconnect(i) }
func (c *Cluster) IsConnected(i int) bool { return c.group.IsConnected(i) }
func (c *Cluster) SetReliable(r bool)     { c.net.Reliable(r) }
func (c *Cluster) RPCCount() int          { return c.net.GetTotalCount() }
func (c *Cluster) MaxLogSize() int        { return c.group.MaxRaftStateSize() }

// Crash kills server i. Only what it saved in its persister survives.
func (c *Cluster) Crash(i int) {
	c.mu.Lock()
	c.rafts[i] = nil
	c.mu.Unlock()
	c.group.ShutdownServer(i)
}

// Restart crashes server i if it is running, then boots a new instance from
// its persisted state and connects it.
func (c *Cluster) Restart(i int) {
	c.Crash(i)
	c.group.StartServer(i)
}

// Cleanup kills every server.
func (c *Cluster) Cleanup() {
	for i := 0; i < c.n; i++ {
		c.Crash(i)
	}
}

// collect plays the service on top of server i: it records what Raft
// applies, checks the safety rules, and takes and installs snapshots.
func (c *Cluster) collect(i int, rf *Raft, applyCh chan ApplyMsg, lastApplied int) {
	for m := range applyCh {
		switch {
		case m.SnapshotValid:
			if !c.snapshots {
				c.fail("server %d got a snapshot but snapshots are off", i)
				continue
			}
			if idx := c.ingestSnapshot(i, m.Snapshot); idx != m.SnapshotIndex {
				c.fail("server %d: snapshot says index %d, ApplyMsg says %d", i, idx, m.SnapshotIndex)
			}
			lastApplied = m.SnapshotIndex
		case m.CommandValid:
			if m.CommandIndex != lastApplied+1 {
				c.fail("server %d applied index %d after %d", i, m.CommandIndex, lastApplied)
			}
			lastApplied = m.CommandIndex
			c.mu.Lock()
			c.record(i, m.CommandIndex, m.Command)
			c.mu.Unlock()
			if c.snapshots && m.CommandIndex%SnapshotInterval == 0 {
				rf.Snapshot(m.CommandIndex, c.encodeSnapshot(i, m.CommandIndex))
			}
		}
	}
}

// record saves that server i applied cmd at index, checking that no other
// server applied something different there. Called with c.mu held.
func (c *Cluster) record(i, index int, cmd any) {
	for j, logs := range c.applied {
		if old, ok := logs[index]; ok && old != cmd {
			c.failLocked("index %d: server %d applied %v but server %d applied %v",
				index, i, cmd, j, old)
		}
	}
	c.applied[i][index] = cmd
}

// A test snapshot is just every command applied so far.
func (c *Cluster) encodeSnapshot(i, upTo int) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmds := make([]any, upTo)
	for idx := 1; idx <= upTo; idx++ {
		cmds[idx-1] = c.applied[i][idx]
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(upTo) != nil || e.Encode(cmds) != nil {
		log.Fatalf("encode snapshot")
	}
	return w.Bytes()
}

func (c *Cluster) ingestSnapshot(i int, data []byte) int {
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var upTo int
	var cmds []any
	if d.Decode(&upTo) != nil || d.Decode(&cmds) != nil {
		log.Fatalf("decode snapshot")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, cmd := range cmds {
		c.record(i, idx+1, cmd)
	}
	return upTo
}

func (c *Cluster) fail(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failLocked(format, args...)
}

func (c *Cluster) failLocked(format string, args ...any) {
	if c.applyErr == "" {
		c.applyErr = fmt.Sprintf(format, args...)
	}
}

// Err reports the first safety violation seen by any server, if any.
func (c *Cluster) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr != "" {
		return fmt.Errorf("%s", c.applyErr)
	}
	return nil
}

// Applied returns the commands server i has applied, in index order.
func (c *Cluster) Applied(i int) []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cmds []any
	for idx := 1; ; idx++ {
		cmd, ok := c.applied[i][idx]
		if !ok {
			return cmds
		}
		cmds = append(cmds, cmd)
	}
}

// CheckOneLeader waits for exactly one connected server to be leader in the
// newest term and returns it.
func (c *Cluster) CheckOneLeader() (int, error) {
	for iters := 0; iters < 10; iters++ {
		time.Sleep(time.Duration(450+rand.Intn(100)) * time.Millisecond)
		leaders := map[int][]int{} // term -> leaders
		for i := 0; i < c.n; i++ {
			if rf := c.Raft(i); rf != nil && c.IsConnected(i) {
				if term, isLeader := rf.GetState(); isLeader {
					leaders[term] = append(leaders[term], i)
				}
			}
		}
		lastTerm := -1
		for term, ls := range leaders {
			if len(ls) > 1 {
				return -1, fmt.Errorf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			lastTerm = max(lastTerm, term)
		}
		if lastTerm != -1 {
			return leaders[lastTerm][0], nil
		}
	}
	return -1, fmt.Errorf("expected one leader, got none")
}

// CheckNoLeader returns an error if any connected server thinks it's leader.
func (c *Cluster) CheckNoLeader() error {
	for i := 0; i < c.n; i++ {
		if rf := c.Raft(i); rf != nil && c.IsConnected(i) {
			if _, isLeader := rf.GetState(); isLeader {
				return fmt.Errorf("expected no leader among connected servers, but %d claims to be", i)
			}
		}
	}
	return nil
}

// CheckTerms returns the term all connected servers agree on.
func (c *Cluster) CheckTerms() (int, error) {
	term := -1
	for i := 0; i < c.n; i++ {
		if rf := c.Raft(i); rf != nil && c.IsConnected(i) {
			t, _ := rf.GetState()
			if term == -1 {
				term = t
			} else if t != term {
				return -1, fmt.Errorf("servers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term, nil
}

// NCommitted returns how many servers have applied index, and the command.
func (c *Cluster) NCommitted(index int) (int, any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	var cmd any
	for i := 0; i < c.n; i++ {
		if v, ok := c.applied[i][index]; ok {
			count++
			cmd = v
		}
	}
	return count, cmd
}

// One submits cmd to whichever server is leader and waits until at least
// `expected` servers have applied it. With retry, it resubmits if the leader
// fails before committing. It returns the log index cmd was committed at.
func (c *Cluster) One(cmd any, expected int, retry bool) (int, error) {
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second {
		if err := c.Err(); err != nil {
			return -1, err
		}
		index := -1
		for si := 0; si < c.n; si++ {
			starts = (starts + 1) % c.n
			if rf := c.Raft(starts); rf != nil && c.IsConnected(starts) {
				if idx, _, ok := rf.Start(cmd); ok {
					index = idx
					break
				}
			}
		}
		if index == -1 {
			time.Sleep(50 * time.Millisecond) // no leader yet
			continue
		}
		t1 := time.Now()
		for time.Since(t1) < 2*time.Second {
			if nd, got := c.NCommitted(index); nd > 0 && nd >= expected && got == cmd {
				return index, nil
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !retry {
			return -1, fmt.Errorf("one(%v) failed to reach agreement", cmd)
		}
	}
	if err := c.Err(); err != nil {
		return -1, err
	}
	return -1, fmt.Errorf("one(%v) failed to reach agreement in 10s", cmd)
}
