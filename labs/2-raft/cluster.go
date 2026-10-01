package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Cluster runs n Raft servers on a simulated network and records what each
// one applies, so the demo and the tests can crash, disconnect, and restart
// servers and then check that they all agree.
type Cluster struct {
	mu         sync.Mutex
	n          int
	net        *Network
	rafts      []*Raft
	persisters []*Persister
	applied    []map[int]any // per server: log index -> command
	applyErr   string        // first safety violation seen, if any
}

func NewCluster(n int, reliable bool) *Cluster {
	c := &Cluster{
		n:          n,
		net:        NewNetwork(n),
		rafts:      make([]*Raft, n),
		persisters: make([]*Persister, n),
		applied:    make([]map[int]any, n),
	}
	c.net.SetReliable(reliable)
	for i := 0; i < n; i++ {
		c.persisters[i] = NewPersister()
		c.applied[i] = map[int]any{}
		c.Restart(i)
	}
	return c
}

func (c *Cluster) N() int { return c.n }

func (c *Cluster) Raft(i int) *Raft {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rafts[i]
}

func (c *Cluster) Connect(i int)          { c.net.Connect(i) }
func (c *Cluster) Disconnect(i int)       { c.net.Disconnect(i) }
func (c *Cluster) IsConnected(i int) bool { return c.net.IsConnected(i) }
func (c *Cluster) SetReliable(r bool)     { c.net.SetReliable(r) }
func (c *Cluster) RPCCount() int          { return c.net.RPCCount() }

// Crash kills server i. Only what it saved in its persister survives.
func (c *Cluster) Crash(i int) {
	c.net.Disconnect(i)
	c.net.detach(i)
	c.mu.Lock()
	rf := c.rafts[i]
	c.rafts[i] = nil
	// The dead instance's goroutines may still write to the old persister;
	// give the next instance a snapshot of it instead.
	c.persisters[i] = c.persisters[i].Copy()
	c.mu.Unlock()
	if rf != nil {
		rf.Kill()
	}
}

// Restart crashes server i if it is running, then boots a new instance from
// its persisted state and connects it.
func (c *Cluster) Restart(i int) {
	c.Crash(i)
	applyCh := make(chan ApplyMsg)
	c.mu.Lock()
	rf := Make(c.net, i, c.persisters[i], applyCh)
	c.rafts[i] = rf
	c.mu.Unlock()
	go c.collect(i, applyCh)
	c.net.attach(i, rf)
	c.net.Connect(i)
}

// Cleanup kills every server.
func (c *Cluster) Cleanup() {
	for i := 0; i < c.n; i++ {
		c.Crash(i)
	}
}

// collect records everything server i applies and checks the two safety
// rules: all servers apply the same command at an index, and each server
// applies indexes in order.
func (c *Cluster) collect(i int, applyCh chan ApplyMsg) {
	for m := range applyCh {
		if !m.CommandValid {
			continue
		}
		c.mu.Lock()
		for j, logs := range c.applied {
			if old, ok := logs[m.CommandIndex]; ok && old != m.Command {
				c.fail("index %d: server %d applied %v but server %d applied %v",
					m.CommandIndex, i, m.Command, j, old)
			}
		}
		if _, ok := c.applied[i][m.CommandIndex-1]; m.CommandIndex > 1 && !ok {
			c.fail("server %d applied index %d before %d", i, m.CommandIndex, m.CommandIndex-1)
		}
		c.applied[i][m.CommandIndex] = m.Command
		c.mu.Unlock()
	}
}

func (c *Cluster) fail(format string, args ...any) {
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
