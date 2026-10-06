package kvraft

import (
	"math/rand"
	"sync"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
)

// Cluster runs n KV servers on a labrpc network, for the tests and demo.
type Cluster struct {
	mu           sync.Mutex
	n            int
	maxraftstate int
	net          *labrpc.Network
	group        *tester.Group
	servers      []*KVServer
	clerks       map[*Clerk]*tester.Clerk
}

func NewCluster(n int, reliable bool, maxraftstate int) *Cluster {
	c := &Cluster{
		n:            n,
		maxraftstate: maxraftstate,
		net:          labrpc.MakeNetwork(),
		servers:      make([]*KVServer, n),
		clerks:       map[*Clerk]*tester.Clerk{},
	}
	c.net.Reliable(reliable)
	c.group = tester.NewGroup(c.net, "kv", n, c.startServer)
	c.group.StartServers()
	return c
}

func (c *Cluster) startServer(ends []*labrpc.ClientEnd, me int, persister *tester.Persister) []tester.IService {
	kv := StartKVServer(ends, me, persister, c.maxraftstate)
	c.mu.Lock()
	c.servers[me] = kv
	c.mu.Unlock()
	// Register both services: peers call "Raft.*", clerks call "KVServer.*".
	return []tester.IService{kv, kv.rsm.Raft()}
}

func (c *Cluster) N() int { return c.n }

// Server returns server i, or nil if it's crashed.
func (c *Cluster) Server(i int) *KVServer {
	if c.group.Services(i) == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[i]
}

// MakeClerk creates a clerk that can reach every server.
func (c *Cluster) MakeClerk() *Clerk {
	tc := c.group.MakeClerk()
	ck := MakeClerk(tc.Ends)
	c.mu.Lock()
	c.clerks[ck] = tc
	c.mu.Unlock()
	return ck
}

// ConnectClerk limits ck to reaching only the servers in `to`.
func (c *Cluster) ConnectClerk(ck *Clerk, to []int) {
	c.mu.Lock()
	tc := c.clerks[ck]
	c.mu.Unlock()
	c.group.ConnectClerk(tc, to)
}

func (c *Cluster) DeleteClerk(ck *Clerk) {
	c.mu.Lock()
	tc := c.clerks[ck]
	delete(c.clerks, ck)
	c.mu.Unlock()
	c.group.DeleteClerk(tc)
}

// Leader returns a server that thinks it's leader, if any.
func (c *Cluster) Leader() (int, bool) {
	for i := 0; i < c.n; i++ {
		if kv := c.Server(i); kv != nil {
			if _, isLeader := kv.Raft().GetState(); isLeader {
				return i, true
			}
		}
	}
	return -1, false
}

func (c *Cluster) Crash(i int)            { c.group.ShutdownServer(i) }
func (c *Cluster) Restart(i int)          { c.group.StartServer(i) }
func (c *Cluster) Connect(i int)          { c.group.Connect(i) }
func (c *Cluster) Disconnect(i int)       { c.group.Disconnect(i) }
func (c *Cluster) ConnectAll()            { c.group.ConnectAll() }
func (c *Cluster) Partition(p1, p2 []int) { c.group.Partition(p1, p2) }
func (c *Cluster) SetReliable(r bool)     { c.net.Reliable(r) }
func (c *Cluster) RPCCount() int          { return c.net.GetTotalCount() }
func (c *Cluster) MaxRaftStateSize() int  { return c.group.MaxRaftStateSize() }
func (c *Cluster) MaxSnapshotSize() int   { return c.group.MaxSnapshotSize() }
func (c *Cluster) Cleanup()               { c.group.Shutdown() }

func (c *Cluster) CrashAll() {
	for i := 0; i < c.n; i++ {
		c.Crash(i)
	}
}

func (c *Cluster) RestartAll() {
	for i := 0; i < c.n; i++ {
		c.Restart(i)
	}
}

// RandomPartition splits the servers into two random sides.
func (c *Cluster) RandomPartition() ([]int, []int) {
	var p1, p2 []int
	for i := 0; i < c.n; i++ {
		if rand.Intn(2) == 0 {
			p1 = append(p1, i)
		} else {
			p2 = append(p2, i)
		}
	}
	c.Partition(p1, p2)
	return p1, p2
}
