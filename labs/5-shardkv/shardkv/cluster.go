package shardkv

import (
	"fmt"
	"sync"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
	"github.com/zssvaidar/go-labs/labs/5-shardkv/shardctrler"
)

// Cluster runs a shard controller and several shard groups on one labrpc
// network, for the tests and demo. Groups are numbered 0..ngroups-1 here;
// their gids are 100, 101, ...
type Cluster struct {
	mu           sync.Mutex
	net          *labrpc.Network
	ctrl         *tester.Group
	mck          *shardctrler.Clerk
	groups       []*tester.Group
	servers      [][]*ShardKV
	maxraftstate int
}

func GID(gi int) int { return 100 + gi }

func NewCluster(ngroups, n int, reliable bool, maxraftstate int) *Cluster {
	c := &Cluster{
		net:          labrpc.MakeNetwork(),
		maxraftstate: maxraftstate,
		servers:      make([][]*ShardKV, ngroups),
	}
	c.net.Reliable(reliable)
	c.ctrl = shardctrler.StartGroup(c.net, 3)
	c.mck = shardctrler.MakeGroupClerk(c.ctrl)
	for gi := 0; gi < ngroups; gi++ {
		gi := gi
		c.servers[gi] = make([]*ShardKV, n)
		g := tester.NewGroup(c.net, fmt.Sprintf("shardkv-%d", GID(gi)), n,
			func(ends []*labrpc.ClientEnd, me int, persister *tester.Persister) []tester.IService {
				kv := StartServer(ends, me, persister, c.maxraftstate, GID(gi), c.ctrlEnds(), c.makeEnd)
				c.mu.Lock()
				c.servers[gi][me] = kv
				c.mu.Unlock()
				return []tester.IService{kv, kv.Raft()}
			})
		c.groups = append(c.groups, g)
		g.StartServers()
	}
	return c
}

func (c *Cluster) ctrlEnds() []*labrpc.ClientEnd {
	var ends []*labrpc.ClientEnd
	for _, name := range c.ctrl.ServerNames() {
		ends = append(ends, tester.MakeEnd(c.net, name))
	}
	return ends
}

func (c *Cluster) makeEnd(name string) *labrpc.ClientEnd {
	return tester.MakeEnd(c.net, name)
}

func (c *Cluster) MakeClerk() *Clerk {
	return MakeClerk(c.ctrlEnds(), c.makeEnd)
}

// Ctrler returns a clerk for the shard controller.
func (c *Cluster) Ctrler() *shardctrler.Clerk { return c.mck }

func (c *Cluster) NGroups() int { return len(c.groups) }

// Join adds groups (by index) to the configuration.
func (c *Cluster) Join(gis ...int) {
	m := map[int][]string{}
	for _, gi := range gis {
		m[GID(gi)] = c.groups[gi].ServerNames()
	}
	c.mck.Join(m)
}

// Leave removes groups (by index) from the configuration.
func (c *Cluster) Leave(gis ...int) {
	var gids []int
	for _, gi := range gis {
		gids = append(gids, GID(gi))
	}
	c.mck.Leave(gids)
}

func (c *Cluster) ShutdownGroup(gi int) { c.groups[gi].Shutdown() }
func (c *Cluster) StartGroup(gi int)    { c.groups[gi].StartServers() }
func (c *Cluster) ShutdownServer(gi, i int) {
	c.groups[gi].ShutdownServer(i)
}
func (c *Cluster) StartServer(gi, i int) { c.groups[gi].StartServer(i) }
func (c *Cluster) SetReliable(r bool)    { c.net.Reliable(r) }

// Server returns server i of group gi, or nil if it's down.
func (c *Cluster) Server(gi, i int) *ShardKV {
	if c.groups[gi].Services(i) == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[gi][i]
}

// Leader returns the leader of group gi, if it has one.
func (c *Cluster) Leader(gi int) *ShardKV {
	for i := 0; i < c.groups[gi].N(); i++ {
		if kv := c.Server(gi, i); kv != nil {
			if _, isLeader := kv.Raft().GetState(); isLeader {
				return kv
			}
		}
	}
	return nil
}

// TotalSize is the persisted Raft state plus snapshots of every server in
// every group.
func (c *Cluster) TotalSize() int {
	total := 0
	for _, g := range c.groups {
		for i := 0; i < g.N(); i++ {
			p := g.Persister(i)
			total += p.RaftStateSize() + p.SnapshotSize()
		}
	}
	return total
}

func (c *Cluster) Cleanup() {
	for _, g := range c.groups {
		g.Shutdown()
	}
	c.ctrl.Shutdown()
}
