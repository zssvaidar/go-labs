// Package tester runs replica groups on a labrpc network for the labs'
// tests and demos: it starts, crashes, restarts, and partitions servers.
package tester

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/zssvaidar/go-labs/internal/labrpc"
)

// IService is anything a server registers on the network. Kill is called
// when the server crashes.
type IService interface {
	Kill()
}

// StartServer creates server `me` of a group. ends[j] reaches server j of
// the same group. It returns the services to register; RPCs are routed to
// them by type name, e.g. "Raft.RequestVote".
type StartServer func(ends []*labrpc.ClientEnd, me int, persister *Persister) []IService

// Group is a set of n replicas that talk to each other, such as a Raft
// cluster. Server i is reachable on the network as ServerName(i).
type Group struct {
	mu         sync.Mutex
	net        *labrpc.Network
	name       string
	n          int
	start      StartServer
	services   [][]IService // nil while crashed
	persisters []*Persister
	endnames   [][]string // endnames[i][j]: server i's end to server j
	connected  []bool
	clerks     map[*Clerk]bool
}

func NewGroup(net *labrpc.Network, name string, n int, start StartServer) *Group {
	g := &Group{
		net:        net,
		name:       name,
		n:          n,
		start:      start,
		services:   make([][]IService, n),
		persisters: make([]*Persister, n),
		endnames:   make([][]string, n),
		connected:  make([]bool, n),
		clerks:     map[*Clerk]bool{},
	}
	for i := range g.persisters {
		g.persisters[i] = MakePersister()
	}
	return g
}

func (g *Group) N() int { return g.n }

func (g *Group) Net() *labrpc.Network { return g.net }

func (g *Group) ServerName(i int) string {
	return fmt.Sprintf("%s-%d", g.name, i)
}

func (g *Group) ServerNames() []string {
	names := make([]string, g.n)
	for i := range names {
		names[i] = g.ServerName(i)
	}
	return names
}

// Services returns the services server i registered, or nil if it is down.
func (g *Group) Services(i int) []IService {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.services[i]
}

func (g *Group) Persister(i int) *Persister {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.persisters[i]
}

// StartServers starts every server and connects them all.
func (g *Group) StartServers() {
	for i := 0; i < g.n; i++ {
		g.StartServer(i)
	}
}

// StartServer (re)starts server i from its persisted state and connects it
// to the other connected servers. A running server is crashed first.
func (g *Group) StartServer(i int) {
	g.ShutdownServer(i)

	g.mu.Lock()
	ends := make([]*labrpc.ClientEnd, g.n)
	g.endnames[i] = make([]string, g.n)
	for j := 0; j < g.n; j++ {
		g.endnames[i][j] = Randstring(20)
		ends[j] = g.net.MakeEnd(g.endnames[i][j])
		g.net.Connect(g.endnames[i][j], g.ServerName(j))
	}
	persister := g.persisters[i]
	g.mu.Unlock()

	// Call out without the lock: start may block or call back into tests.
	services := g.start(ends, i, persister)

	srv := labrpc.MakeServer()
	for _, svc := range services {
		srv.AddService(labrpc.MakeService(svc))
	}
	g.mu.Lock()
	g.services[i] = services
	g.mu.Unlock()
	g.net.AddServer(g.ServerName(i), srv)
	g.Connect(i)
}

// ShutdownServer crashes server i. Only what it saved in its persister
// survives.
func (g *Group) ShutdownServer(i int) {
	g.Disconnect(i)
	g.net.DeleteServer(g.ServerName(i))

	g.mu.Lock()
	// The dead instance may still be writing to its persister; give the
	// next instance a snapshot of it instead.
	g.persisters[i] = g.persisters[i].Copy()
	services := g.services[i]
	g.services[i] = nil
	g.mu.Unlock()

	for _, svc := range services {
		svc.Kill()
	}
}

// Shutdown crashes every server.
func (g *Group) Shutdown() {
	for i := 0; i < g.n; i++ {
		g.ShutdownServer(i)
	}
}

func (g *Group) IsConnected(i int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.connected[i]
}

// Connect lets server i talk to the other connected servers, and lets
// clerks that are allowed to reach it do so.
func (g *Group) Connect(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.connected[i] = true
	for j := 0; j < g.n; j++ {
		if g.connected[j] {
			g.enable(i, j, true)
			g.enable(j, i, true)
		}
	}
	for ck := range g.clerks {
		g.net.Enable(ck.endnames[i], ck.allowed[i])
	}
}

// Disconnect cuts server i off from its peers and from all clerks.
func (g *Group) Disconnect(i int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.connected[i] = false
	for j := 0; j < g.n; j++ {
		g.enable(i, j, false)
		g.enable(j, i, false)
	}
	for ck := range g.clerks {
		g.net.Enable(ck.endnames[i], false)
	}
}

func (g *Group) enable(from, to int, on bool) {
	if g.endnames[from] != nil {
		g.net.Enable(g.endnames[from][to], on)
	}
}

func (g *Group) ConnectAll() {
	for i := 0; i < g.n; i++ {
		g.Connect(i)
	}
}

// Partition splits the group: servers in p1 only talk to p1, servers in p2
// only to p2. Servers in neither are disconnected.
func (g *Group) Partition(p1, p2 []int) {
	for i := 0; i < g.n; i++ {
		g.Disconnect(i)
	}
	for _, side := range [][]int{p1, p2} {
		g.mu.Lock()
		for _, i := range side {
			g.connected[i] = true
		}
		for _, i := range side {
			for _, j := range side {
				g.enable(i, j, true)
			}
			for ck := range g.clerks {
				g.net.Enable(ck.endnames[i], ck.allowed[i])
			}
		}
		g.mu.Unlock()
	}
}

// MaxRaftStateSize is the largest Raft state any server has persisted.
func (g *Group) MaxRaftStateSize() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, p := range g.persisters {
		n = max(n, p.RaftStateSize())
	}
	return n
}

// MaxSnapshotSize is the largest snapshot any server has persisted.
func (g *Group) MaxSnapshotSize() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, p := range g.persisters {
		n = max(n, p.SnapshotSize())
	}
	return n
}

// Clerk is a client's set of ends, one per server in the group.
type Clerk struct {
	Ends     []*labrpc.ClientEnd
	endnames []string
	allowed  []bool
}

// MakeClerk creates ends from a new client to every server.
func (g *Group) MakeClerk() *Clerk {
	g.mu.Lock()
	defer g.mu.Unlock()
	ck := &Clerk{
		Ends:     make([]*labrpc.ClientEnd, g.n),
		endnames: make([]string, g.n),
		allowed:  make([]bool, g.n),
	}
	for j := 0; j < g.n; j++ {
		ck.endnames[j] = Randstring(20)
		ck.Ends[j] = g.net.MakeEnd(ck.endnames[j])
		g.net.Connect(ck.endnames[j], g.ServerName(j))
		ck.allowed[j] = true
		g.net.Enable(ck.endnames[j], g.connected[j])
	}
	g.clerks[ck] = true
	return ck
}

// ConnectClerk limits the clerk to reaching only the servers in `to`.
func (g *Group) ConnectClerk(ck *Clerk, to []int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for j := range ck.allowed {
		ck.allowed[j] = false
	}
	for _, j := range to {
		ck.allowed[j] = true
	}
	for j := 0; j < g.n; j++ {
		g.net.Enable(ck.endnames[j], ck.allowed[j] && g.connected[j])
	}
}

// DeleteClerk stops the group tracking the clerk.
func (g *Group) DeleteClerk(ck *Clerk) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.clerks, ck)
}

// MakeEnd returns a new end connected to the named server, for servers and
// clients that look other servers up by name (like sharded KV).
func MakeEnd(net *labrpc.Network, servername string) *labrpc.ClientEnd {
	name := Randstring(20)
	end := net.MakeEnd(name)
	net.Connect(name, servername)
	net.Enable(name, true)
	return end
}

func Randstring(n int) string {
	b := make([]byte, 2*n)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)[:n]
}
