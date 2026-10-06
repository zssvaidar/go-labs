package shardctrler

import (
	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
)

// StartGroup starts n controller replicas on net, named "ctrler-0",
// "ctrler-1", ...
func StartGroup(net *labrpc.Network, n int) *tester.Group {
	g := tester.NewGroup(net, "ctrler", n, func(ends []*labrpc.ClientEnd, me int, persister *tester.Persister) []tester.IService {
		sc := StartServer(ends, me, persister)
		return []tester.IService{sc, sc.Raft()}
	})
	g.StartServers()
	return g
}

// MakeGroupClerk creates a clerk that can reach every replica in g.
func MakeGroupClerk(g *tester.Group) *Clerk {
	return MakeClerk(g.MakeClerk().Ends)
}
