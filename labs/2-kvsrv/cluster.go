package kvsrv

import (
	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
)

// Cluster is one KV server on a labrpc network, for the tests and demo.
type Cluster struct {
	net    *labrpc.Network
	Server *KVServer
}

func NewCluster(reliable bool) *Cluster {
	c := &Cluster{net: labrpc.MakeNetwork(), Server: StartKVServer()}
	c.net.Reliable(reliable)
	srv := labrpc.MakeServer()
	srv.AddService(labrpc.MakeService(c.Server))
	c.net.AddServer("kvserver", srv)
	return c
}

func (c *Cluster) MakeClerk() *Clerk {
	return MakeClerk(tester.MakeEnd(c.net, "kvserver"))
}

func (c *Cluster) SetReliable(r bool) { c.net.Reliable(r) }
func (c *Cluster) RPCCount() int      { return c.net.GetTotalCount() }
