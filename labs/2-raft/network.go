package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Network simulates the wires between servers. Servers can be disconnected,
// and in unreliable mode RPCs are delayed and randomly dropped, like in the
// MIT 6.5840 test harness.
type Network struct {
	mu        sync.Mutex
	servers   []*Raft // current instance of each server, nil if crashed
	connected []bool
	reliable  bool
	rpcCount  int
}

func NewNetwork(n int) *Network {
	return &Network{
		servers:   make([]*Raft, n),
		connected: make([]bool, n),
		reliable:  true,
	}
}

func (net *Network) Size() int {
	return len(net.servers)
}

func (net *Network) attach(i int, rf *Raft) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.servers[i] = rf
}

func (net *Network) detach(i int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.servers[i] = nil
}

func (net *Network) Connect(i int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connected[i] = true
}

func (net *Network) Disconnect(i int) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connected[i] = false
}

func (net *Network) IsConnected(i int) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.connected[i]
}

func (net *Network) SetReliable(reliable bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.reliable = reliable
}

func (net *Network) RPCCount() int {
	net.mu.Lock()
	defer net.mu.Unlock()
	return net.rpcCount
}

// call delivers one RPC from a sender to server `to` by running handler on
// the receiver. It returns false if the request or the reply was lost, in
// which case the caller must ignore the reply.
func (net *Network) call(from *Raft, to int, handler func(*Raft)) bool {
	net.mu.Lock()
	net.rpcCount++
	// The sender must still be the live instance: a crashed server's
	// leftover goroutines must not be able to talk to anyone.
	ok := net.servers[from.me] == from && net.connected[from.me] &&
		net.servers[to] != nil && net.connected[to]
	target := net.servers[to]
	reliable := net.reliable
	net.mu.Unlock()

	if !ok {
		// Simulate waiting for a timeout on a dead link.
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		return false
	}
	if !reliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(10) == 0 {
			return false // request lost
		}
	}
	handler(target)
	if !reliable && rand.Intn(10) == 0 {
		return false // reply lost
	}
	return true
}
