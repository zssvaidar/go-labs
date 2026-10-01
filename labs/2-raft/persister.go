package raft

import "sync"

// Persister stands in for a disk. Raft saves its persistent state here so a
// crashed server can recover it when it restarts.
type Persister struct {
	mu        sync.Mutex
	raftstate []byte
}

func NewPersister() *Persister {
	return &Persister{}
}

// Copy returns an independent persister with the same contents. The cluster
// hands a restarted server a copy so the crashed instance can't overwrite it.
func (ps *Persister) Copy() *Persister {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return &Persister{raftstate: clone(ps.raftstate)}
}

func (ps *Persister) Save(raftstate []byte) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func clone(b []byte) []byte {
	return append([]byte(nil), b...)
}
