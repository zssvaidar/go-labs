// Package rsm turns any deterministic state machine into a replicated one
// on top of Raft. A server calls Submit(req); rsm puts req in the Raft log,
// waits until it is committed, applies it to the state machine on every
// replica in log order, and returns the result to the server that
// submitted it. When Raft's state grows too large, rsm snapshots the state
// machine so Raft can trim its log.
//
// Labs 4 (kvraft) and 5 (shardctrler, shardkv) are all built on this.
package rsm

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
	raft "github.com/zssvaidar/go-labs/labs/3-raft"
)

// StateMachine is the service being replicated. DoOp must be
// deterministic: every replica applies the same requests in the same order
// and must end up in the same state.
type StateMachine interface {
	DoOp(req any) any
	Snapshot() []byte
	Restore(data []byte)
}

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
)

// submitTimeout bounds how long Submit waits for a commit. A leader cut off
// from the majority never learns it lost; the client must try elsewhere.
const submitTimeout = 2 * time.Second

// Op is what goes in the Raft log. Me and Id identify the Submit call, so
// the submitter can tell whether the entry committed at its index is its
// own or another leader's.
type Op struct {
	Me  int
	Id  int64
	Req any
}

func init() {
	gob.Register(Op{})
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot when Raft's state exceeds this; -1 means never
	persister    *tester.Persister
	sm           StateMachine
	waiting      map[int]*waiter // log index -> the Submit waiting for it
	lastApplied  int
}

type waiter struct {
	id   int64
	done chan result
}

type result struct {
	err   Err
	value any
}

// MakeRSM starts Raft for server `me` and restores sm from the latest
// snapshot, if there is one.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		applyCh:      make(chan raft.ApplyMsg),
		maxraftstate: maxraftstate,
		persister:    persister,
		sm:           sm,
		waiting:      map[int]*waiter{},
	}
	if snap := persister.ReadSnapshot(); len(snap) > 0 {
		rsm.restore(snap)
	}
	rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() *raft.Raft {
	return rsm.rf
}

// Submit replicates req and returns the state machine's result for it. It
// returns ErrWrongLeader if this server isn't the leader, or loses
// leadership before req commits; the caller should retry at another server.
func (rsm *RSM) Submit(req any) (Err, any) {
	op := Op{Me: rsm.me, Id: rand.Int63(), Req: req}

	// Hold the lock across Start so the reader can't apply this index
	// before we've registered to wait for it.
	rsm.mu.Lock()
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return ErrWrongLeader, nil
	}
	w := &waiter{id: op.Id, done: make(chan result, 1)}
	if old := rsm.waiting[index]; old != nil {
		// An earlier leader term handed out this index too; that entry
		// can no longer commit here.
		old.done <- result{err: ErrWrongLeader}
	}
	rsm.waiting[index] = w
	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(submitTimeout)
	for {
		select {
		case r := <-w.done:
			return r.err, r.value
		case <-ticker.C:
			// If the term changed, our entry may have been overwritten.
			if t, isLeader := rsm.rf.GetState(); t != term || !isLeader {
				rsm.stopWaiting(index, w)
				return ErrWrongLeader, nil
			}
		case <-timeout:
			rsm.stopWaiting(index, w)
			return ErrWrongLeader, nil
		}
	}
}

func (rsm *RSM) stopWaiting(index int, w *waiter) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if rsm.waiting[index] == w {
		delete(rsm.waiting, index)
	}
}

// reader applies everything Raft commits, in order, on every replica.
func (rsm *RSM) reader() {
	for m := range rsm.applyCh {
		rsm.mu.Lock()
		switch {
		case m.SnapshotValid:
			if m.SnapshotIndex > rsm.lastApplied {
				rsm.restore(m.Snapshot)
			}
		case m.CommandValid && m.CommandIndex > rsm.lastApplied:
			rsm.lastApplied = m.CommandIndex
			op := m.Command.(Op)
			value := rsm.sm.DoOp(op.Req)

			if w := rsm.waiting[m.CommandIndex]; w != nil {
				delete(rsm.waiting, m.CommandIndex)
				if op.Me == rsm.me && op.Id == w.id {
					w.done <- result{err: OK, value: value}
				} else {
					w.done <- result{err: ErrWrongLeader} // someone else's op won this index
				}
			}

			if rsm.maxraftstate != -1 && rsm.persister.RaftStateSize() >= rsm.maxraftstate {
				rsm.rf.Snapshot(m.CommandIndex, rsm.snapshot())
			}
		}
		rsm.mu.Unlock()
	}
}

// A snapshot is the state machine's state plus the index it reflects.
func (rsm *RSM) snapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(rsm.lastApplied) != nil || e.Encode(rsm.sm.Snapshot()) != nil {
		log.Fatalf("rsm: encode snapshot")
	}
	return w.Bytes()
}

func (rsm *RSM) restore(data []byte) {
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var lastApplied int
	var smData []byte
	if d.Decode(&lastApplied) != nil || d.Decode(&smData) != nil {
		log.Fatalf("rsm: decode snapshot")
	}
	rsm.lastApplied = lastApplied
	rsm.sm.Restore(smData)
}

// Kill stops Raft. The reader keeps draining applyCh so Raft's goroutines
// don't block forever.
func (rsm *RSM) Kill() {
	rsm.rf.Kill()
}
