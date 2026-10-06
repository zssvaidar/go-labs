// Package kvraft is lab 4: a fault-tolerant key/value service. Each server
// runs Raft (via rsm); every Get, Put, and Append goes through the Raft log,
// so all replicas apply the same operations in the same order. The service
// keeps working as long as a majority of servers are up and connected.
package kvraft

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
	"github.com/zssvaidar/go-labs/labs/4-kvraft/rsm"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// State machine, changed only by DoOp and Restore (called by rsm).
	mu      sync.Mutex
	data    map[string]string
	lastSeq map[int64]int64 // client -> highest Seq applied, for deduplication
}

// StartKVServer starts server `me`; servers[i] reaches server i. It
// snapshots when Raft's persisted state reaches maxraftstate bytes (-1
// disables snapshots).
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int) *KVServer {
	kv := &KVServer{
		me:      me,
		data:    map[string]string{},
		lastSeq: map[int64]int64{},
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Reads go through the log too: a leader that has been partitioned
	// away might not know it's stale, but it can't commit anything.
	err, result := kv.rsm.Submit(*args)
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = result.(GetReply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = result.(PutAppendReply)
}

// DoOp applies one committed request. Every replica calls it with the same
// requests in the same order.
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch r := req.(type) {
	case GetArgs:
		v, ok := kv.data[r.Key]
		if !ok {
			return GetReply{Err: ErrNoKey}
		}
		return GetReply{Err: OK, Value: v}
	case PutAppendArgs:
		// A client retries until it hears back, so the same request can be
		// committed more than once. Apply it only the first time.
		if r.Seq <= kv.lastSeq[r.ClientId] {
			return PutAppendReply{Err: OK}
		}
		kv.lastSeq[r.ClientId] = r.Seq
		if r.Append {
			kv.data[r.Key] += r.Value
		} else {
			kv.data[r.Key] = r.Value
		}
		return PutAppendReply{Err: OK}
	}
	log.Fatalf("kvraft: unknown request %T", req)
	return nil
}

// Snapshot must include the dedup table: otherwise a server restored from
// a snapshot would re-apply a retried Append.
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(kv.data) != nil || e.Encode(kv.lastSeq) != nil {
		log.Fatalf("kvraft: encode snapshot")
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var kvdata map[string]string
	var lastSeq map[int64]int64
	if d.Decode(&kvdata) != nil || d.Decode(&lastSeq) != nil {
		log.Fatalf("kvraft: decode snapshot")
	}
	kv.data = kvdata
	kv.lastSeq = lastSeq
}

// Size returns how many keys this replica holds.
func (kv *KVServer) Size() int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.data)
}

func (kv *KVServer) Raft() interface{ GetState() (int, bool) } {
	return kv.rsm.Raft()
}

func (kv *KVServer) Kill() {
	kv.rsm.Kill()
}
