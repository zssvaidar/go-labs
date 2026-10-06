// Package kvsrv is lab 2: a key/value server on one machine. Clients reach
// it over an unreliable network, so they must retry, and the server must
// make sure a retried Put or Append takes effect only once.
//
// There is no replication yet: if the server crashes, the data is gone.
// Lab 4 fixes that with Raft.
package kvsrv

import "sync"

type KVServer struct {
	mu      sync.Mutex
	data    map[string]string
	clients map[int64]*lastReply // client -> its most recent Put/Append
}

// lastReply is all the server keeps per client: a client only has one
// request outstanding, so once it sends Seq n+1 it has seen the reply to
// n, and the server can forget it. This keeps memory small.
type lastReply struct {
	seq   int64
	value string
}

func StartKVServer() *KVServer {
	return &KVServer{
		data:    map[string]string{},
		clients: map[int64]*lastReply{},
	}
}

// Get is safe to repeat, so it needs no deduplication.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.duplicate(args); ok {
		reply.Value = last
		return
	}
	kv.data[args.Key] = args.Value
	kv.remember(args, "")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if last, ok := kv.duplicate(args); ok {
		reply.Value = last // the same old value the lost reply carried
		return
	}
	old := kv.data[args.Key]
	kv.data[args.Key] = old + args.Value
	kv.remember(args, old)
	reply.Value = old
}

// duplicate reports whether this request was already applied, and if so
// returns the reply it got the first time.
func (kv *KVServer) duplicate(args *PutAppendArgs) (string, bool) {
	if last := kv.clients[args.ClientId]; last != nil && args.Seq <= last.seq {
		return last.value, true
	}
	return "", false
}

func (kv *KVServer) remember(args *PutAppendArgs, value string) {
	kv.clients[args.ClientId] = &lastReply{seq: args.Seq, value: value}
}

// Size returns the number of keys and of client records, for tests.
func (kv *KVServer) Size() (keys, clients int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return len(kv.data), len(kv.clients)
}

func (kv *KVServer) Kill() {}
