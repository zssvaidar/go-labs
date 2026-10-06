package kvraft

import (
	"math/rand"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
)

// Clerk is a client of the replicated KV service. It keeps retrying until
// it finds the leader and the leader commits its request.
type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int // server we think is leader, tried first
	clientId int64
	seq      int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:  servers,
		clientId: rand.Int63(),
	}
}

// Get fetches the current value for key, or "" if it doesn't exist.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for tried := 0; ; tried++ {
		var reply GetReply
		ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			return reply.Value
		}
		ck.nextServer(tried)
	}
}

func (ck *Clerk) Put(key, value string)    { ck.putAppend(key, value, false) }
func (ck *Clerk) Append(key, value string) { ck.putAppend(key, value, true) }

func (ck *Clerk) putAppend(key, value string, isAppend bool) {
	ck.seq++
	// The same Seq on every retry: the server applies it only once.
	args := PutAppendArgs{Key: key, Value: value, Append: isAppend, ClientId: ck.clientId, Seq: ck.seq}
	for tried := 0; ; tried++ {
		var reply PutAppendReply
		ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			return
		}
		ck.nextServer(tried)
	}
}

// nextServer moves on to the next server, pausing after each full round so
// we don't spin while an election is in progress.
func (ck *Clerk) nextServer(tried int) {
	ck.leader = (ck.leader + 1) % len(ck.servers)
	if (tried+1)%len(ck.servers) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
}
