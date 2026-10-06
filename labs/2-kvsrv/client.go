package kvsrv

import (
	"math/rand"

	"github.com/zssvaidar/go-labs/internal/labrpc"
)

// Clerk talks to the server. Each call keeps retrying until it gets a reply:
// a lost request and a lost reply look the same to the client.
type Clerk struct {
	server   *labrpc.ClientEnd
	clientId int64
	seq      int64
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	return &Clerk{server: server, clientId: rand.Int63()}
}

// Get fetches the current value for key, or "" if it doesn't exist.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for {
		var reply GetReply
		if ck.server.Call("KVServer.Get", &args, &reply) {
			return reply.Value
		}
	}
}

func (ck *Clerk) Put(key, value string) {
	ck.putAppend("Put", key, value)
}

// Append adds value to the end of key's value and returns the old value.
func (ck *Clerk) Append(key, value string) string {
	return ck.putAppend("Append", key, value)
}

func (ck *Clerk) putAppend(op, key, value string) string {
	ck.seq++
	// Same Seq on every retry, so the server can spot duplicates.
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, Seq: ck.seq}
	for {
		var reply PutAppendReply
		if ck.server.Call("KVServer."+op, &args, &reply) {
			return reply.Value
		}
	}
}
