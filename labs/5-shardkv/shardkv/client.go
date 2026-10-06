package shardkv

import (
	"math/rand"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/labs/5-shardkv/shardctrler"
)

// Clerk is a client of the sharded KV service. It asks the controller which
// group owns a key's shard, sends the request there, and asks again if the
// group says the shard isn't its (anymore).
type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	makeEnd  func(string) *labrpc.ClientEnd
	ends     map[string]*labrpc.ClientEnd
	clientId int64
	seq      int64
}

func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	return &Clerk{
		sm:       shardctrler.MakeClerk(ctrlers),
		makeEnd:  makeEnd,
		ends:     map[string]*labrpc.ClientEnd{},
		clientId: rand.Int63(),
	}
}

func (ck *Clerk) end(name string) *labrpc.ClientEnd {
	if ck.ends[name] == nil {
		ck.ends[name] = ck.makeEnd(name)
	}
	return ck.ends[name]
}

// Get fetches the current value for key, or "" if it doesn't exist.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}
	for {
		gid := ck.config.Shards[key2shard(key)]
		for _, name := range ck.config.Groups[gid] {
			var reply GetReply
			ok := ck.end(name).Call("ShardKV.Get", &args, &reply)
			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
				return reply.Value
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key, value string)    { ck.putAppend(key, value, false) }
func (ck *Clerk) Append(key, value string) { ck.putAppend(key, value, true) }

func (ck *Clerk) putAppend(key, value string, isAppend bool) {
	ck.seq++
	// Same Seq on every retry, even at a different group: the shard's
	// dedup table moves with it.
	args := PutAppendArgs{Key: key, Value: value, Append: isAppend, ClientId: ck.clientId, Seq: ck.seq}
	for {
		gid := ck.config.Shards[key2shard(key)]
		for _, name := range ck.config.Groups[gid] {
			var reply PutAppendReply
			ok := ck.end(name).Call("ShardKV.PutAppend", &args, &reply)
			if ok && reply.Err == OK {
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}
