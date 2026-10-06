package shardctrler

import (
	"math/rand"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
)

// Clerk talks to the controller replicas, retrying until the leader
// answers.
type Clerk struct {
	servers  []*labrpc.ClientEnd
	leader   int
	clientId int64
	seq      int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{servers: servers, clientId: rand.Int63()}
}

// Query returns config number num, or the latest if num is -1.
func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
	for tried := 0; ; tried++ {
		var reply QueryReply
		if ck.servers[ck.leader].Call("ShardCtrler.Query", &args, &reply) && reply.Err == OK {
			return reply.Config
		}
		ck.nextServer(tried)
	}
}

// Join adds new replica groups, gid -> server names.
func (ck *Clerk) Join(servers map[int][]string) {
	ck.seq++
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, Seq: ck.seq}
	for tried := 0; ; tried++ {
		var reply JoinReply
		if ck.servers[ck.leader].Call("ShardCtrler.Join", &args, &reply) && reply.Err == OK {
			return
		}
		ck.nextServer(tried)
	}
}

// Leave removes replica groups; their shards move to the remaining ones.
func (ck *Clerk) Leave(gids []int) {
	ck.seq++
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, Seq: ck.seq}
	for tried := 0; ; tried++ {
		var reply LeaveReply
		if ck.servers[ck.leader].Call("ShardCtrler.Leave", &args, &reply) && reply.Err == OK {
			return
		}
		ck.nextServer(tried)
	}
}

// Move assigns one shard to a group.
func (ck *Clerk) Move(shard int, gid int) {
	ck.seq++
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, Seq: ck.seq}
	for tried := 0; ; tried++ {
		var reply MoveReply
		if ck.servers[ck.leader].Call("ShardCtrler.Move", &args, &reply) && reply.Err == OK {
			return
		}
		ck.nextServer(tried)
	}
}

func (ck *Clerk) nextServer(tried int) {
	ck.leader = (ck.leader + 1) % len(ck.servers)
	if (tried+1)%len(ck.servers) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
}
