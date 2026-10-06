package shardctrler

import (
	"bytes"
	"encoding/gob"
	"log"
	"sort"
	"sync"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
	"github.com/zssvaidar/go-labs/labs/4-kvraft/rsm"
)

type ShardCtrler struct {
	me  int
	rsm *rsm.RSM

	// State machine, changed only by DoOp and Restore.
	mu      sync.Mutex
	configs []Config        // indexed by config number
	lastSeq map[int64]int64 // client -> highest Seq applied
}

// StartServer starts controller replica `me`. The controller's state is
// small, so it never snapshots.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *tester.Persister) *ShardCtrler {
	sc := &ShardCtrler{
		me:      me,
		configs: []Config{{Groups: map[int][]string{}}},
		lastSeq: map[int64]int64{},
	}
	sc.rsm = rsm.MakeRSM(servers, me, persister, -1, sc)
	return sc
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err = sc.submit(*args)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err = sc.submit(*args)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err = sc.submit(*args)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	err, result := sc.rsm.Submit(*args)
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = OK
	reply.Config = result.(Config)
}

func (sc *ShardCtrler) submit(req any) Err {
	if err, _ := sc.rsm.Submit(req); err != rsm.OK {
		return ErrWrongLeader
	}
	return OK
}

// DoOp applies one committed request on every replica.
func (sc *ShardCtrler) DoOp(req any) any {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if q, ok := req.(QueryArgs); ok {
		if q.Num < 0 || q.Num >= len(sc.configs) {
			return sc.configs[len(sc.configs)-1]
		}
		return sc.configs[q.Num]
	}

	// Join, Leave, and Move change the config: dedup them first.
	var clientId, seq int64
	switch r := req.(type) {
	case JoinArgs:
		clientId, seq = r.ClientId, r.Seq
	case LeaveArgs:
		clientId, seq = r.ClientId, r.Seq
	case MoveArgs:
		clientId, seq = r.ClientId, r.Seq
	default:
		log.Fatalf("shardctrler: unknown request %T", req)
	}
	if seq <= sc.lastSeq[clientId] {
		return nil
	}
	sc.lastSeq[clientId] = seq

	next := sc.configs[len(sc.configs)-1].Copy()
	next.Num++
	switch r := req.(type) {
	case JoinArgs:
		for gid, servers := range r.Servers {
			next.Groups[gid] = append([]string(nil), servers...)
		}
		rebalance(&next)
	case LeaveArgs:
		for _, gid := range r.GIDs {
			delete(next.Groups, gid)
			for s, g := range next.Shards {
				if g == gid {
					next.Shards[s] = 0
				}
			}
		}
		rebalance(&next)
	case MoveArgs:
		// No rebalance: the admin asked for this placement explicitly.
		next.Shards[r.Shard] = r.GID
	}
	sc.configs = append(sc.configs, next)
	return nil
}

// rebalance spreads the shards as evenly as possible over the groups while
// moving as few shards as possible. It must be deterministic: Go map
// iteration order is random, so it works on sorted gids.
func rebalance(c *Config) {
	if len(c.Groups) == 0 {
		c.Shards = [NShards]int{}
		return
	}
	owned := map[int][]int{} // gid -> shards it currently has
	gids := make([]int, 0, len(c.Groups))
	for gid := range c.Groups {
		gids = append(gids, gid)
		owned[gid] = nil
	}
	var free []int // shards with no valid owner
	for s, gid := range c.Shards {
		if _, ok := c.Groups[gid]; ok {
			owned[gid] = append(owned[gid], s)
		} else {
			free = append(free, s)
		}
	}

	// Groups that already have the most shards get to keep the extra ones,
	// so fewer shards move. Ties broken by gid.
	sort.Slice(gids, func(i, j int) bool {
		if len(owned[gids[i]]) != len(owned[gids[j]]) {
			return len(owned[gids[i]]) > len(owned[gids[j]])
		}
		return gids[i] < gids[j]
	})
	target := map[int]int{}
	for i, gid := range gids {
		target[gid] = NShards / len(gids)
		if i < NShards%len(gids) {
			target[gid]++
		}
	}

	// Take extra shards away from groups over their target...
	for _, gid := range gids {
		for len(owned[gid]) > target[gid] {
			last := len(owned[gid]) - 1
			free = append(free, owned[gid][last])
			owned[gid] = owned[gid][:last]
		}
	}
	// ...and hand them to groups under it.
	sort.Ints(free)
	for _, gid := range gids {
		for len(owned[gid]) < target[gid] {
			s := free[0]
			free = free[1:]
			owned[gid] = append(owned[gid], s)
			c.Shards[s] = gid
		}
	}
}

func (sc *ShardCtrler) Snapshot() []byte {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if e.Encode(sc.configs) != nil || e.Encode(sc.lastSeq) != nil {
		log.Fatalf("shardctrler: encode snapshot")
	}
	return w.Bytes()
}

func (sc *ShardCtrler) Restore(data []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var configs []Config
	var lastSeq map[int64]int64
	if d.Decode(&configs) != nil || d.Decode(&lastSeq) != nil {
		log.Fatalf("shardctrler: decode snapshot")
	}
	sc.configs = configs
	sc.lastSeq = lastSeq
}

func (sc *ShardCtrler) Raft() interface{ Kill() } {
	return sc.rsm.Raft()
}

func (sc *ShardCtrler) Kill() {
	sc.rsm.Kill()
}
