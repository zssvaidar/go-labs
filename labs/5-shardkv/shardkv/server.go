// Package shardkv is lab 5B: a sharded, fault-tolerant key/value service.
// Keys are split into shards; each shard is served by one replica group (a
// Raft cluster). Groups poll the shard controller for new configs and move
// shards between each other when ownership changes, while still serving
// the shards that aren't moving.
//
// A shard moves from old owner A to new owner B in three steps, each
// committed through the group's own Raft log:
//
//  1. Both groups apply the new config. B marks the shard Pulling; A marks
//     it BePulling and stops serving it, but keeps the data.
//  2. B fetches the data from A and installs it (InsertShardsOp). B starts
//     serving the shard and marks it GCing.
//  3. B tells A to delete its copy (DeleteShardsOp on A). When A confirms,
//     B marks the shard Serving (GCDoneOp).
//
// A group only moves to the next config once all its shards are Serving,
// so a shard is never in two moves at once.
package shardkv

import (
	"bytes"
	"encoding/gob"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
	"github.com/zssvaidar/go-labs/labs/4-kvraft/rsm"
	"github.com/zssvaidar/go-labs/labs/5-shardkv/shardctrler"
)

type ShardState int

const (
	Serving   ShardState = iota // nothing pending: ours and served, or not ours
	Pulling                     // ours in the new config; waiting for the data
	BePulling                   // no longer ours; keeping the data for the new owner
	GCing                       // ours and served; old owner hasn't deleted its copy yet
)

func (s ShardState) String() string {
	return [...]string{"Serving", "Pulling", "BePulling", "GCing"}[s]
}

type Shard struct {
	Data  map[string]string
	State ShardState
}

const pollInterval = 100 * time.Millisecond

type ShardKV struct {
	me      int
	gid     int
	rsm     *rsm.RSM
	mck     *shardctrler.Clerk
	makeEnd func(string) *labrpc.ClientEnd
	dead    int32

	mu   sync.Mutex
	ends map[string]*labrpc.ClientEnd // cached ends to other groups' servers

	// State machine, changed only by DoOp and Restore (called by rsm).
	cur     shardctrler.Config
	prev    shardctrler.Config // the config before cur: who to pull from
	shards  [NShards]Shard
	lastSeq map[int64]int64 // client -> highest Seq applied
}

// StartServer starts server `me` of group gid. servers[i] reaches server i
// of this group, ctrlers reach the shard controller, and makeEnd connects
// to any server by name (for talking to other groups).
func StartServer(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int,
	gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	kv := &ShardKV{
		me:      me,
		gid:     gid,
		mck:     shardctrler.MakeClerk(ctrlers),
		makeEnd: makeEnd,
		ends:    map[string]*labrpc.ClientEnd{},
		cur:     shardctrler.Config{Groups: map[int][]string{}},
		prev:    shardctrler.Config{Groups: map[int][]string{}},
		lastSeq: map[int64]int64{},
	}
	for s := range kv.shards {
		kv.shards[s].Data = map[string]string{}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	go kv.leaderLoop(kv.pollConfig)
	go kv.leaderLoop(kv.pullShards)
	go kv.leaderLoop(kv.gcShards)
	return kv
}

// ---------------------------------------------------------------------------
// Client RPC handlers

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = result.(GetReply)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	*reply = result.(PutAppendReply)
}

// ---------------------------------------------------------------------------
// Group-to-group RPC handlers

// FetchShards returns shard data to the group that now owns it. Any replica
// that has reached the config can answer: from that point on, the data of
// a shard that moved away is frozen.
func (kv *ShardKV) FetchShards(args *FetchShardsArgs, reply *FetchShardsReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.cur.Num < args.ConfigNum {
		reply.Err = ErrNotReady
		return
	}
	reply.Shards = map[int]map[string]string{}
	for _, s := range args.Shards {
		reply.Shards[s] = copyMap(kv.shards[s].Data)
	}
	reply.LastSeq = copyMap(kv.lastSeq)
	reply.Err = OK
}

// DeleteShards is called by the new owner once it has installed shards.
func (kv *ShardKV) DeleteShards(args *DeleteShardsArgs, reply *DeleteShardsReply) {
	kv.mu.Lock()
	done := args.ConfigNum < kv.cur.Num // we moved on, so we deleted them already
	kv.mu.Unlock()
	if done {
		reply.Err = OK
		return
	}
	err, result := kv.rsm.Submit(DeleteShardsOp{ConfigNum: args.ConfigNum, Shards: args.Shards})
	if err != rsm.OK {
		reply.Err = ErrWrongLeader
		return
	}
	reply.Err = result.(Err)
}

// ---------------------------------------------------------------------------
// Background work, done by each group's leader

func (kv *ShardKV) leaderLoop(work func()) {
	for !kv.killed() {
		if _, isLeader := kv.rsm.Raft().GetState(); isLeader {
			work()
		}
		time.Sleep(pollInterval)
	}
}

// pollConfig asks the controller for the next config, once the current
// one is fully installed.
func (kv *ShardKV) pollConfig() {
	kv.mu.Lock()
	idle := kv.allServing()
	num := kv.cur.Num
	kv.mu.Unlock()
	if !idle {
		return
	}
	if next := kv.mck.Query(num + 1); next.Num == num+1 {
		kv.rsm.Submit(ConfigOp{Config: next})
	}
}

// shardsFromPrevOwners groups the shards in `state` by the gid that owned
// them in the previous config.
func (kv *ShardKV) shardsFromPrevOwners(state ShardState) (map[int][]int, int, shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	byGid := map[int][]int{}
	for s := range kv.shards {
		if kv.shards[s].State == state {
			gid := kv.prev.Shards[s]
			byGid[gid] = append(byGid[gid], s)
		}
	}
	return byGid, kv.cur.Num, kv.prev
}

// pullShards fetches the shards we're Pulling from their old owners.
func (kv *ShardKV) pullShards() {
	byGid, num, prev := kv.shardsFromPrevOwners(Pulling)
	var wg sync.WaitGroup
	for gid, shards := range byGid {
		wg.Add(1)
		go func(servers []string, shards []int) {
			defer wg.Done()
			args := FetchShardsArgs{ConfigNum: num, Shards: shards}
			for _, name := range servers {
				var reply FetchShardsReply
				if kv.end(name).Call("ShardKV.FetchShards", &args, &reply) && reply.Err == OK {
					kv.rsm.Submit(InsertShardsOp{ConfigNum: num, Shards: reply.Shards, LastSeq: reply.LastSeq})
					return
				}
			}
		}(prev.Groups[gid], shards)
	}
	wg.Wait()
}

// gcShards tells old owners to delete shards we've installed.
func (kv *ShardKV) gcShards() {
	byGid, num, prev := kv.shardsFromPrevOwners(GCing)
	var wg sync.WaitGroup
	for gid, shards := range byGid {
		wg.Add(1)
		go func(servers []string, shards []int) {
			defer wg.Done()
			args := DeleteShardsArgs{ConfigNum: num, Shards: shards}
			for _, name := range servers {
				var reply DeleteShardsReply
				if kv.end(name).Call("ShardKV.DeleteShards", &args, &reply) && reply.Err == OK {
					kv.rsm.Submit(GCDoneOp{ConfigNum: num, Shards: shards})
					return
				}
			}
		}(prev.Groups[gid], shards)
	}
	wg.Wait()
}

func (kv *ShardKV) end(name string) *labrpc.ClientEnd {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.ends[name] == nil {
		kv.ends[name] = kv.makeEnd(name)
	}
	return kv.ends[name]
}

// ---------------------------------------------------------------------------
// State machine

// DoOp applies one committed operation. Every replica in the group calls
// it with the same operations in the same order.
func (kv *ShardKV) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op := req.(type) {
	case GetArgs:
		s := key2shard(op.Key)
		if !kv.canServe(s) {
			return GetReply{Err: ErrWrongGroup}
		}
		v, ok := kv.shards[s].Data[op.Key]
		if !ok {
			return GetReply{Err: ErrNoKey}
		}
		return GetReply{Err: OK, Value: v}

	case PutAppendArgs:
		s := key2shard(op.Key)
		// Check ownership at apply time, not when the request arrived: the
		// config may have changed in between.
		if !kv.canServe(s) {
			return PutAppendReply{Err: ErrWrongGroup}
		}
		if op.Seq <= kv.lastSeq[op.ClientId] {
			return PutAppendReply{Err: OK}
		}
		kv.lastSeq[op.ClientId] = op.Seq
		if op.Append {
			kv.shards[s].Data[op.Key] += op.Value
		} else {
			kv.shards[s].Data[op.Key] = op.Value
		}
		return PutAppendReply{Err: OK}

	case ConfigOp:
		kv.applyConfig(op.Config)
		return OK

	case InsertShardsOp:
		if op.ConfigNum != kv.cur.Num {
			return OK // a duplicate from an earlier config
		}
		for s, data := range op.Shards {
			if kv.shards[s].State == Pulling {
				kv.shards[s].Data = copyMap(data)
				kv.shards[s].State = GCing
			}
		}
		for client, seq := range op.LastSeq {
			kv.lastSeq[client] = max(kv.lastSeq[client], seq)
		}
		return OK

	case DeleteShardsOp:
		if op.ConfigNum < kv.cur.Num {
			return OK
		}
		if op.ConfigNum > kv.cur.Num {
			return ErrNotReady
		}
		for _, s := range op.Shards {
			if kv.shards[s].State == BePulling {
				kv.shards[s] = Shard{Data: map[string]string{}, State: Serving}
			}
		}
		return OK

	case GCDoneOp:
		if op.ConfigNum == kv.cur.Num {
			for _, s := range op.Shards {
				if kv.shards[s].State == GCing {
					kv.shards[s].State = Serving
				}
			}
		}
		return OK
	}
	log.Fatalf("shardkv: unknown op %T", req)
	return nil
}

func (kv *ShardKV) applyConfig(next shardctrler.Config) {
	if next.Num != kv.cur.Num+1 || !kv.allServing() {
		return // stale or duplicate, or the previous move isn't finished
	}
	for s := 0; s < NShards; s++ {
		oldGid, newGid := kv.cur.Shards[s], next.Shards[s]
		switch {
		case oldGid != kv.gid && newGid == kv.gid:
			if oldGid != 0 {
				kv.shards[s].State = Pulling
			} // else: a brand-new shard, nothing to fetch
		case oldGid == kv.gid && newGid != kv.gid:
			if newGid != 0 {
				kv.shards[s].State = BePulling
			} else {
				// Every group left; nobody will ever ask for this data.
				kv.shards[s] = Shard{Data: map[string]string{}}
			}
		}
	}
	kv.prev = kv.cur
	kv.cur = next.Copy()
}

// canServe says whether this group currently serves shard s.
func (kv *ShardKV) canServe(s int) bool {
	st := kv.shards[s].State
	return kv.cur.Shards[s] == kv.gid && (st == Serving || st == GCing)
}

func (kv *ShardKV) allServing() bool {
	for _, sh := range kv.shards {
		if sh.State != Serving {
			return false
		}
	}
	return true
}

func (kv *ShardKV) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	for _, v := range []any{kv.cur, kv.prev, kv.shards, kv.lastSeq} {
		if err := e.Encode(v); err != nil {
			log.Fatalf("shardkv: encode snapshot: %v", err)
		}
	}
	return w.Bytes()
}

func (kv *ShardKV) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var cur, prev shardctrler.Config
	var shards [NShards]Shard
	var lastSeq map[int64]int64
	if d.Decode(&cur) != nil || d.Decode(&prev) != nil || d.Decode(&shards) != nil || d.Decode(&lastSeq) != nil {
		log.Fatalf("shardkv: decode snapshot")
	}
	// gob turns empty maps into nil maps; we need writable ones.
	for s := range shards {
		if shards[s].Data == nil {
			shards[s].Data = map[string]string{}
		}
	}
	if lastSeq == nil {
		lastSeq = map[int64]int64{}
	}
	kv.cur, kv.prev, kv.shards, kv.lastSeq = cur, prev, shards, lastSeq
}

// Status describes this replica's view, for the demo and tests.
type Status struct {
	ConfigNum int
	States    [NShards]ShardState
	Owned     [NShards]bool
	Keys      int
}

func (kv *ShardKV) Status() Status {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	st := Status{ConfigNum: kv.cur.Num}
	for s, sh := range kv.shards {
		st.States[s] = sh.State
		st.Owned[s] = kv.cur.Shards[s] == kv.gid
		st.Keys += len(sh.Data)
	}
	return st
}

func (kv *ShardKV) Raft() interface {
	GetState() (int, bool)
	Kill()
} {
	return kv.rsm.Raft()
}

func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rsm.Kill()
}

func (kv *ShardKV) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// copyMap returns a fresh, non-nil copy.
func copyMap[K comparable, V any](m map[K]V) map[K]V {
	c := make(map[K]V, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}
