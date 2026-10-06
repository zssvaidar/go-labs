package shardkv

import (
	"encoding/gob"

	"github.com/zssvaidar/go-labs/labs/5-shardkv/shardctrler"
)

const NShards = shardctrler.NShards

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup" // this group doesn't serve the key's shard (right now)
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrNotReady    Err = "ErrNotReady" // the other group hasn't reached that config yet
)

// key2shard maps a key to its shard: by its first byte.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	return shard % NShards
}

// ---------------------------------------------------------------------------
// Client RPCs

type PutAppendArgs struct {
	Key      string
	Value    string
	Append   bool
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

// ---------------------------------------------------------------------------
// Group-to-group RPCs for moving shards

type FetchShardsArgs struct {
	ConfigNum int // the config in which the caller became the new owner
	Shards    []int
}

type FetchShardsReply struct {
	Err     Err
	Shards  map[int]map[string]string
	LastSeq map[int64]int64 // dedup table, so moved requests aren't re-applied
}

type DeleteShardsArgs struct {
	ConfigNum int
	Shards    []int
}

type DeleteShardsReply struct {
	Err Err
}

// ---------------------------------------------------------------------------
// Operations that go through a group's Raft log, besides client requests.
// Every replica must make the same state changes at the same point in its
// log, so config changes and shard moves are logged too.

// ConfigOp switches the group to the next config.
type ConfigOp struct {
	Config shardctrler.Config
}

// InsertShardsOp installs shard data fetched from the old owner.
type InsertShardsOp struct {
	ConfigNum int
	Shards    map[int]map[string]string
	LastSeq   map[int64]int64
}

// DeleteShardsOp drops shards the new owner has confirmed it installed.
type DeleteShardsOp struct {
	ConfigNum int
	Shards    []int
}

// GCDoneOp records that the old owner deleted its copies.
type GCDoneOp struct {
	ConfigNum int
	Shards    []int
}

func init() {
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
	gob.Register(ConfigOp{})
	gob.Register(InsertShardsOp{})
	gob.Register(DeleteShardsOp{})
	gob.Register(GCDoneOp{})
}
