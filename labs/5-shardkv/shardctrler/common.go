// Package shardctrler is lab 5A: the shard controller. It is a replicated
// service (Raft via rsm) that decides which replica group serves each
// shard. Admins call Join, Leave, and Move; every change produces a new,
// numbered Config. Shard groups and clients Query it to find out who owns
// what.
package shardctrler

import (
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
)

// NShards is the number of shards the key space is split into.
const NShards = 10

// Config says which group serves each shard, and which servers make up each
// group. Configs are numbered; config 0 is empty and has every shard
// assigned to the invalid group 0.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> server names
}

// Copy returns a deep copy, so a new config never shares its map with an
// old one.
func (c Config) Copy() Config {
	nc := Config{Num: c.Num, Shards: c.Shards, Groups: map[int][]string{}}
	for gid, servers := range c.Groups {
		nc.Groups[gid] = append([]string(nil), servers...)
	}
	return nc
}

func (c Config) String() string {
	gids := make([]int, 0, len(c.Groups))
	for gid := range c.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	var parts []string
	for _, gid := range gids {
		var shards []string
		for s, g := range c.Shards {
			if g == gid {
				shards = append(shards, fmt.Sprint(s))
			}
		}
		parts = append(parts, fmt.Sprintf("G%d:[%s]", gid, strings.Join(shards, " ")))
	}
	return fmt.Sprintf("config %d: %s", c.Num, strings.Join(parts, " "))
}

type Err string

const (
	OK             Err = "OK"
	ErrWrongLeader Err = "ErrWrongLeader"
)

type JoinArgs struct {
	Servers  map[int][]string // new gid -> server names
	ClientId int64
	Seq      int64
}

type JoinReply struct {
	Err Err
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	Seq      int64
}

type LeaveReply struct {
	Err Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	Seq      int64
}

type MoveReply struct {
	Err Err
}

type QueryArgs struct {
	Num int // desired config number; -1 or too large means the latest
}

type QueryReply struct {
	Err    Err
	Config Config
}

func init() {
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
}
