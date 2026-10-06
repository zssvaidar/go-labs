package main

import (
	"fmt"
	"os"

	mr "github.com/zssvaidar/go-labs/labs/1-mapreduce"
	kvsrv "github.com/zssvaidar/go-labs/labs/2-kvsrv"
	raft "github.com/zssvaidar/go-labs/labs/3-raft"
	kvraft "github.com/zssvaidar/go-labs/labs/4-kvraft"
	"github.com/zssvaidar/go-labs/labs/5-shardkv/shardkv"
)

type lab struct {
	name string
	run  func()
}

// The labs of MIT 6.5840 (Distributed Systems), in order.
var labs = []lab{
	{"mapreduce: coordinator and workers, surviving worker crashes", mr.Demo},
	{"kvsrv: key/value server with at-most-once semantics on a lossy network", kvsrv.Demo},
	{"raft: leader election, log replication, persistence, snapshots", raft.Demo},
	{"kvraft: fault-tolerant key/value service on Raft", kvraft.Demo},
	{"shardkv: sharded key/value service with shard migration", shardkv.Demo},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}
	var n int
	if _, err := fmt.Sscan(os.Args[1], &n); err != nil || n < 1 || n > len(labs) {
		fmt.Fprintf(os.Stderr, "unknown lab %q\n\n", os.Args[1])
		usage()
		os.Exit(1)
	}
	labs[n-1].run()
}

func usage() {
	fmt.Println("usage: go run . <lab>")
	fmt.Println()
	for i, l := range labs {
		fmt.Printf("  %d  %s\n", i+1, l.name)
	}
}
