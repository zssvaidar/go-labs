package main

import (
	"fmt"
	"os"

	periodic "github.com/zssvaidar/go-labs/labs/1-periodic"
	raft "github.com/zssvaidar/go-labs/labs/2-raft"
)

type lab struct {
	name string
	run  func()
}

var labs = map[string]lab{
	"1": {"periodic: cancel a goroutine with a mutex-protected flag", periodic.Run},
	"2": {"raft: leader election, log replication, crashes", raft.Demo},
}

func main() {
	id := "1"
	if len(os.Args) > 1 {
		id = os.Args[1]
	}
	l, ok := labs[id]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown lab %q, available:\n", id)
		for _, k := range []string{"1", "2"} {
			fmt.Fprintf(os.Stderr, "  %s  %s\n", k, labs[k].name)
		}
		os.Exit(1)
	}
	l.run()
}
