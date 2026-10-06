package kvsrv

import (
	"fmt"
	"strings"
	"sync"
)

// Demo shows why a server needs to deduplicate requests on a lossy network.
func Demo() {
	c := NewCluster(false)
	fmt.Println("== Unreliable network: ~10% of requests and ~10% of replies are lost")
	fmt.Println("   A lost reply means the server DID apply the Append, but the client")
	fmt.Println("   can't tell and retries. Without dedup, the value would be appended twice.")

	const nclients, nappends = 3, 20
	var wg sync.WaitGroup
	for cli := 0; cli < nclients; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()
			ck := c.MakeClerk()
			for j := 0; j < nappends; j++ {
				ck.Append("log", fmt.Sprintf("[c%d:%d]", cli, j))
			}
		}(cli)
	}
	wg.Wait()

	ops := nclients * nappends
	fmt.Printf("\n== %d clients x %d appends = %d operations took %d RPCs (%d retries)\n",
		nclients, nappends, ops, c.RPCCount(), c.RPCCount()-ops)

	c.SetReliable(true)
	v := c.MakeClerk().Get("log")
	dups := 0
	for cli := 0; cli < nclients; cli++ {
		for j := 0; j < nappends; j++ {
			if n := strings.Count(v, fmt.Sprintf("[c%d:%d]", cli, j)); n != 1 {
				dups++
			}
		}
	}
	fmt.Printf("== Final value has %d entries, %d missing or duplicated\n", strings.Count(v, "["), dups)
	fmt.Printf("   %s...\n", v[:min(len(v), 72)])

	_, clients := c.Server.Size()
	fmt.Printf("\n== Server memory for dedup: %d records (one per client, not per request)\n", clients)
}
