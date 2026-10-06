package raft

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Demo walks a 5-server cluster through the scenarios from the lecture:
// election, replication, leader crash, losing the majority, and recovery.
func Demo() {
	c := NewCluster(5, true, false)
	defer c.Cleanup()
	start := time.Now()

	step := func(format string, args ...any) {
		fmt.Printf("\n[%5.1fs] == %s ==\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
	}
	must := func(err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	}
	leader := func() int {
		l, err := c.CheckOneLeader()
		must(err)
		return l
	}
	commit := func(cmd string, expected int) {
		idx, err := c.One(cmd, expected, true)
		must(err)
		fmt.Printf("  committed %q at index %d on >= %d servers\n", cmd, idx, expected)
	}

	step("1. Leader election: all 5 servers start as followers")
	l1 := leader()
	c.PrintStatus()

	step("2. Log replication: the leader replicates 3 commands to everyone")
	for _, cmd := range []string{"x=1", "x=2", "x=3"} {
		commit(cmd, 5)
	}
	c.PrintStatus()

	step("3. Leader failure: crash S%d, the rest elect a new leader", l1)
	c.Crash(l1)
	l2 := leader()
	commit("x=4", 4)
	c.PrintStatus()

	step("4. No majority: disconnect 2 more servers, leaving 2 of 5")
	var cut []int
	for i := 0; i < c.N() && len(cut) < 2; i++ {
		if i != l1 && i != l2 {
			c.Disconnect(i)
			cut = append(cut, i)
		}
	}
	idx, _, _ := c.Raft(l2).Start("x=5")
	fmt.Printf("  leader S%d appended \"x=5\" at index %d, waiting 2s...\n", l2, idx)
	time.Sleep(2 * time.Second)
	n, _ := c.NCommitted(idx)
	fmt.Printf("  servers that applied index %d: %d (needs a majority of 3, so nothing commits)\n", idx, n)
	c.PrintStatus()

	step("5. Reconnect S%d and S%d: majority is back", cut[0], cut[1])
	for _, i := range cut {
		c.Connect(i)
	}
	// While cut off, the two servers kept timing out and bumping their term.
	// Their higher term forces a new election, which only a server holding
	// "x=5" can win (the election restriction).
	fmt.Printf("  new leader after re-election: S%d\n", leader())
	// "x=5" commits along with a new entry from the current term (Figure 8).
	commit("x=6", 4)
	n, cmd := c.NCommitted(idx)
	fmt.Printf("  index %d (%v) is now applied on %d servers\n", idx, cmd, n)
	c.PrintStatus()

	step("6. Restart S%d: it reloads its persisted log and catches up", l1)
	c.Restart(l1)
	commit("x=7", 5)
	c.PrintStatus()

	step("Every server applied the same commands in the same order")
	for i := 0; i < c.N(); i++ {
		fmt.Printf("  S%d: %v\n", i, c.Applied(i))
	}
	must(c.Err())
}

// PrintStatus prints one line per server: role, term, commit index, and the
// log as term:command pairs.
func (c *Cluster) PrintStatus() {
	for i := 0; i < c.n; i++ {
		rf := c.Raft(i)
		if rf == nil {
			fmt.Printf("  S%d  crashed\n", i)
			continue
		}
		s := rf.Status()
		var entries []string
		for _, e := range s.Log {
			entries = append(entries, fmt.Sprintf("%d:%v", e.Term, e.Command))
		}
		net := ""
		if !c.IsConnected(i) {
			net = "  (disconnected)"
		}
		fmt.Printf("  S%d  %-9s term=%-2d commit=%-2d log=[%s]%s\n",
			i, s.State, s.Term, s.CommitIndex, strings.Join(entries, " "), net)
	}
}
