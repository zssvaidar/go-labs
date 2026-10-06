package kvraft

import (
	"fmt"
	"time"
)

// Demo runs a 5-server replicated KV store through crashes and partitions.
func Demo() {
	const n = 5
	c := NewCluster(n, true, 1000)
	defer c.Cleanup()
	ck := c.MakeClerk()
	start := time.Now()
	step := func(format string, args ...any) {
		fmt.Printf("\n[%5.1fs] == %s ==\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
	}
	leader := func() int {
		for {
			if l, ok := c.Leader(); ok {
				return l
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	timed := func(what string, f func() string) {
		t0 := time.Now()
		v := f()
		fmt.Printf("  %-32s -> %-10s (%dms)\n", what, v, time.Since(t0).Milliseconds())
	}

	step("1. Five replicas elect a leader; every op goes through the Raft log")
	timed(`Put("color", "red")`, func() string { ck.Put("color", "red"); return "ok" })
	timed(`Append("color", "!")`, func() string { ck.Append("color", "!"); return "ok" })
	timed(`Get("color")`, func() string { return fmt.Sprintf("%q", ck.Get("color")) })
	l1 := leader()
	fmt.Printf("  leader is S%d\n", l1)

	step("2. Crash the leader S%d: the others elect a new one, data survives", l1)
	c.Crash(l1)
	timed(`Get("color")`, func() string { return fmt.Sprintf("%q", ck.Get("color")) })
	timed(`Put("color", "blue")`, func() string { ck.Put("color", "blue"); return "ok" })
	l2 := leader()
	fmt.Printf("  new leader is S%d\n", l2)

	step("3. Restart S%d, then partition: leader S%d and one follower vs the other three", l1, l2)
	var minority, majority []int
	minority = append(minority, l2)
	for i := 0; i < n; i++ {
		if i == l1 || i == l2 {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, i)
		} else {
			majority = append(majority, i)
		}
	}
	// Bring the crashed server back on the majority side, so it has 3 of 5.
	c.Restart(l1)
	majority = append(majority, l1)
	c.Partition(minority, majority)
	fmt.Printf("  minority %v, majority %v (S%d restarted from its persisted log)\n", minority, majority, l1)

	ckMin := c.MakeClerk()
	c.ConnectClerk(ckMin, minority)
	done := make(chan bool)
	go func() {
		ckMin.Put("color", "green")
		done <- true
	}()
	select {
	case <-done:
		fmt.Println("  BUG: minority committed a Put")
	case <-time.After(1500 * time.Millisecond):
		fmt.Println(`  Put("color", "green") via the minority: no reply after 1.5s (can't commit)`)
	}
	ckMaj := c.MakeClerk()
	c.ConnectClerk(ckMaj, majority)
	timed(`Get("color") via majority`, func() string { return fmt.Sprintf("%q", ckMaj.Get("color")) })
	timed(`Put("size", "XL") via majority`, func() string { ckMaj.Put("size", "XL"); return "ok" })

	step("4. Heal the partition: the stuck Put finally commits")
	c.ConnectAll()
	c.ConnectClerk(ckMin, []int{0, 1, 2, 3, 4})
	<-done
	timed(`Get("color")`, func() string { return fmt.Sprintf("%q", ck.Get("color")) })
	timed(`Get("size")`, func() string { return fmt.Sprintf("%q", ck.Get("size")) })

	step("5. Snapshots keep the Raft log small (maxraftstate = 1000 bytes)")
	for i := 0; i < 200; i++ {
		ck.Append("counter", "+")
	}
	fmt.Printf("  after 200 more appends: largest Raft state %d bytes, largest snapshot %d bytes\n",
		c.MaxRaftStateSize(), c.MaxSnapshotSize())
	c.CrashAll()
	c.RestartAll()
	timed(`restart all, Get("counter")`, func() string {
		v := ck.Get("counter")
		return fmt.Sprintf("%d +'s", len(v))
	})
}
