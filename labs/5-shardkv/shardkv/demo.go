package shardkv

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Demo shows shards moving between replica groups as groups join and leave,
// while clients keep reading and writing.
func Demo() {
	c := NewCluster(3, 3, true, 1000)
	defer c.Cleanup()
	ck := c.MakeClerk()
	start := time.Now()
	step := func(format string, args ...any) {
		fmt.Printf("\n[%5.1fs] == %s ==\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
	}

	keys := make([]string, NShards)
	vals := make([]string, NShards)
	for i := range keys {
		keys[i] = strconv.Itoa(i) // keys "0".."9" land on all 10 shards
	}
	checkAll := func() {
		ok := 0
		for i := range keys {
			if ck.Get(keys[i]) == vals[i] {
				ok++
			}
		}
		fmt.Printf("  clients read all %d keys: %d correct\n", len(keys), ok)
	}
	waitSettled := func() {
		for {
			settled := true
			latest := c.Ctrler().Query(-1).Num
			for gi := 0; gi < c.NGroups(); gi++ {
				if kv := c.Leader(gi); kv != nil {
					st := kv.Status()
					for _, s := range st.States {
						settled = settled && s == Serving
					}
					settled = settled && st.ConfigNum == latest
				}
			}
			if settled {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	show := func() {
		waitSettled()
		fmt.Printf("  %v\n", c.Ctrler().Query(-1))
		for gi := 0; gi < c.NGroups(); gi++ {
			kv := c.Leader(gi)
			if kv == nil {
				fmt.Printf("  G%d: down\n", GID(gi))
				continue
			}
			st := kv.Status()
			var owned []string
			for s, o := range st.Owned {
				if o {
					owned = append(owned, strconv.Itoa(s))
				}
			}
			fmt.Printf("  G%d holds %2d keys, serves shards [%s]\n", GID(gi), st.Keys, strings.Join(owned, " "))
		}
	}

	step("1. Group 100 joins: it gets all %d shards", NShards)
	c.Join(0)
	for i := range keys {
		vals[i] = "v" + keys[i]
		ck.Put(keys[i], vals[i])
	}
	show()

	step("2. Groups 101 and 102 join: shards move to them")
	c.Join(1)
	c.Join(2)
	for i := range keys {
		ck.Append(keys[i], "+")
		vals[i] += "+"
	}
	show()
	checkAll()

	step("3. Group 100 leaves: its shards move to 101 and 102, and it deletes its copies")
	c.Leave(0)
	show()

	step("4. Shut group 100 down: nothing depends on it anymore")
	c.ShutdownGroup(0)
	for i := range keys {
		ck.Append(keys[i], "!")
		vals[i] += "!"
	}
	checkAll()
	fmt.Printf("  e.g. key %q = %q\n", keys[3], ck.Get(keys[3]))
}
