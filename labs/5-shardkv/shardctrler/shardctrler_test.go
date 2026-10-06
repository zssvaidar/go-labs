package shardctrler

import (
	"fmt"
	"sync"
	"testing"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
)

func setup(t *testing.T, n int) (*tester.Group, *Clerk) {
	net := labrpc.MakeNetwork()
	g := StartGroup(net, n)
	t.Cleanup(g.Shutdown)
	return g, MakeGroupClerk(g)
}

// check verifies the latest config has exactly these groups and that
// shards are spread as evenly as possible.
func check(t *testing.T, groups []int, ck *Clerk) {
	t.Helper()
	c := ck.Query(-1)
	if len(c.Groups) != len(groups) {
		t.Fatalf("wanted %v groups, got %v", len(groups), len(c.Groups))
	}
	for _, g := range groups {
		if _, ok := c.Groups[g]; !ok {
			t.Fatalf("missing group %v", g)
		}
	}
	// Any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range c.Shards {
			if _, ok := c.Groups[g]; !ok {
				t.Fatalf("shard %v -> invalid group %v", s, g)
			}
		}
	}
	// More or less balanced sharding?
	counts := map[int]int{}
	for _, g := range c.Shards {
		counts[g]++
	}
	lo, hi := NShards+1, -1
	for g := range c.Groups {
		lo = min(lo, counts[g])
		hi = max(hi, counts[g])
	}
	if hi > lo+1 {
		t.Fatalf("max %v too much larger than min %v", hi, lo)
	}
}

func checkSameConfig(t *testing.T, c1, c2 Config) {
	t.Helper()
	if c1.String() != c2.String() {
		t.Fatalf("configs differ:\n%v\n%v", c1, c2)
	}
}

func TestBasic5A(t *testing.T) {
	const nservers = 3
	g, ck := setup(t, nservers)

	var cfa []Config
	cfa = append(cfa, ck.Query(-1))
	check(t, nil, ck)

	gid1, gid2 := 1, 2
	ck.Join(map[int][]string{gid1: {"x", "y", "z"}})
	check(t, []int{gid1}, ck)
	cfa = append(cfa, ck.Query(-1))

	ck.Join(map[int][]string{gid2: {"a", "b", "c"}})
	check(t, []int{gid1, gid2}, ck)
	cfa = append(cfa, ck.Query(-1))

	if sa := ck.Query(-1).Groups[gid1]; len(sa) != 3 || sa[0] != "x" || sa[1] != "y" || sa[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v", gid1, sa)
	}

	ck.Leave([]int{gid1})
	check(t, []int{gid2}, ck)
	cfa = append(cfa, ck.Query(-1))

	ck.Leave([]int{gid2})
	cfa = append(cfa, ck.Query(-1))

	// Historical queries, across controller restarts.
	for s := 0; s < nservers; s++ {
		g.ShutdownServer(s)
		for i := 0; i < len(cfa); i++ {
			checkSameConfig(t, ck.Query(cfa[i].Num), cfa[i])
		}
		g.StartServer(s)
	}

	// Move.
	ck.Join(map[int][]string{503: {"3a", "3b", "3c"}})
	ck.Join(map[int][]string{504: {"4a", "4b", "4c"}})
	for i := 0; i < NShards; i++ {
		cf := ck.Query(-1)
		if i < NShards/2 {
			ck.Move(i, 503)
			if cf.Shards[i] != 503 {
				if ck.Query(-1).Shards[i] != 503 {
					t.Fatalf("Move should go to 503")
				}
			}
		} else {
			ck.Move(i, 504)
		}
	}
	cf2 := ck.Query(-1)
	for i := 0; i < NShards; i++ {
		want := 503
		if i >= NShards/2 {
			want = 504
		}
		if cf2.Shards[i] != want {
			t.Fatalf("expected shard %v on %v, got %v", i, want, cf2.Shards[i])
		}
	}
	ck.Leave([]int{503})
	ck.Leave([]int{504})
}

func TestConcurrentJoinLeave5A(t *testing.T) {
	const npara = 10
	_, ck := setup(t, 3)

	var wg sync.WaitGroup
	var gids []int
	for i := 0; i < npara; i++ {
		gids = append(gids, (i*10)+100)
	}
	clerks := make([]*Clerk, npara)
	for i := range clerks {
		clerks[i] = MakeClerk(ck.servers)
	}
	for i := 0; i < npara; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			gid := gids[i]
			ck := clerks[i]
			ck.Join(map[int][]string{gid + 1000: {"a", "b", "c"}})
			ck.Join(map[int][]string{gid: {"a", "b", "c"}})
			ck.Leave([]int{gid + 1000})
		}(i)
	}
	wg.Wait()
	check(t, gids, ck)
}

// TestMinimalTransfers5A checks that joins and leaves move as few shards
// as possible.
func TestMinimalTransfers5A(t *testing.T) {
	_, ck := setup(t, 3)
	for gid := 1; gid <= 5; gid++ {
		ck.Join(map[int][]string{gid: {fmt.Sprint(gid)}})
	}
	c1 := ck.Query(-1)

	ck.Join(map[int][]string{1001: {"a"}})
	c2 := ck.Query(-1)
	for i := 0; i < NShards; i++ {
		if c2.Shards[i] < 1000 && c1.Shards[i] != c2.Shards[i] {
			t.Fatalf("non-minimal transfer after Join(): shard %d moved %d -> %d",
				i, c1.Shards[i], c2.Shards[i])
		}
	}

	ck.Leave([]int{1001})
	c3 := ck.Query(-1)
	for i := 0; i < NShards; i++ {
		if c2.Shards[i] != 1001 && c2.Shards[i] != c3.Shards[i] {
			t.Fatalf("non-minimal transfer after Leave(): shard %d moved %d -> %d",
				i, c2.Shards[i], c3.Shards[i])
		}
	}
}

// TestMultiJoinLeave5A joins and leaves several groups in one call, and
// more groups than shards.
func TestMultiJoinLeave5A(t *testing.T) {
	_, ck := setup(t, 3)
	groups := map[int][]string{}
	var gids []int
	for gid := 1; gid <= NShards+3; gid++ {
		groups[gid] = []string{fmt.Sprint(gid)}
		gids = append(gids, gid)
	}
	ck.Join(groups)
	check(t, gids, ck)

	ck.Leave(gids[:5])
	check(t, gids[5:], ck)

	ck.Leave(gids[5:])
	check(t, nil, ck)
	for s, g := range ck.Query(-1).Shards {
		if g != 0 {
			t.Fatalf("shard %d assigned to %d with no groups", s, g)
		}
	}
}

func TestRebalanceDeterministic5A(t *testing.T) {
	// Every replica must compute the same config from the same requests,
	// even though map iteration order is random.
	var first [NShards]int
	for i := 0; i < 50; i++ {
		c := Config{Groups: map[int][]string{}}
		for gid := 1; gid <= 4; gid++ {
			c.Groups[gid] = nil
		}
		rebalance(&c)
		if i == 0 {
			first = c.Shards
		} else if c.Shards != first {
			t.Fatalf("rebalance not deterministic: %v vs %v", c.Shards, first)
		}
	}
}
