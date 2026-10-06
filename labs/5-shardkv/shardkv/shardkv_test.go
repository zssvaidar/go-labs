package shardkv

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const n = 3 // servers per group

func newCluster(t *testing.T, ngroups int, reliable bool, maxraftstate int) *Cluster {
	t.Helper()
	c := NewCluster(ngroups, n, reliable, maxraftstate)
	t.Cleanup(c.Cleanup)
	return c
}

func randValue() string {
	return strconv.Itoa(rand.Int())
}

// keys returns nkeys keys that together cover every shard.
func keys(nkeys int) []string {
	ka := make([]string, nkeys)
	for i := range ka {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
	}
	return ka
}

func check(t *testing.T, ck *Clerk, key, want string) {
	t.Helper()
	if v := ck.Get(key); v != want {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, want, v)
	}
}

// TestStaticShards5B checks that each group serves only its own shards:
// with one group down, half the keys are unreachable and the rest work.
func TestStaticShards5B(t *testing.T) {
	c := newCluster(t, 3, true, -1)
	ck := c.MakeClerk()

	c.Join(0)
	c.Join(1)

	ka, va := keys(10), make([]string, 10)
	for i := range ka {
		va[i] = randValue()
		ck.Put(ka[i], va[i])
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	// Make sure that the data really is sharded by shutting down one group
	// and checking that some Get()s don't succeed.
	c.ShutdownGroup(1)

	var ndone int32
	for i := range ka {
		go func(i int) {
			ck1 := c.MakeClerk()
			if ck1.Get(ka[i]) == va[i] {
				atomic.AddInt32(&ndone, 1)
			}
		}(i)
	}
	time.Sleep(2 * time.Second)
	if got := atomic.LoadInt32(&ndone); got != 5 {
		t.Fatalf("expected 5 completions with one shard dead; got %v", got)
	}

	// Bring the crashed shard/group back to life.
	c.StartGroup(1)
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

// TestJoinLeave5B moves shards as groups join and leave.
func TestJoinLeave5B(t *testing.T) {
	c := newCluster(t, 3, true, -1)
	ck := c.MakeClerk()
	c.Join(0)

	ka, va := keys(10), make([]string, 10)
	for i := range ka {
		va[i] = randValue()
		ck.Put(ka[i], va[i])
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	c.Join(1)
	for i := range ka {
		check(t, ck, ka[i], va[i])
		x := randValue()
		ck.Append(ka[i], x)
		va[i] += x
	}

	c.Leave(0)
	for i := range ka {
		check(t, ck, ka[i], va[i])
		x := randValue()
		ck.Append(ka[i], x)
		va[i] += x
	}

	// Allow time for shards to transfer.
	time.Sleep(time.Second)
	c.ShutdownGroup(0)
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

// TestSnapshot5B moves shards around with snapshots on, then restarts every
// server so they recover from snapshots.
func TestSnapshot5B(t *testing.T) {
	c := newCluster(t, 3, true, 1000)
	ck := c.MakeClerk()
	c.Join(0)

	ka, va := keys(30), make([]string, 30)
	for i := range ka {
		va[i] = randValue()
		ck.Put(ka[i], va[i])
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	c.Join(1)
	c.Join(2)
	c.Leave(0)
	for i := range ka {
		check(t, ck, ka[i], va[i])
		x := randValue()
		ck.Append(ka[i], x)
		va[i] += x
	}

	c.Leave(1)
	c.Join(0)
	for i := range ka {
		check(t, ck, ka[i], va[i])
		x := randValue()
		ck.Append(ka[i], x)
		va[i] += x
	}

	time.Sleep(time.Second)
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(time.Second)
	for gi := 0; gi < 3; gi++ {
		c.ShutdownGroup(gi)
	}
	for gi := 0; gi < 3; gi++ {
		c.StartGroup(gi)
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

// TestMissChange5B crashes a server in every group while configs change,
// so restarted servers must catch up on config changes they missed.
func TestMissChange5B(t *testing.T) {
	c := newCluster(t, 3, true, 1000)
	ck := c.MakeClerk()
	c.Join(0)

	ka, va := keys(10), make([]string, 10)
	for i := range ka {
		va[i] = randValue()
		ck.Put(ka[i], va[i])
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	c.Join(1)
	for gi := 0; gi < 3; gi++ {
		c.ShutdownServer(gi, 0)
	}
	c.Join(2)
	c.Leave(1)
	c.Leave(0)

	appendAll := func() {
		for i := range ka {
			check(t, ck, ka[i], va[i])
			x := randValue()
			ck.Append(ka[i], x)
			va[i] += x
		}
	}
	appendAll()
	c.Join(1)
	appendAll()

	for gi := 0; gi < 3; gi++ {
		c.StartServer(gi, 0)
	}
	appendAll()

	time.Sleep(2 * time.Second)
	for gi := 0; gi < 3; gi++ {
		c.ShutdownServer(gi, 1)
	}
	c.Join(0)
	c.Leave(2)
	appendAll()

	for gi := 0; gi < 3; gi++ {
		c.StartServer(gi, 1)
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

// concurrentTest runs appenders while groups join and leave, optionally
// crashing groups and dropping messages.
func concurrentTest(t *testing.T, reliable, crash bool, maxraftstate int) {
	c := newCluster(t, 3, reliable, maxraftstate)
	ck := c.MakeClerk()
	c.Join(0)

	const nkeys = 20
	ka, va := keys(nkeys), make([]string, nkeys)
	for i := range ka {
		va[i] = randValue()
		ck.Put(ka[i], va[i])
	}

	var done int32
	var wg sync.WaitGroup
	for i := 0; i < nkeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck1 := c.MakeClerk()
			for atomic.LoadInt32(&done) == 0 {
				x := randValue()
				ck1.Append(ka[i], x)
				va[i] += x
				time.Sleep(10 * time.Millisecond)
			}
		}(i)
	}

	// Reconfigure while the appends run.
	for round := 0; round < 3; round++ {
		c.Join(1)
		c.Join(2)
		time.Sleep(time.Duration(rand.Intn(900)) * time.Millisecond)
		c.Leave(0)
		if crash {
			c.ShutdownGroup(0)
			time.Sleep(500 * time.Millisecond)
			c.StartGroup(0)
		}
		c.Join(0)
		c.Leave(1)
		time.Sleep(time.Duration(rand.Intn(900)) * time.Millisecond)
		c.Leave(2)
	}

	time.Sleep(time.Second)
	atomic.StoreInt32(&done, 1)
	wg.Wait()

	c.SetReliable(true)
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

func TestConcurrent1_5B(t *testing.T) { concurrentTest(t, true, false, 1000) }
func TestConcurrent2_5B(t *testing.T) { concurrentTest(t, true, true, 1000) }
func TestUnreliable1_5B(t *testing.T) { concurrentTest(t, false, false, 1000) }
func TestUnreliable2_5B(t *testing.T) { concurrentTest(t, false, true, -1) }

// TestDeleteShards5B checks that old owners delete shards they've handed
// off: total storage stays close to the size of the live data.
func TestDeleteShards5B(t *testing.T) {
	c := newCluster(t, 3, true, 1)
	ck := c.MakeClerk()
	c.Join(0)

	// 30,000 bytes of total values.
	const nkeys = 30
	ka, va := keys(nkeys), make([]string, nkeys)
	for i := range ka {
		va[i] = fmt.Sprintf("%01000d", rand.Int())
		ck.Put(ka[i], va[i])
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}

	for iter := 0; iter < 2; iter++ {
		c.Join(1)
		c.Join(2)
		time.Sleep(2 * time.Second)
		for i := range ka {
			check(t, ck, ka[i], va[i])
		}
		c.Leave(1)
		c.Leave(2)
		time.Sleep(2 * time.Second)
		for i := range ka {
			check(t, ck, ka[i], va[i])
		}
	}
	c.Join(1)
	c.Join(2)
	time.Sleep(time.Second)
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
	time.Sleep(time.Second)

	// Each of the 3 groups x 3 replicas keeps a snapshot and some Raft
	// state. If old owners didn't delete shards, every replica would hold
	// all 30,000 bytes: about 9 x 30,000. Expect about 3 x 30,000.
	const expected = 3 * nkeys * 1000
	if total := c.TotalSize(); total > expected*2 {
		t.Fatalf("persisted state is too big: %v > %v", total, expected*2)
	}
	for i := range ka {
		check(t, ck, ka[i], va[i])
	}
}

// TestUnaffected5B checks that shards that don't move keep being served
// while others are stuck waiting on a dead group.
func TestUnaffected5B(t *testing.T) {
	c := newCluster(t, 2, true, 100)
	ck := c.MakeClerk()

	// JOIN 100
	c.Join(0)
	ka, va := keys(10), make([]string, 10)
	for i := range ka {
		va[i] = "100"
		ck.Put(ka[i], va[i])
	}

	// JOIN 101
	c.Join(1)
	// QUERY to find shards now owned by 101.
	cfg := c.Ctrler().Query(-1)
	owned := map[int]bool{}
	for s, gid := range cfg.Shards {
		if gid == GID(1) {
			owned[s] = true
		}
	}
	// Wait for migration to the new config to complete, and for clients to
	// see it.
	time.Sleep(time.Second)
	for i := range ka {
		if owned[key2shard(ka[i])] {
			va[i] = "101"
			ck.Put(ka[i], va[i])
		}
	}

	// KILL 100, and LEAVE 100 so 101 has to pull its shards from a dead
	// group.
	c.ShutdownGroup(0)
	c.Leave(0)

	// Wait for the config to reach 101.
	time.Sleep(time.Second)

	// Check that 101 can still serve the shards it already owned.
	done := make(chan error, 1)
	go func() {
		for i := range ka {
			if owned[key2shard(ka[i])] {
				if v := ck.Get(ka[i]); v != va[i] {
					done <- fmt.Errorf("Get(%v) = %v, want %v", ka[i], v, va[i])
					return
				}
				ck.Put(ka[i], va[i]+"-1")
				if v := ck.Get(ka[i]); v != va[i]+"-1" {
					done <- fmt.Errorf("Get(%v) = %v, want %v-1", ka[i], v, va[i])
					return
				}
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unaffected shards were not served")
	}
}
