package raft

import (
	"math/rand"
	"sync"
	"testing"
	"time"
)

// electionTimeout is how long the tests give a cluster to elect a leader.
const electionTimeout = 1 * time.Second

func newCluster(t *testing.T, n int, reliable bool) *Cluster {
	t.Helper()
	c := NewCluster(n, reliable, false)
	t.Cleanup(c.Cleanup)
	return c
}

func checkOneLeader(t *testing.T, c *Cluster) int {
	t.Helper()
	l, err := c.CheckOneLeader()
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func one(t *testing.T, c *Cluster, cmd any, expected int, retry bool) int {
	t.Helper()
	idx, err := c.One(cmd, expected, retry)
	if err != nil {
		t.Fatal(err)
	}
	return idx
}

func TestInitialElection3A(t *testing.T) {
	c := newCluster(t, 3, true)
	checkOneLeader(t, c)

	// All servers should agree on the term, and it shouldn't change while
	// nothing fails.
	time.Sleep(50 * time.Millisecond)
	term1, err := c.CheckTerms()
	if err != nil {
		t.Fatal(err)
	}
	if term1 < 1 {
		t.Fatalf("term is %d, but should be at least 1", term1)
	}
	time.Sleep(2 * electionTimeout)
	term2, err := c.CheckTerms()
	if err != nil {
		t.Fatal(err)
	}
	if term1 != term2 {
		t.Logf("warning: term changed from %d to %d with no failures", term1, term2)
	}
	checkOneLeader(t, c)
}

func TestReElection3A(t *testing.T) {
	c := newCluster(t, 3, true)
	leader1 := checkOneLeader(t, c)

	// If the leader disconnects, a new one should be elected.
	c.Disconnect(leader1)
	checkOneLeader(t, c)

	// The old leader rejoining shouldn't disturb the new leader.
	c.Connect(leader1)
	leader2 := checkOneLeader(t, c)

	// With no quorum, there should be no leader.
	c.Disconnect(leader2)
	c.Disconnect((leader2 + 1) % 3)
	time.Sleep(2 * electionTimeout)
	if err := c.CheckNoLeader(); err != nil {
		t.Fatal(err)
	}

	// Restoring a quorum should elect a leader.
	c.Connect((leader2 + 1) % 3)
	checkOneLeader(t, c)

	c.Connect(leader2)
	checkOneLeader(t, c)
}

func TestManyElections3A(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true)
	checkOneLeader(t, c)

	for iter := 0; iter < 5; iter++ {
		// Disconnect three random servers; four is still a majority.
		i1, i2, i3 := rand.Intn(n), rand.Intn(n), rand.Intn(n)
		c.Disconnect(i1)
		c.Disconnect(i2)
		c.Disconnect(i3)
		checkOneLeader(t, c)
		c.Connect(i1)
		c.Connect(i2)
		c.Connect(i3)
	}
	checkOneLeader(t, c)
}

func TestBasicAgree3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	for index := 1; index <= 3; index++ {
		if nd, _ := c.NCommitted(index); nd > 0 {
			t.Fatalf("some servers committed before Start()")
		}
		if got := one(t, c, index*100, n, false); got != index {
			t.Fatalf("got index %d, expected %d", got, index)
		}
	}
}

func TestFollowerFailure3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	one(t, c, 101, n, false)

	// One follower down: the other two still make progress.
	leader1 := checkOneLeader(t, c)
	c.Disconnect((leader1 + 1) % n)
	one(t, c, 102, n-1, false)
	time.Sleep(electionTimeout)
	one(t, c, 103, n-1, false)

	// Both followers down: nothing can commit.
	leader2 := checkOneLeader(t, c)
	c.Disconnect((leader2 + 1) % n)
	c.Disconnect((leader2 + 2) % n)
	index, _, ok := c.Raft(leader2).Start(104)
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 4 {
		t.Fatalf("expected index 4, got %d", index)
	}
	time.Sleep(2 * electionTimeout)
	if nd, _ := c.NCommitted(index); nd > 0 {
		t.Fatalf("%d committed but no majority", nd)
	}
}

func TestFailAgree3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	one(t, c, 101, n, false)

	// Disconnect one follower; the rest keep agreeing.
	leader := checkOneLeader(t, c)
	c.Disconnect((leader + 1) % n)
	for _, cmd := range []int{102, 103, 104, 105} {
		one(t, c, cmd, n-1, false)
	}

	// The follower reconnects and catches up.
	c.Connect((leader + 1) % n)
	one(t, c, 106, n, true)
	one(t, c, 107, n, true)
}

func TestFailNoAgree3B(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)
	one(t, c, 10, n, false)

	// 3 of 5 followers disconnect: no majority.
	leader := checkOneLeader(t, c)
	c.Disconnect((leader + 1) % n)
	c.Disconnect((leader + 2) % n)
	c.Disconnect((leader + 3) % n)

	index, _, ok := c.Raft(leader).Start(20)
	if !ok {
		t.Fatalf("leader rejected Start()")
	}
	if index != 2 {
		t.Fatalf("expected index 2, got %d", index)
	}
	time.Sleep(2 * electionTimeout)
	if nd, _ := c.NCommitted(index); nd > 0 {
		t.Fatalf("%d committed but no majority", nd)
	}

	// Repair: the uncommitted entry may be overwritten, but agreement resumes.
	c.Connect((leader + 1) % n)
	c.Connect((leader + 2) % n)
	c.Connect((leader + 3) % n)
	leader2 := checkOneLeader(t, c)
	index2, _, ok := c.Raft(leader2).Start(30)
	if !ok {
		t.Fatalf("leader2 rejected Start()")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %d", index2)
	}
	one(t, c, 1000, n, true)
}

func TestRejoin3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	one(t, c, 101, n, true)

	// The leader is partitioned and appends entries nobody else sees.
	leader1 := checkOneLeader(t, c)
	c.Disconnect(leader1)
	c.Raft(leader1).Start(102)
	c.Raft(leader1).Start(103)
	c.Raft(leader1).Start(104)

	// The others elect a new leader and commit at index 2.
	one(t, c, 103, 2, true)

	// The new leader is partitioned too; the old one comes back.
	leader2 := checkOneLeader(t, c)
	c.Disconnect(leader2)
	c.Connect(leader1)
	one(t, c, 104, 2, true)

	// Everyone together: the stale entries must have been overwritten.
	c.Connect(leader2)
	one(t, c, 105, n, true)
}

// TestBackup makes followers' logs diverge a lot so the leader has to back
// up nextIndex quickly over many conflicting entries.
func TestBackup3B(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)
	one(t, c, rand.Int(), n, true)

	// Put the leader and one follower in a partition.
	leader1 := checkOneLeader(t, c)
	for _, i := range []int{2, 3, 4} {
		c.Disconnect((leader1 + i) % n)
	}
	// Lots of entries that won't commit.
	for i := 0; i < 50; i++ {
		c.Raft(leader1).Start(rand.Int())
	}
	time.Sleep(electionTimeout / 2)
	c.Disconnect((leader1 + 0) % n)
	c.Disconnect((leader1 + 1) % n)

	// Allow the other partition to recover and commit lots of entries.
	for _, i := range []int{2, 3, 4} {
		c.Connect((leader1 + i) % n)
	}
	for i := 0; i < 50; i++ {
		one(t, c, rand.Int(), 3, true)
	}

	// Now another partitioned leader and one follower.
	leader2 := checkOneLeader(t, c)
	other := (leader1 + 2) % n
	if leader2 == other {
		other = (leader2 + 1) % n
	}
	c.Disconnect(other)

	// Lots more entries that won't commit.
	for i := 0; i < 50; i++ {
		c.Raft(leader2).Start(rand.Int())
	}
	time.Sleep(electionTimeout / 2)

	// Bring the original leader back to life.
	for i := 0; i < n; i++ {
		c.Disconnect(i)
	}
	c.Connect((leader1 + 0) % n)
	c.Connect((leader1 + 1) % n)
	c.Connect(other)

	// Lots of entries that commit.
	for i := 0; i < 50; i++ {
		one(t, c, rand.Int(), 3, true)
	}

	// Now everyone.
	for i := 0; i < n; i++ {
		c.Connect(i)
	}
	one(t, c, rand.Int(), n, true)
}

func TestPersist3C(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	one(t, c, 11, n, true)

	// Crash and restart everyone: committed entries must survive.
	for i := 0; i < n; i++ {
		c.Restart(i)
	}
	one(t, c, 12, n, true)

	// Crash and restart the leader.
	leader1 := checkOneLeader(t, c)
	c.Restart(leader1)
	one(t, c, 13, n, true)

	// Crash the leader, commit without it, restart it.
	leader2 := checkOneLeader(t, c)
	c.Crash(leader2)
	one(t, c, 14, n-1, true)
	c.Restart(leader2)

	// Wait for leader2 to catch up.
	t0 := time.Now()
	for len(c.Applied(leader2)) < 4 {
		if time.Since(t0) > 5*time.Second {
			t.Fatalf("restarted server %d didn't catch up", leader2)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Crash a follower, commit, restart it.
	i3 := (checkOneLeader(t, c) + 1) % n
	c.Crash(i3)
	one(t, c, 15, n-1, true)
	c.Restart(i3)
	one(t, c, 16, n, true)
}

// TestUnreliableAgree submits commands concurrently while the network delays
// and drops RPCs.
func TestUnreliableAgree3C(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for iters := 1; iters < 20; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				if _, err := c.One(100*iters+j, 1, true); err != nil {
					errs <- err
				}
			}(iters, j)
		}
		one(t, c, iters, 1, true)
	}
	c.SetReliable(true)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	one(t, c, 100, n, true)
}

// TestCrashUnreliable randomly crashes, restarts, and partitions servers on
// a lossy network, checking safety throughout.
func TestCrashUnreliable3C(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)
	one(t, c, rand.Int()%10000, 1, true)

	up := make([]bool, n)
	for i := range up {
		up[i] = true
	}
	for iters := 0; iters < 20; iters++ {
		leader := -1
		for i := 0; i < n; i++ {
			if rf := c.Raft(i); rf != nil {
				if _, _, ok := rf.Start(rand.Int()); ok && up[i] {
					leader = i
				}
			}
		}
		if rand.Intn(1000) < 100 {
			time.Sleep(time.Duration(rand.Intn(electionTimeoutMs/2)) * time.Millisecond)
		} else {
			time.Sleep(time.Duration(rand.Intn(13)) * time.Millisecond)
		}
		if leader != -1 && rand.Intn(1000) < 500 {
			c.Crash(leader)
			up[leader] = false
		}
		// Keep a majority up most of the time.
		upCount := 0
		for _, u := range up {
			if u {
				upCount++
			}
		}
		if upCount < (n+1)/2 {
			s := rand.Intn(n)
			if !up[s] {
				c.Restart(s)
				up[s] = true
			}
		}
	}
	for i := 0; i < n; i++ {
		if !up[i] {
			c.Restart(i)
		}
	}
	c.SetReliable(true)
	one(t, c, rand.Int()%10000, n, true)
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
}

const electionTimeoutMs = int(electionTimeout / time.Millisecond)
