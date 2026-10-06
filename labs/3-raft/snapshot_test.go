package raft

import (
	"math/rand"
	"testing"
	"time"
)

// maxLogBytes bounds the persisted Raft state when snapshots are on. With a
// snapshot every 10 entries, the log should never hold many more than that.
const maxLogBytes = 2000

func newSnapCluster(t *testing.T, n int, reliable bool) *Cluster {
	t.Helper()
	c := NewCluster(n, reliable, true)
	t.Cleanup(c.Cleanup)
	return c
}

// snapCommon commits batches of entries while a victim server is
// disconnected or crashed, so that when it returns the leader has already
// discarded the entries it needs and must send a snapshot instead.
func snapCommon(t *testing.T, disconnect, reliable, crash bool) {
	const n = 3
	const iters = 20
	c := newSnapCluster(t, n, reliable)
	one(t, c, rand.Int(), n, true)
	leader1 := checkOneLeader(t, c)

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % n
		sender := leader1
		if i%3 == 1 {
			sender = (leader1 + 1) % n
			victim = leader1
		}

		if disconnect {
			c.Disconnect(victim)
			one(t, c, rand.Int(), n-1, true)
		}
		if crash {
			c.Crash(victim)
			one(t, c, rand.Int(), n-1, true)
		}

		// Send enough to get a snapshot.
		for j := 0; j < SnapshotInterval+1; j++ {
			if rf := c.Raft(sender); rf != nil {
				rf.Start(rand.Int())
			}
		}
		// Let the applier threads catch up with the Start()s.
		if !disconnect && !crash {
			one(t, c, rand.Int(), n, true)
		} else {
			one(t, c, rand.Int(), n-1, true)
		}

		if size := c.MaxLogSize(); size >= maxLogBytes {
			t.Fatalf("log size too large: %d bytes", size)
		}
		if disconnect {
			// Reconnect a follower, who maybe needs to be brought up to date
			// with a snapshot.
			c.Connect(victim)
			one(t, c, rand.Int(), n, true)
			leader1 = checkOneLeader(t, c)
		}
		if crash {
			c.Restart(victim)
			one(t, c, rand.Int(), n, true)
			leader1 = checkOneLeader(t, c)
		}
	}
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotBasic3D(t *testing.T) {
	snapCommon(t, false, true, false)
}

func TestSnapshotInstall3D(t *testing.T) {
	snapCommon(t, true, true, false)
}

func TestSnapshotInstallUnreliable3D(t *testing.T) {
	snapCommon(t, true, false, false)
}

func TestSnapshotInstallCrash3D(t *testing.T) {
	snapCommon(t, false, true, true)
}

func TestSnapshotInstallUnCrash3D(t *testing.T) {
	snapCommon(t, false, false, true)
}

// TestSnapshotAllCrash3D crashes and restarts all servers: they must come
// back from their snapshots and keep going.
func TestSnapshotAllCrash3D(t *testing.T) {
	const n = 3
	c := newSnapCluster(t, n, false)
	one(t, c, rand.Int(), n, true)

	for i := 0; i < 5; i++ {
		// Perhaps enough to get a snapshot.
		for j := 0; j < SnapshotInterval+1; j++ {
			one(t, c, rand.Int(), n, true)
		}
		index1 := one(t, c, rand.Int(), n, true)

		for j := 0; j < n; j++ {
			c.Crash(j)
		}
		for j := 0; j < n; j++ {
			c.Restart(j)
		}

		index2 := one(t, c, rand.Int(), n, true)
		if index2 < index1+1 {
			t.Fatalf("index decreased from %d to %d", index1, index2)
		}
	}
}

// TestSnapshotInit3D checks that a restarted server's applier starts after
// its snapshot instead of re-applying entries the snapshot covers.
func TestSnapshotInit3D(t *testing.T) {
	const n = 3
	c := newSnapCluster(t, n, false)
	one(t, c, rand.Int(), n, true)

	for j := 0; j < SnapshotInterval+1; j++ {
		one(t, c, rand.Int(), n, true)
	}
	for j := 0; j < n; j++ {
		c.Crash(j)
	}
	for j := 0; j < n; j++ {
		c.Restart(j)
	}
	// A single op, to get something to be written back to persistent storage.
	one(t, c, rand.Int(), n, true)

	for j := 0; j < n; j++ {
		c.Crash(j)
	}
	for j := 0; j < n; j++ {
		c.Restart(j)
	}
	// Do another op to trigger potential bug.
	one(t, c, rand.Int(), n, true)
	time.Sleep(100 * time.Millisecond)
	if err := c.Err(); err != nil {
		t.Fatal(err)
	}
}
