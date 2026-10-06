package kvraft

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const electionTimeout = 1 * time.Second

func newCluster(t *testing.T, n int, reliable bool, maxraftstate int) *Cluster {
	t.Helper()
	c := NewCluster(n, reliable, maxraftstate)
	t.Cleanup(c.Cleanup)
	return c
}

// checkClntAppends checks that every append client `clnt` made to its key
// is in v exactly once, in order.
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	t.Helper()
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := fmt.Sprintf("x %d %d y", clnt, j)
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		if off1 := strings.LastIndex(v, wanted); off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// runClients starts nclients clients that each append to (and read) their
// own key until done is set, and returns how many appends each made.
func runClients(c *Cluster, nclients int, done *int32, errs chan<- error) (wait func() []int) {
	var wg sync.WaitGroup
	counts := make([]int, nclients)
	for cli := 0; cli < nclients; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()
			ck := c.MakeClerk()
			defer c.DeleteClerk(ck)
			key := strconv.Itoa(cli)
			last := ""
			ck.Put(key, last)
			j := 0
			for atomic.LoadInt32(done) == 0 {
				if rand.Intn(2) == 0 {
					nv := fmt.Sprintf("x %d %d y", cli, j)
					ck.Append(key, nv)
					last += nv
					j++
				} else if v := ck.Get(key); v != last {
					errs <- fmt.Errorf("get wrong value, key %v, wanted:\n%v\n, got\n%v", key, last, v)
					return
				}
			}
			counts[cli] = j
		}(cli)
	}
	return func() []int {
		wg.Wait()
		return counts
	}
}

// genericTest runs clients for a few rounds while optionally partitioning
// the network, crashing servers, and dropping messages, then checks every
// client's appends survived exactly once.
func genericTest(t *testing.T, nclients int, unreliable, crash, partitions bool, maxraftstate int) {
	const nservers = 5
	c := newCluster(t, nservers, !unreliable, maxraftstate)
	ck := c.MakeClerk()

	for round := 0; round < 3; round++ {
		var done int32
		errs := make(chan error, nclients)
		wait := runClients(c, nclients, &done, errs)

		stopPartitioner := make(chan struct{})
		partitionerDone := make(chan struct{})
		go func() {
			defer close(partitionerDone)
			if !partitions {
				return
			}
			for {
				select {
				case <-stopPartitioner:
					return
				default:
				}
				c.RandomPartition()
				time.Sleep(electionTimeout + time.Duration(rand.Intn(200))*time.Millisecond)
			}
		}()

		time.Sleep(2 * time.Second)
		atomic.StoreInt32(&done, 1)
		close(stopPartitioner)
		<-partitionerDone
		c.ConnectAll()

		if crash {
			c.CrashAll()
			// Wait for a while for servers to shut down, since shutdown
			// isn't a real crash and isn't instantaneous.
			time.Sleep(electionTimeout)
			c.RestartAll()
		}

		counts := wait()
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
		}
		for cli, count := range counts {
			v := ck.Get(strconv.Itoa(cli))
			checkClntAppends(t, cli, v, count)
		}

		if maxraftstate > 0 {
			// Check that the logs aren't too big: allow 8x the threshold
			// for state that's committed but not yet snapshotted.
			if sz := c.MaxRaftStateSize(); sz > 8*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
			}
		}
		if maxraftstate < 0 {
			if sz := c.MaxSnapshotSize(); sz > 0 {
				t.Fatalf("snapshot too large (%v), should not be used when maxraftstate = %d", sz, maxraftstate)
			}
		}
	}
}

func TestBasic4A(t *testing.T)      { genericTest(t, 1, false, false, false, -1) }
func TestConcurrent4A(t *testing.T) { genericTest(t, 5, false, false, false, -1) }
func TestUnreliable4A(t *testing.T) { genericTest(t, 5, true, false, false, -1) }
func TestPartitions4A(t *testing.T) { genericTest(t, 5, false, false, true, -1) }
func TestPersist4A(t *testing.T)    { genericTest(t, 5, false, true, false, -1) }
func TestPersistPartitionUnreliable4A(t *testing.T) {
	genericTest(t, 5, true, true, true, -1)
}

// TestOnePartition4A checks that a minority can't make progress, and that
// operations started in it complete once the partition heals.
func TestOnePartition4A(t *testing.T) {
	const nservers = 5
	c := newCluster(t, nservers, true, -1)
	ck := c.MakeClerk()
	ck.Put("1", "13")

	// Find a leader, and put it on the majority side.
	time.Sleep(electionTimeout)
	leader, ok := c.Leader()
	if !ok {
		t.Fatal("no leader")
	}
	var p1, p2 []int
	p1 = append(p1, leader)
	for i := 0; i < nservers; i++ {
		if i == leader {
			continue
		}
		if len(p1) < 3 {
			p1 = append(p1, i)
		} else {
			p2 = append(p2, i)
		}
	}
	c.Partition(p1, p2)

	ckp1 := c.MakeClerk()
	ckp2a := c.MakeClerk()
	ckp2b := c.MakeClerk()
	c.ConnectClerk(ckp1, p1)
	c.ConnectClerk(ckp2a, p2)
	c.ConnectClerk(ckp2b, p2)

	// Progress in the majority.
	ckp1.Put("1", "14")
	if v := ckp1.Get("1"); v != "14" {
		t.Fatalf("got %q, want 14", v)
	}

	// No progress in the minority.
	done0 := make(chan bool)
	done1 := make(chan bool)
	go func() {
		ckp2a.Put("1", "15")
		done0 <- true
	}()
	go func() {
		ckp2b.Get("1") // different clerk in p2
		done1 <- true
	}()
	select {
	case <-done0:
		t.Fatal("Put in minority completed")
	case <-done1:
		t.Fatal("Get in minority completed")
	case <-time.After(time.Second):
	}

	// Completion after heal.
	c.ConnectAll()
	c.ConnectClerk(ckp2a, []int{0, 1, 2, 3, 4})
	c.ConnectClerk(ckp2b, []int{0, 1, 2, 3, 4})
	time.Sleep(electionTimeout)
	for _, ch := range []chan bool{done0, done1} {
		select {
		case <-ch:
		case <-time.After(3 * time.Second):
			t.Fatal("operation didn't complete after the partition healed")
		}
	}
	if v := ck.Get("1"); v != "15" {
		t.Fatalf("got %q, want 15", v)
	}
}

// TestSnapshotRPC4B checks that a server that was cut off while the log was
// compacted catches up via InstallSnapshot.
func TestSnapshotRPC4B(t *testing.T) {
	const nservers = 3
	const maxraftstate = 1000
	c := newCluster(t, nservers, true, maxraftstate)
	ck := c.MakeClerk()
	ck.Put("a", "A")
	if v := ck.Get("a"); v != "A" {
		t.Fatalf("got %q", v)
	}

	// A bunch of puts into the majority partition.
	c.Partition([]int{0, 1}, []int{2})
	ck1 := c.MakeClerk()
	c.ConnectClerk(ck1, []int{0, 1})
	for i := 0; i < 50; i++ {
		ck1.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	time.Sleep(electionTimeout)
	ck1.Put("b", "B")

	// Check that the majority partition has thrown away most of its log.
	if sz := c.MaxRaftStateSize(); sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// Now make a group that requires participation of the lagging server,
	// so that it has to catch up.
	c.Partition([]int{0, 2}, []int{1})
	ck2 := c.MakeClerk()
	c.ConnectClerk(ck2, []int{0, 2})
	ck2.Put("c", "C")
	ck2.Put("d", "D")
	for k, want := range map[string]string{"a": "A", "b": "B", "1": "1", "49": "49"} {
		if v := ck2.Get(k); v != want {
			t.Fatalf("get %q = %q, want %q", k, v, want)
		}
	}

	// Now everybody.
	c.Partition([]int{0, 1, 2}, nil)
	ck.Put("e", "E")
	if v := ck.Get("c"); v != "C" {
		t.Fatalf("got %q", v)
	}
	if v := ck.Get("e"); v != "E" {
		t.Fatalf("got %q", v)
	}
}

// TestSnapshotSize4B checks that both the Raft state and the snapshot stay
// small when the data set is small.
func TestSnapshotSize4B(t *testing.T) {
	const maxraftstate = 1000
	const maxsnapshotstate = 500
	c := newCluster(t, 3, true, maxraftstate)
	ck := c.MakeClerk()
	for i := 0; i < 200; i++ {
		ck.Put("x", "0")
		ck.Put("x", "1")
	}
	if sz := c.MaxRaftStateSize(); sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}
	if sz := c.MaxSnapshotSize(); sz > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", sz, maxsnapshotstate)
	}
}

func TestSnapshotRecover4B(t *testing.T) {
	genericTest(t, 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients4B(t *testing.T) {
	genericTest(t, 20, false, true, false, 1000)
}

func TestSnapshotUnreliable4B(t *testing.T) {
	genericTest(t, 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition4B(t *testing.T) {
	genericTest(t, 5, true, true, true, 1000)
}
