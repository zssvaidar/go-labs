package kvsrv

import (
	"fmt"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	c := NewCluster(true)
	ck := c.MakeClerk()
	if v := ck.Get("a"); v != "" {
		t.Fatalf("missing key: got %q, want empty", v)
	}
	ck.Put("a", "1")
	if v := ck.Get("a"); v != "1" {
		t.Fatalf("got %q", v)
	}
	if old := ck.Append("a", "2"); old != "1" {
		t.Fatalf("Append returned %q, want old value 1", old)
	}
	if v := ck.Get("a"); v != "12" {
		t.Fatalf("got %q", v)
	}
	ck.Put("a", "x")
	if v := ck.Get("a"); v != "x" {
		t.Fatalf("got %q", v)
	}
}

// checkAppends checks every append from client cli is in v exactly once,
// in order.
func checkAppends(t *testing.T, cli int, v string, count int) {
	t.Helper()
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := fmt.Sprintf("x %d %d y", cli, j)
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("client %d: missing %q in %q", cli, wanted, v)
		}
		if strings.LastIndex(v, wanted) != off {
			t.Fatalf("client %d: duplicate %q", cli, wanted)
		}
		if off <= lastoff {
			t.Fatalf("client %d: %q out of order", cli, wanted)
		}
		lastoff = off
	}
}

// concurrent runs nclients clients that each append to their own key and
// to one shared key, then checks nothing was lost or applied twice.
func concurrent(t *testing.T, reliable bool, nclients, nappends int) {
	c := NewCluster(reliable)
	var wg sync.WaitGroup
	for cli := 0; cli < nclients; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()
			ck := c.MakeClerk()
			key := strconv.Itoa(cli)
			last := ""
			for j := 0; j < nappends; j++ {
				nv := fmt.Sprintf("x %d %d y", cli, j)
				if old := ck.Append(key, nv); old != last {
					t.Errorf("client %d: Append returned %q, want %q", cli, old, last)
					return
				}
				last += nv
				ck.Append("shared", nv)
				if v := ck.Get(key); v != last {
					t.Errorf("client %d: Get = %q, want %q", cli, v, last)
					return
				}
			}
		}(cli)
	}
	wg.Wait()
	if t.Failed() {
		return
	}

	c.SetReliable(true)
	ck := c.MakeClerk()
	shared := ck.Get("shared")
	for cli := 0; cli < nclients; cli++ {
		checkAppends(t, cli, ck.Get(strconv.Itoa(cli)), nappends)
		checkAppends(t, cli, shared, nappends)
	}
}

func TestConcurrent(t *testing.T) {
	concurrent(t, true, 5, 50)
}

// TestUnreliable drops ~10% of requests and ~10% of replies. Without
// deduplication some appends would show up twice.
func TestUnreliable(t *testing.T) {
	concurrent(t, false, 5, 50)
}

// TestMemory checks that the server keeps one record per client, not one
// per request, and doesn't hold on to old Append values.
func TestMemory(t *testing.T) {
	c := NewCluster(false)
	const nclients = 10
	var wg sync.WaitGroup
	big := strings.Repeat("z", 10000)
	for cli := 0; cli < nclients; cli++ {
		wg.Add(1)
		go func(cli int) {
			defer wg.Done()
			ck := c.MakeClerk()
			for j := 0; j < 20; j++ {
				ck.Put(strconv.Itoa(cli), big)
				ck.Append(strconv.Itoa(cli), "x")
			}
		}(cli)
	}
	wg.Wait()
	if _, clients := c.Server.Size(); clients != nclients {
		t.Fatalf("server keeps %d client records, want %d", clients, nclients)
	}
	// Each client's record holds at most one old value (~10KB).
	c.Server.mu.Lock()
	defer c.Server.mu.Unlock()
	total := 0
	for _, r := range c.Server.clients {
		total += len(r.value)
	}
	if total > nclients*(len(big)+1) {
		t.Fatalf("client records hold %d bytes, want at most %d", total, nclients*(len(big)+1))
	}
}

// TestRetryUntilReachable checks that clerks keep retrying while the
// network drops everything, and finish once it recovers.
func TestRetryUntilReachable(t *testing.T) {
	c := NewCluster(true)
	ck := c.MakeClerk()
	ck.Put("k", "a")

	c.net.DeleteServer("kvserver")
	done := make(chan string)
	go func() { done <- ck.Append("k", "b") }()
	select {
	case <-done:
		t.Fatal("Append completed while the server was unreachable")
	case <-time.After(500 * time.Millisecond):
	}

	c.restore()
	if old := <-done; old != "a" {
		t.Fatalf("Append returned %q, want a", old)
	}
	if v := ck.Get("k"); v != "ab" {
		t.Fatalf("got %q, want ab", v)
	}
}

// restore puts the same server instance back on the network (a network
// outage, not a crash: the data survives).
func (c *Cluster) restore() {
	srv := labrpc.MakeServer()
	srv.AddService(labrpc.MakeService(c.Server))
	c.net.AddServer("kvserver", srv)
}
