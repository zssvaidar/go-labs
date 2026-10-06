package labrpc

import (
	"sync"
	"testing"
	"time"
)

type EchoArgs struct{ X int }
type EchoReply struct{ X int }

type Echo struct {
	mu    sync.Mutex
	calls int
	block chan struct{}
}

func (e *Echo) Double(args *EchoArgs, reply *EchoReply) {
	e.mu.Lock()
	e.calls++
	e.mu.Unlock()
	reply.X = 2 * args.X
}

func (e *Echo) Block(args *EchoArgs, reply *EchoReply) {
	<-e.block
}

// Not an RPC handler: wrong signature, so MakeService must skip it.
func (e *Echo) Calls() int { return e.calls }

func setup(t *testing.T) (*Network, *ClientEnd, *Echo) {
	net := MakeNetwork()
	e := &Echo{block: make(chan struct{})}
	t.Cleanup(func() { close(e.block) })
	srv := MakeServer()
	srv.AddService(MakeService(e))
	net.AddServer("s", srv)
	end := net.MakeEnd("c")
	net.Connect("c", "s")
	net.Enable("c", true)
	return net, end, e
}

func TestBasic(t *testing.T) {
	net, end, _ := setup(t)
	var reply EchoReply
	if !end.Call("Echo.Double", &EchoArgs{X: 21}, &reply) {
		t.Fatal("call failed")
	}
	if reply.X != 42 {
		t.Fatalf("got %d, want 42", reply.X)
	}
	if net.GetCount("s") != 1 || net.GetTotalCount() != 1 {
		t.Fatalf("wrong counts")
	}
}

func TestDisabled(t *testing.T) {
	net, end, e := setup(t)
	net.Enable("c", false)
	var reply EchoReply
	if end.Call("Echo.Double", &EchoArgs{X: 1}, &reply) {
		t.Fatal("call on a disabled end succeeded")
	}
	if e.calls != 0 {
		t.Fatal("handler ran on a disabled end")
	}
}

func TestDeletedServerWhileBlocked(t *testing.T) {
	net, end, _ := setup(t)
	done := make(chan bool)
	go func() {
		var reply EchoReply
		done <- end.Call("Echo.Block", &EchoArgs{}, &reply)
	}()
	time.Sleep(50 * time.Millisecond)
	net.DeleteServer("s")
	select {
	case ok := <-done:
		if ok {
			t.Fatal("call to a crashed server succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("call didn't notice the server crashed")
	}
}

func TestUnreliable(t *testing.T) {
	net, end, _ := setup(t)
	net.Reliable(false)
	ok := 0
	for i := 0; i < 300; i++ {
		var reply EchoReply
		if end.Call("Echo.Double", &EchoArgs{X: i}, &reply) {
			ok++
			if reply.X != 2*i {
				t.Fatalf("bad reply %d", reply.X)
			}
		}
	}
	// About 81% should get through (10% request loss, 10% reply loss).
	if ok < 200 || ok > 290 {
		t.Fatalf("%d/300 calls succeeded, expected about 243", ok)
	}
}
