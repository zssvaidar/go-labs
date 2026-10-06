// Package labrpc is a simulated RPC network, modeled on the one MIT 6.5840
// uses to test its labs. Servers and clients live in one process, but every
// call is gob-encoded like a real RPC, so they never share memory. The
// network can disconnect servers, and in unreliable mode it delays, drops,
// and loses replies to RPCs.
//
//	net := labrpc.MakeNetwork()
//	end := net.MakeEnd("client-1")      // a client's handle to one server
//	srv := labrpc.MakeServer()
//	srv.AddService(labrpc.MakeService(kv)) // exposes kv's RPC methods
//	net.AddServer("server-1", srv)
//	net.Connect("client-1", "server-1")
//	net.Enable("client-1", true)
//	ok := end.Call("KVServer.Get", &args, &reply)
//
// Call returns false if the request or reply was lost. RPC handlers must
// have the signature func (r *T) Method(args *A, reply *R).
package labrpc

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Network struct {
	mu          sync.Mutex
	reliable    bool
	longDelays  bool                  // pause a long time before failing a call to a dead server
	ends        map[string]*ClientEnd // by end name
	enabled     map[string]bool       // by end name
	servers     map[string]*Server    // by server name
	connections map[string]string     // end name -> server name
	count       int64                 // total RPCs, accessed atomically
	bytes       int64                 // total bytes sent, accessed atomically
}

func MakeNetwork() *Network {
	return &Network{
		reliable:    true,
		ends:        map[string]*ClientEnd{},
		enabled:     map[string]bool{},
		servers:     map[string]*Server{},
		connections: map[string]string{},
	}
}

func (net *Network) Reliable(yes bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.reliable = yes
}

func (net *Network) LongDelays(yes bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.longDelays = yes
}

// MakeEnd creates a client end. It can't reach anything until it is
// connected to a server and enabled.
func (net *Network) MakeEnd(endname string) *ClientEnd {
	net.mu.Lock()
	defer net.mu.Unlock()
	if _, ok := net.ends[endname]; ok {
		log.Fatalf("labrpc: MakeEnd: %v already exists", endname)
	}
	e := &ClientEnd{name: endname, net: net}
	net.ends[endname] = e
	net.enabled[endname] = false
	return e
}

func (net *Network) AddServer(servername string, rs *Server) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.servers[servername] = rs
}

// DeleteServer simulates a crash: calls to it fail, and replies from calls
// it is still handling are discarded.
func (net *Network) DeleteServer(servername string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.servers[servername] = nil
}

// Connect routes an end to a server. A server's name stays the same across
// restarts, so ends keep working when it comes back.
func (net *Network) Connect(endname, servername string) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.connections[endname] = servername
}

func (net *Network) Enable(endname string, enabled bool) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.enabled[endname] = enabled
}

// GetCount returns how many RPCs a server has received.
func (net *Network) GetCount(servername string) int {
	net.mu.Lock()
	defer net.mu.Unlock()
	if rs := net.servers[servername]; rs != nil {
		return rs.GetCount()
	}
	return 0
}

func (net *Network) GetTotalCount() int {
	return int(atomic.LoadInt64(&net.count))
}

func (net *Network) GetTotalBytes() int64 {
	return atomic.LoadInt64(&net.bytes)
}

// isServerDead reports whether the end has been disabled or the server it
// was talking to has crashed since the call started.
func (net *Network) isServerDead(endname, servername string, server *Server) bool {
	net.mu.Lock()
	defer net.mu.Unlock()
	return !net.enabled[endname] || net.servers[servername] != server
}

func (net *Network) call(endname, svcMeth string, args, reply any) bool {
	atomic.AddInt64(&net.count, 1)
	argBytes := encode(args)
	atomic.AddInt64(&net.bytes, int64(len(argBytes)))

	net.mu.Lock()
	enabled := net.enabled[endname]
	servername := net.connections[endname]
	var server *Server
	if servername != "" {
		server = net.servers[servername]
	}
	reliable := net.reliable
	longDelays := net.longDelays
	net.mu.Unlock()

	if !enabled || server == nil {
		// Simulate waiting for a timeout on a dead link.
		ms := rand.Intn(100)
		if longDelays {
			ms = rand.Intn(2000)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
		return false
	}

	if !reliable {
		time.Sleep(time.Duration(rand.Intn(27)) * time.Millisecond)
		if rand.Intn(1000) < 100 {
			return false // request lost
		}
	}

	// Run the handler in its own goroutine, so we notice if the server
	// crashes while the handler is blocked (e.g. waiting for Raft).
	type result struct {
		ok    bool
		reply []byte
	}
	ch := make(chan result, 1)
	go func() {
		r, ok := server.dispatch(svcMeth, argBytes)
		ch <- result{ok, r}
	}()
	var res result
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for done := false; !done; {
		select {
		case res = <-ch:
			done = true
		case <-ticker.C:
			if net.isServerDead(endname, servername, server) {
				return false
			}
		}
	}

	if !res.ok || net.isServerDead(endname, servername, server) {
		return false
	}
	if !reliable && rand.Intn(1000) < 100 {
		return false // reply lost
	}
	atomic.AddInt64(&net.bytes, int64(len(res.reply)))
	if err := gob.NewDecoder(bytes.NewBuffer(res.reply)).Decode(reply); err != nil {
		log.Fatalf("labrpc: decode reply for %v: %v", svcMeth, err)
	}
	return true
}

// ClientEnd is a client's handle for calling one server.
type ClientEnd struct {
	name string
	net  *Network
}

// Call sends an RPC and waits for the reply. It returns false if the
// request or reply was lost; then reply must be ignored. Pass a fresh,
// zero-valued reply each time.
func (e *ClientEnd) Call(svcMeth string, args any, reply any) bool {
	return e.net.call(e.name, svcMeth, args, reply)
}

// Server holds one or more services, e.g. "Raft" and "KVServer".
type Server struct {
	mu       sync.Mutex
	services map[string]*Service
	count    int
}

func MakeServer() *Server {
	return &Server{services: map[string]*Service{}}
}

func (rs *Server) AddService(svc *Service) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *Server) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

func (rs *Server) dispatch(svcMeth string, args []byte) ([]byte, bool) {
	rs.mu.Lock()
	rs.count++
	dot := strings.LastIndex(svcMeth, ".")
	var svc *Service
	ok := false
	if dot > 0 {
		svc, ok = rs.services[svcMeth[:dot]]
	}
	rs.mu.Unlock()
	if !ok {
		log.Fatalf("labrpc: unknown service in %q", svcMeth)
	}
	return svc.dispatch(svcMeth[dot+1:], args)
}

// Service exposes an object's RPC methods: exported methods of the form
// func (r *T) Name(args *A, reply *R).
type Service struct {
	name    string
	rcvr    reflect.Value
	methods map[string]reflect.Method
}

func MakeService(rcvr any) *Service {
	svc := &Service{
		name:    reflect.Indirect(reflect.ValueOf(rcvr)).Type().Name(),
		rcvr:    reflect.ValueOf(rcvr),
		methods: map[string]reflect.Method{},
	}
	typ := reflect.TypeOf(rcvr)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		mtype := method.Type
		if method.PkgPath != "" || mtype.NumIn() != 3 || mtype.NumOut() != 0 ||
			mtype.In(1).Kind() != reflect.Ptr || mtype.In(2).Kind() != reflect.Ptr {
			continue // not an RPC handler
		}
		svc.methods[method.Name] = method
	}
	return svc
}

func (svc *Service) dispatch(methname string, args []byte) ([]byte, bool) {
	method, ok := svc.methods[methname]
	if !ok {
		log.Fatalf("labrpc: unknown method %v.%v", svc.name, methname)
	}
	argv := reflect.New(method.Type.In(1).Elem())
	if err := gob.NewDecoder(bytes.NewBuffer(args)).Decode(argv.Interface()); err != nil {
		log.Fatalf("labrpc: decode args for %v.%v: %v", svc.name, methname, err)
	}
	replyv := reflect.New(method.Type.In(2).Elem())
	method.Func.Call([]reflect.Value{svc.rcvr, argv, replyv})
	return encode(replyv.Interface()), true
}

func encode(v any) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		log.Fatalf("labrpc: encode %T: %v", v, err)
	}
	return buf.Bytes()
}
