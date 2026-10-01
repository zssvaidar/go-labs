// Package raft is lab 2: the Raft consensus algorithm, following Figure 2 of
// the Raft paper (https://raft.github.io/raft.pdf) and the MIT 6.5840 lab.
//
// It covers leader election, heartbeats, log replication, commit, and
// persistence across crashes. Snapshots are not implemented.
package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	default:
		return "Leader"
	}
}

const (
	// HeartbeatInterval must be well below the election timeout so followers
	// hear from a healthy leader before they give up on it.
	HeartbeatInterval  = 100 * time.Millisecond
	electionTimeoutMin = 300 * time.Millisecond
	electionTimeoutMax = 600 * time.Millisecond
	tickInterval       = 10 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command any
}

// ApplyMsg is sent on applyCh once an entry is committed, in log order.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
}

// Status is a read-only copy of a server's state, for printing and tests.
type Status struct {
	State       State
	Term        int
	CommitIndex int
	Log         []LogEntry // Log[0] is a dummy entry
}

type Raft struct {
	mu        sync.Mutex
	net       *Network
	persister *Persister
	me        int
	n         int
	dead      int32

	// Persistent state on all servers (saved before answering RPCs).
	currentTerm int
	votedFor    int        // -1 if none in currentTerm
	log         []LogEntry // log[0] is a dummy so real entries start at index 1

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders, reset after each election.
	nextIndex  []int // next log index to send to each server
	matchIndex []int // highest index known replicated on each server

	state           State
	lastHeard       time.Time // last time we heard from a leader or granted a vote
	electionTimeout time.Duration
	lastBroadcast   time.Time // last time the leader sent AppendEntries

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // signalled when commitIndex advances
}

// Make creates server `me` and starts its background goroutines. If the
// persister holds state from before a crash, the server resumes from it.
func Make(net *Network, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		net:        net,
		persister:  persister,
		me:         me,
		n:          net.Size(),
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make([]int, net.Size()),
		matchIndex: make([]int, net.Size()),
		state:      Follower,
		applyCh:    applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	rf.resetElectionTimer()

	go rf.ticker()
	go rf.applier()
	return rf
}

// GetState returns the current term and whether this server thinks it is
// the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		State:       rf.state,
		Term:        rf.currentTerm,
		CommitIndex: rf.commitIndex,
		Log:         append([]LogEntry(nil), rf.log...),
	}
}

// Start asks the server to append command to the replicated log. It returns
// immediately, without waiting for the entry to commit. If this server isn't
// the leader it returns false. Even if it is, the entry may never commit
// (for example if the leader loses an election first).
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index = rf.lastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// Replicate right away instead of waiting for the next heartbeat.
	rf.broadcastAppendEntries()
	rf.advanceCommitIndex() // a one-server cluster commits on its own
	return index, rf.currentTerm, true
}

// Kill stops the server's goroutines. Like lab 1's done flag, but atomic.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast() // wake the applier so it can exit
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// ---------------------------------------------------------------------------
// Background loops

// ticker is lab 1's periodic loop: every tick a follower or candidate checks
// whether the election timeout has passed, and a leader checks whether a
// heartbeat is due.
func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()
		switch {
		case rf.state == Leader && time.Since(rf.lastBroadcast) >= HeartbeatInterval:
			rf.broadcastAppendEntries()
		case rf.state != Leader && time.Since(rf.lastHeard) >= rf.electionTimeout:
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(tickInterval)
	}
}

// applier sends committed entries to applyCh in order. It doesn't hold the
// lock while sending, because the receiver may be slow.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// ---------------------------------------------------------------------------
// State transitions (all called with rf.mu held)

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
	spread := int64(electionTimeoutMax - electionTimeoutMin)
	// Random timeouts make it unlikely that two servers start elections at
	// the same moment and split the vote forever.
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(spread))
}

// becomeFollower is called whenever we see a term at least as new as ours
// from a legitimate source. A newer term always wins.
func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	// Announce leadership immediately so others don't start elections.
	rf.broadcastAppendEntries()
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.log) - 1
}

// ---------------------------------------------------------------------------
// Leader election

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer() // if this election fails, try again later

	term := rf.currentTerm
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastLogIndex(),
		LastLogTerm:  rf.log[rf.lastLogIndex()].Term,
	}
	votes := 1 // our own; guarded by rf.mu
	if votes > rf.n/2 {
		rf.becomeLeader()
		return
	}

	for p := 0; p < rf.n; p++ {
		if p == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.net.call(rf, p, func(peer *Raft) { peer.RequestVote(&args, &reply) }) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// The reply may arrive after this election is over: only count
			// it if we're still a candidate in the same term.
			if rf.state != Candidate || rf.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if votes > rf.n/2 {
				rf.becomeLeader()
			}
		}(p)
	}
}

// RequestVote is the RPC handler a candidate calls to ask for our vote.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return // stale candidate
	}

	// Election restriction (§5.4.1): only vote for a candidate whose log is
	// at least as up-to-date as ours, so a new leader has every committed
	// entry.
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.log[lastIndex].Term
	upToDate := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}

// ---------------------------------------------------------------------------
// Log replication and heartbeats

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of the entry just before Entries
	PrevLogTerm  int
	Entries      []LogEntry // empty for a heartbeat
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// On failure, where the leader should retry from, so it can skip back a
	// whole term at a time instead of one entry per round trip.
	ConflictTerm  int // term of the conflicting entry, or -1 if our log is too short
	ConflictIndex int // first index of ConflictTerm in our log, or our log length
}

func (rf *Raft) broadcastAppendEntries() {
	rf.lastBroadcast = time.Now()
	for p := 0; p < rf.n; p++ {
		if p != rf.me {
			go rf.replicateTo(p, rf.currentTerm)
		}
	}
}

// replicateTo sends one AppendEntries to peer p carrying everything from
// nextIndex[p] onward, and handles the reply.
func (rf *Raft) replicateTo(p int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	prev := rf.nextIndex[p] - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.log[prev].Term,
		// Copy so the receiver never shares memory with our log.
		Entries:      append([]LogEntry(nil), rf.log[prev+1:]...),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.net.call(rf, p, func(peer *Raft) { peer.AppendEntries(&args, &reply) }) {
		return // the next heartbeat will retry
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return
	}

	if reply.Success {
		match := args.PrevLogIndex + len(args.Entries)
		// Replies can arrive out of order; never move backwards.
		if match > rf.matchIndex[p] {
			rf.matchIndex[p] = match
		}
		if match+1 > rf.nextIndex[p] {
			rf.nextIndex[p] = match + 1
		}
		rf.advanceCommitIndex()
		return
	}

	// Rejected: the follower's log doesn't match at PrevLogIndex. Back up
	// nextIndex, unless another reply already moved it.
	if rf.nextIndex[p] != args.PrevLogIndex+1 {
		return
	}
	next := reply.ConflictIndex
	if reply.ConflictTerm != -1 {
		// If we have entries from ConflictTerm, resume after our last one.
		for i := args.PrevLogIndex; i > 0; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
			if rf.log[i].Term < reply.ConflictTerm {
				break
			}
		}
	}
	rf.nextIndex[p] = max(1, min(next, rf.lastLogIndex()+1))
	go rf.replicateTo(p, term) // retry now rather than at the next heartbeat
}

// AppendEntries is the RPC handler the leader calls to replicate entries.
// An empty Entries is a heartbeat.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return // stale leader
	}
	// A valid leader exists for this term: follow it, and don't start an
	// election while it keeps talking to us.
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Consistency check: our log must contain the entry before the new ones.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = len(rf.log)
		return
	}
	if t := rf.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return
	}

	// Append new entries. Only truncate on an actual conflict: a delayed,
	// older AppendEntries must not erase entries a newer one added.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastLogIndex() {
			if rf.log[idx].Term == e.Term {
				continue
			}
			rf.log = rf.log[:idx]
		}
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
		break
	}
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		// We only know our log matches the leader's up to the last new entry.
		newCommit := min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}
}

// advanceCommitIndex commits the highest index stored on a majority. A
// leader only commits entries from its own term by counting replicas
// (§5.4.2, Figure 8); older entries become committed along with them.
func (rf *Raft) advanceCommitIndex() {
	for idx := rf.lastLogIndex(); idx > rf.commitIndex; idx-- {
		if rf.log[idx].Term != rf.currentTerm {
			break
		}
		count := 0
		for _, m := range rf.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count > rf.n/2 {
			rf.commitIndex = idx
			rf.applyCond.Broadcast()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Persistence

// persist saves the state that must survive a crash. Call it with rf.mu held
// after changing currentTerm, votedFor, or log, before replying to anyone.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		log.Fatalf("persist: %v", err)
	}
	if err := e.Encode(rf.votedFor); err != nil {
		log.Fatalf("persist: %v", err)
	}
	if err := e.Encode(rf.log); err != nil {
		log.Fatalf("persist: %v", err)
	}
	rf.persister.Save(w.Bytes())
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return // fresh server
	}
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var currentTerm, votedFor int
	var entries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&entries) != nil {
		log.Fatalf("readPersist: corrupt state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = entries
}
