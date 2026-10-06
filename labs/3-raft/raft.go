// Package raft is lab 3: the Raft consensus algorithm, following Figure 2 of
// the Raft paper (https://raft.github.io/raft.pdf) and the MIT 6.5840 lab.
//
// It covers leader election (3A), log replication (3B), persistence (3C),
// and log compaction with snapshots (3D). Servers talk over labrpc, so
// every RPC is copied and can be lost.
package raft

import (
	"bytes"
	"encoding/gob"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zssvaidar/go-labs/internal/labrpc"
	"github.com/zssvaidar/go-labs/internal/tester"
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

// ApplyMsg is what Raft sends the service on applyCh: either a committed
// command (in log order), or a snapshot from the leader that replaces the
// service's state.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Status is a read-only copy of a server's state, for printing and tests.
type Status struct {
	State             State
	Term              int
	CommitIndex       int
	LastIncludedIndex int        // last index covered by the snapshot
	Log               []LogEntry // entries after LastIncludedIndex
}

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int
	dead      int32

	// Persistent state on all servers (saved before answering RPCs).
	currentTerm int
	votedFor    int // -1 if none in currentTerm
	// log[0] stands for the last entry covered by the snapshot: it sits at
	// index lastIncludedIndex and holds its term. Real entries follow it.
	// Before any snapshot, lastIncludedIndex is 0 and log[0] is a dummy.
	log               []LogEntry
	lastIncludedIndex int
	snapshot          []byte

	// Volatile state on all servers.
	commitIndex     int
	lastApplied     int
	snapshotPending bool // a snapshot from the leader is waiting to go to applyCh

	// Volatile state on leaders, reset after each election.
	nextIndex  []int // next log index to send to each server
	matchIndex []int // highest index known replicated on each server

	state           State
	lastHeard       time.Time // last time we heard from a leader or granted a vote
	electionTimeout time.Duration
	lastBroadcast   time.Time // last time the leader sent AppendEntries

	applyCh   chan ApplyMsg
	applyCond *sync.Cond // signalled when there's something to apply
}

// Make creates server `me`; peers[i] reaches server i. If the persister
// holds state from before a crash, the server resumes from it. The service
// is responsible for restoring its own state from persister.ReadSnapshot().
func Make(peers []*labrpc.ClientEnd, me int, persister *tester.Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		votedFor:   -1,
		log:        []LogEntry{{Term: 0}},
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		state:      Follower,
		applyCh:    applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	// Everything in the snapshot was committed and applied before the crash.
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
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
		State:             rf.state,
		Term:              rf.currentTerm,
		CommitIndex:       rf.commitIndex,
		LastIncludedIndex: rf.lastIncludedIndex,
		Log:               append([]LogEntry(nil), rf.log[1:]...),
	}
}

// Start asks the server to append command to the replicated log. It returns
// immediately, without waiting for the entry to commit. If this server isn't
// the leader it returns false. Even if it is, the entry may never commit
// (for example if the leader loses an election first).
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
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

// Snapshot is called by the service once it has saved its state up to and
// including index. Raft discards the log up to index and keeps the snapshot
// instead, so the log doesn't grow forever.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex || index > rf.lastApplied {
		return // stale, or covers entries the service hasn't seen
	}
	rf.compactLog(index, rf.termAt(index))
	rf.snapshot = snapshot
	rf.persist()
}

// Kill stops the server's goroutines.
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
// Log indexing (all called with rf.mu held). Log indexes are absolute; the
// slice is offset by lastIncludedIndex.

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) termAt(index int) int {
	return rf.log[index-rf.lastIncludedIndex].Term
}

// compactLog drops everything up to index, keeping entries after it.
func (rf *Raft) compactLog(index, term int) {
	var rest []LogEntry
	if index < rf.lastLogIndex() {
		rest = rf.log[index-rf.lastIncludedIndex+1:]
	}
	// A new slice, so the old array (and its commands) can be freed.
	rf.log = append([]LogEntry{{Term: term}}, rest...)
	rf.lastIncludedIndex = index
}

// ---------------------------------------------------------------------------
// Background loops

// ticker runs periodically: a follower or candidate checks whether the
// election timeout has passed, and a leader checks whether a heartbeat is
// due.
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

// applier sends snapshots and committed entries to applyCh in order. It
// doesn't hold the lock while sending, because the service may be slow and
// may call back into Raft (e.g. Snapshot) before reading the next message.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		var msg ApplyMsg
		switch {
		case rf.snapshotPending:
			rf.snapshotPending = false
			msg = ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.log[0].Term,
				SnapshotIndex: rf.lastIncludedIndex,
			}
		case rf.lastApplied < rf.commitIndex:
			rf.lastApplied++
			msg = ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied,
			}
		default:
			rf.applyCond.Wait()
			continue
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
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
		LastLogTerm:  rf.termAt(rf.lastLogIndex()),
	}
	votes := 1 // our own; guarded by rf.mu
	if votes > len(rf.peers)/2 {
		rf.becomeLeader()
		return
	}

	for p := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(p int) {
			var reply RequestVoteReply
			if !rf.peers[p].Call("Raft.RequestVote", &args, &reply) {
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
			if votes > len(rf.peers)/2 {
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
	lastTerm := rf.termAt(lastIndex)
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
	for p := range rf.peers {
		if p != rf.me {
			go rf.replicateTo(p, rf.currentTerm)
		}
	}
}

// replicateTo brings peer p up to date: it sends everything from
// nextIndex[p] onward, or the snapshot if those entries were discarded.
func (rf *Raft) replicateTo(p int, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[p] <= rf.lastIncludedIndex {
		rf.sendSnapshotTo(p, term) // unlocks
		return
	}
	prev := rf.nextIndex[p] - 1
	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prev,
		PrevLogTerm:  rf.termAt(prev),
		Entries:      append([]LogEntry(nil), rf.log[prev+1-rf.lastIncludedIndex:]...),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if !rf.peers[p].Call("Raft.AppendEntries", &args, &reply) {
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
		rf.matchUpTo(p, args.PrevLogIndex+len(args.Entries))
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
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
			if rf.termAt(i) == reply.ConflictTerm {
				next = i + 1
				break
			}
			if rf.termAt(i) < reply.ConflictTerm {
				break
			}
		}
	}
	rf.nextIndex[p] = max(1, min(next, rf.lastLogIndex()+1))
	go rf.replicateTo(p, term) // retry now rather than at the next heartbeat
}

// matchUpTo records that peer p's log matches ours up to index.
func (rf *Raft) matchUpTo(p, index int) {
	// Replies can arrive out of order; never move backwards.
	rf.matchIndex[p] = max(rf.matchIndex[p], index)
	rf.nextIndex[p] = max(rf.nextIndex[p], index+1)
	rf.advanceCommitIndex()
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

	// Entries up to lastIncludedIndex are already in our snapshot (they're
	// committed, so they match). Skip them.
	if args.PrevLogIndex < rf.lastIncludedIndex {
		skip := rf.lastIncludedIndex - args.PrevLogIndex
		if skip >= len(args.Entries) {
			reply.Success = true
			return
		}
		args.Entries = args.Entries[skip:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.log[0].Term
	}

	// Consistency check: our log must contain the entry before the new ones.
	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}
	if t := rf.termAt(args.PrevLogIndex); t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > rf.lastIncludedIndex+1 && rf.termAt(i-1) == t {
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
			if rf.termAt(idx) == e.Term {
				continue
			}
			rf.log = rf.log[:idx-rf.lastIncludedIndex]
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
		if rf.termAt(idx) != rf.currentTerm {
			break
		}
		count := 0
		for _, m := range rf.matchIndex {
			if m >= idx {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx
			rf.applyCond.Broadcast()
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshots (§7)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// sendSnapshotTo sends our snapshot to a follower that is so far behind
// that we've discarded the entries it needs. Called with rf.mu held;
// returns with it released.
func (rf *Raft) sendSnapshotTo(p int, term int) {
	args := InstallSnapshotArgs{
		Term:              term,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.log[0].Term,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	var reply InstallSnapshotReply
	if !rf.peers[p].Call("Raft.InstallSnapshot", &args, &reply) {
		return
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
	rf.matchUpTo(p, args.LastIncludedIndex)
}

// InstallSnapshot is the RPC handler the leader calls to replace a lagging
// follower's state with its snapshot.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.becomeFollower(args.Term)
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		return // we already have everything it covers
	}

	if args.LastIncludedIndex <= rf.lastLogIndex() &&
		rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		// Our log continues past the snapshot and matches it: keep the rest.
		rf.compactLog(args.LastIncludedIndex, args.LastIncludedTerm)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		rf.lastIncludedIndex = args.LastIncludedIndex
	}
	rf.snapshot = args.Data
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()

	// The applier hands the snapshot to the service, which resets its state.
	rf.snapshotPending = true
	rf.applyCond.Broadcast()
}

// ---------------------------------------------------------------------------
// Persistence

// persist saves the state that must survive a crash, along with the current
// snapshot. Call it with rf.mu held after changing currentTerm, votedFor,
// or the log, before replying to anyone.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	for _, v := range []any{rf.currentTerm, rf.votedFor, rf.lastIncludedIndex, rf.log} {
		if err := e.Encode(v); err != nil {
			log.Fatalf("persist: %v", err)
		}
	}
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return // fresh server
	}
	d := gob.NewDecoder(bytes.NewBuffer(data))
	var currentTerm, votedFor, lastIncludedIndex int
	var entries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&entries) != nil {
		log.Fatalf("readPersist: corrupt state")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.log = entries
}
