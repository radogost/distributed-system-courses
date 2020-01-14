package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"labgob"
	"labrpc"
	"log"
	"os"
	"sort"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	IsSnapshot   bool
	SnapshotData []byte
}

// LogEntry stores state machine commands
type LogEntry struct {
	Index   int         // position in the log
	Term    int         // term number when the entry was received by the leader
	Command interface{} // state machine command
}

// RaftPersistentState is used to persist non-volatile raft state
type RaftPersistentState struct {
	CurrentTerm int
	VotedFor    int
	Logs        []LogEntry

	LastSnapshotIndex int
	LastSnapshotTerm  int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh     chan ApplyMsg // required for lab
	logger      *log.Logger
	lastContact time.Time // time when follower has received last AppendEntries or RequestVoting

	// ServerState, one of {Follower, Candidate, Leader}
	serverState ServerState

	// Persistent state on all servers
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on all leaders (Reinitialized after election)
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	replicatingLog []bool

	// stores the index and term of the last entry in the last snapshot
	lastSnapshotIndex int
	lastSnapshotTerm  int
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isLeader := rf.serverState == Leader
	return term, isLeader
}

func (rf *Raft) ServerState() ServerState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.serverState
}

func (rf *Raft) setLastContact() {
	rf.lastContact = time.Now()
}

func (rf *Raft) convertToFollower(term int) {
	rf.logger.Printf(
		"currentTerm: %d, receivedTerm: %d. Convert to follower",
		rf.currentTerm,
		term,
	)
	rf.votedFor = -1
	rf.serverState = Follower
	rf.currentTerm = term
}

// return last index and term in logs
func (rf *Raft) LastEntry() (int, int) {
	logLength := len(rf.logs)
	if logLength > 0 {
		lastLogEntry := rf.logs[logLength-1]
		return lastLogEntry.Index, lastLogEntry.Term
	} else {
		return rf.lastSnapshotIndex, rf.lastSnapshotTerm
	}
}

// return index of commandIndex in log
func (rf *Raft) getLogIndex(commandIndex int) int {
	low, high := 0, len(rf.logs)-1
	for low <= high {
		mid := low + (high-low)/2
		candidate := rf.logs[mid].Index
		if candidate == commandIndex {
			return mid
		} else if candidate < commandIndex {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return -1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	data := rf.getState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	persistentState := RaftPersistentState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Logs:              rf.logs,
		LastSnapshotIndex: rf.lastSnapshotIndex,
		LastSnapshotTerm:  rf.lastSnapshotTerm,
	}
	e.Encode(persistentState)
	data := w.Bytes()

	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistentState RaftPersistentState

	if d.Decode(&persistentState) != nil {
		rf.logger.Println("Could not restore persisted state!")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentTerm = persistentState.CurrentTerm
	rf.votedFor = persistentState.VotedFor
	rf.logs = persistentState.Logs

	rf.lastSnapshotIndex = persistentState.LastSnapshotIndex
	rf.lastSnapshotTerm = persistentState.LastSnapshotTerm
}

// InstallSnapshot RPC arguments structure
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // Allow followers to redirect clients
	LastIncludedIndex int    // snapshot replaces all entries up to (including) this entry
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot
}

// InstallSnapshot RPC reply structure
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	} else if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	indexInLog, logIndex := false, -1
	for i := 0; i < len(rf.logs); i++ {
		log := rf.logs[i]
		if log.Index == args.LastIncludedIndex && log.Term == args.LastIncludedTerm {
			indexInLog = true
			logIndex = i
			break
		}
	}

	if indexInLog {
		rf.logs = rf.logs[logIndex+1:]
	} else {
		rf.logs = make([]LogEntry, 0)
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.getState(), args.Data)

	rf.setLastContact()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	rf.mu.Lock()
	if rf.replicatingLog[server] {
		rf.mu.Unlock()
		return false
	}
	rf.replicatingLog[server] = true
	rf.mu.Unlock()

	// Although "Call" can timeout, the timeouts can be too big.
	// Hence, we use our own.
	respCh := make(chan bool)
	timeout := time.After(time.Duration(200) * time.Millisecond)
	go func() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		respCh <- ok
	}()

	defer func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.replicatingLog[server] = false
	}()

	select {
	case ok := <-respCh:
		return ok
	case <-timeout:
		return false
	}
}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // Allow followers to redirect clients
	PrevLogIndex int        // int index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

// AppendEntries RPC reply structure
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// Optimization to reduce number of rejected AppendEntries RPCs
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // §5.1, become follower
		rf.convertToFollower(args.Term)
		reply.Term = args.Term
		reply.Success = false
	}

	if args.Term < rf.currentTerm { // §5.1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	lastIndex, _ := rf.LastEntry()
	if args.PrevLogIndex > lastIndex { // §5.3, no entry at prevLogIndex
		reply.Term = rf.currentTerm
		reply.Success = false

		reply.ConflictIndex = lastIndex + 1
		return
	}

	if args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm != rf.lastSnapshotTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
	}

	logIndex := rf.getLogIndex(args.PrevLogIndex)
	if logIndex != -1 && rf.logs[logIndex].Term != args.PrevLogTerm { // §5.3, different terms at prevLogIndex
		reply.Term = rf.currentTerm
		reply.Success = false

		conflictTerm := rf.logs[logIndex].Term
		conflictIndex := logIndex
		for conflictIndex >= 0 && rf.logs[conflictIndex].Term == conflictTerm {
			conflictIndex--
		}
		conflictIndex += 1

		reply.ConflictTerm = conflictTerm
		reply.ConflictIndex = rf.logs[conflictIndex].Index
		return
	}

	for _, entry := range args.Entries {
		if entry.Index > rf.lastSnapshotIndex {
			// Append new entries not already in the log
			if len(rf.logs) == 0 || rf.logs[len(rf.logs)-1].Index < entry.Index {
				rf.logs = append(rf.logs, entry)
			} else {
				idx := rf.getLogIndex(entry.Index)
				// §5.3, Replace on conflict
				if rf.logs[idx].Term != entry.Term {
					rf.logs = rf.logs[:idx]
					rf.logs = append(rf.logs, entry)
				}

			}
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Index < args.LeaderCommit {
			rf.commitIndex = args.Entries[len(args.Entries)-1].Index
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	rf.persist()
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.setLastContact()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.mu.Lock()
	if rf.replicatingLog[server] {
		rf.mu.Unlock()
		return false
	}
	rf.replicatingLog[server] = true
	rf.mu.Unlock()

	// Although "Call" can timeout, the timeouts can be too big.
	// Hence, we use our own.
	respCh := make(chan bool)
	timeout := time.After(time.Duration(200) * time.Millisecond)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		respCh <- ok
	}()

	defer func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.replicatingLog[server] = false
	}()

	select {
	case ok := <-respCh:
		return ok
	case <-timeout:
		return false
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogTerm  int // Term of candidate's last log entry
	LastLogIndex int // Index of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm { // §5.1
		reply.VoteGranted = false
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId { // §5.2
		reply.VoteGranted = false
		return
	}

	lastLogIndex, lastLogTerm := rf.LastEntry()
	if lastLogTerm > args.LastLogTerm { // § 5.4.1
		reply.VoteGranted = false
		return
	} else if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		reply.VoteGranted = false
		return
	}

	rf.setLastContact()
	rf.serverState = Follower
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		isLeader = false
		return index, term, isLeader
	}

	lastIndex, _ := rf.LastEntry()
	index = lastIndex + 1
	term = rf.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}
	rf.logs = append(rf.logs, entry)
	rf.matchIndex[rf.me] = index

	go rf.replicateLogEntries()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.logger.SetOutput(ioutil.Discard)
}

func (rf *Raft) run() {
	for {
		switch rf.ServerState() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		}
	}
}

func (rf *Raft) runFollower() {
	electionTimeout := randomTimeout()
	timeoutCh := time.After(electionTimeout)

	for rf.ServerState() == Follower {
		select {
		case <-timeoutCh:
			rf.mu.Lock()
			if rf.serverState != Follower {
				rf.mu.Unlock()
				return
			}

			lastContact := rf.lastContact
			contactGap := time.Now().Sub(lastContact)
			if contactGap < electionTimeout {
				// We had successful contact, restart timer and continue
				timeoutCh = time.After(electionTimeout)
			} else {
				rf.logger.Printf("No contact for %d ms!\n", contactGap/1000/1000)
				rf.serverState = Candidate
			}
			rf.mu.Unlock()

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) runCandidate() {
	rf.logger.Printf("Starting election")

	rf.mu.Lock()
	replyCh := make(chan *RequestVoteReply, len(rf.peers))
	receivedVotes := 1

	if rf.serverState != Candidate {
		rf.mu.Unlock()
		return
	}

	// 1) Increment currentTerm
	rf.currentTerm += 1

	// 2) Vote for self
	rf.votedFor = rf.me

	// 3) Send RequestVote for all other servers
	lastLogIndex, lastLogTerm := rf.LastEntry()

	requestArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peer, requestArgs, reply)
			if ok {
				replyCh <- reply
			}
		}(i)
	}
	rf.mu.Unlock()

	// 4) Reset election timer
	electionTimeout := randomTimeout()
	timeoutCh := time.After(electionTimeout)

	// wait for majority of votes or election timeout
	for rf.ServerState() == Candidate {
		select {
		case reply := <-replyCh:
			rf.mu.Lock()

			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				rf.mu.Unlock()
				return
			}

			if rf.serverState != Candidate {
				rf.mu.Unlock()
				return
			}

			if reply.Term < rf.currentTerm { // stale response, ignore
				rf.mu.Unlock()
				continue
			}

			if reply.VoteGranted {
				receivedVotes += 1
			}

			if receivedVotes >= rf.Majority() {
				rf.logger.Println("Election won, convert to leader!")
				rf.serverState = Leader
			}
			rf.mu.Unlock()

		case <-timeoutCh:
			rf.logger.Printf("Election timeout after %d ms!\n", electionTimeout/1000/1000)
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}

	}
}

func (rf *Raft) runLeader() {
	rf.mu.Lock()
	// Altough len(rf.peers)-1 would be enough, keep it the same length as rf.peers
	// so that same indices refer to same servers
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.replicatingLog = make([]bool, len(rf.peers))

	lastIndex, _ := rf.LastEntry()
	nextIndex := lastIndex + 1
	for i := range rf.peers {
		rf.nextIndex[i] = nextIndex
		rf.matchIndex[i] = 0
		rf.replicatingLog[i] = false
	}
	rf.mu.Unlock()

	// §5.2 send initial empty AppendEntries RPCs to each server
	go rf.replicateLogEntries()

	// Send heartbeats every 200ms
	heartbeatTicker := time.Tick(200 * time.Millisecond)

	for rf.ServerState() == Leader {
		select {
		case <-heartbeatTicker:
			go rf.replicateLogEntries()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) replicateLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIdx := rf.nextIndex[i]
		if nextIdx <= rf.lastSnapshotIndex {
			go rf.replicateSnapshotToPeer(i)
		} else {
			prevLogIndex := rf.getLogIndex(nextIdx - 1)
			var prevLogTerm int
			if prevLogIndex != -1 {
				prevLogTerm = rf.logs[prevLogIndex].Term
			} else {
				prevLogIndex, prevLogTerm = rf.LastEntry()
			}
			go rf.replicateLogEntriesToPeer(i, prevLogIndex, prevLogTerm)
		}
	}
}

func (rf *Raft) replicateLogEntriesToPeer(peer int, prevLogIndex int, prevLogTerm int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return
	}

	logIndex := rf.getLogIndex(prevLogIndex)
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.logs[logIndex+1 : len(rf.logs)],
		LeaderCommit: rf.commitIndex,
	}

	go func() {
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.serverState != Leader { // We aren't leader anymore, nothing to do
			return
		}

		if !ok {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		}

		if reply.Success {
			var nextIndex int
			var matchIndex int
			if len(args.Entries) == 0 {
				nextIndex = args.PrevLogIndex + 1
				matchIndex = args.PrevLogIndex
			} else {
				nextIndex = args.Entries[len(args.Entries)-1].Index + 1
				matchIndex = args.Entries[len(args.Entries)-1].Index
			}
			rf.nextIndex[peer] = nextIndex
			rf.matchIndex[peer] = matchIndex

			// §5.3, §5.4: Recompute commitIndex
			// Store matchIndexes in a new slice, sort it.
			// After sorting, we know that the middle element is replicated on majority
			matchIndexes := make([]int, len(rf.matchIndex))
			copy(matchIndexes, rf.matchIndex)
			sort.Ints(matchIndexes)
			majorityIndex := matchIndexes[(len(matchIndexes)-1)/2]

			idx := rf.getLogIndex(majorityIndex)
			if majorityIndex > rf.commitIndex && rf.logs[idx].Term == rf.currentTerm {
				rf.commitIndex = majorityIndex
			}

			lastLogIndex, _ := rf.LastEntry()
			nextIndexForPeer := rf.nextIndex[peer]
			if lastLogIndex >= nextIndexForPeer { // not all logs replicated, send more
				if nextIndexForPeer <= rf.lastSnapshotIndex {
					go rf.replicateSnapshotToPeer(peer)
				} else {
					idx := rf.getLogIndex(nextIndexForPeer)
					nextLogTermForPeer := rf.logs[idx].Term
					go rf.replicateLogEntriesToPeer(
						peer,
						nextIndexForPeer,
						nextLogTermForPeer,
					)
				}
			}

		} else { // Follower does not contain entry matching prevLogIndex and prevLogTerm
			var nextIndex int
			if reply.ConflictTerm > 0 {
				nextIndex = 0
				for nextIndex < len(rf.logs) && rf.logs[nextIndex].Term < reply.ConflictTerm {
					nextIndex += 1
				}

				if nextIndex == len(rf.logs) || rf.logs[nextIndex].Term != reply.ConflictTerm {
					nextIndex = reply.ConflictIndex
				}
			} else if reply.ConflictIndex > 0 {
				nextIndex = reply.ConflictIndex
			} else {
				// §5.3 LogInconsistency: decrement nextIndex and retry
				nextIndex = prevLogIndex
			}

			if nextIndex <= rf.lastSnapshotIndex {
				go rf.replicateSnapshotToPeer(peer)
			} else {
				rf.nextIndex[peer] = nextIndex
				prevLogIndexToSend := nextIndex - 1
				idx := rf.getLogIndex(prevLogIndexToSend)

				var prevLogTermToSend int
				if idx == -1 {
					prevLogIndexToSend = rf.lastSnapshotIndex
					prevLogTermToSend = rf.lastSnapshotTerm
				} else {
					prevLogTermToSend = rf.logs[idx].Term
				}

				go rf.replicateLogEntriesToPeer(
					peer,
					prevLogIndexToSend,
					prevLogTermToSend,
				)
			}
		}
	}()
}

func (rf *Raft) replicateSnapshotToPeer(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return
	}

	snapshotData := rf.persister.ReadSnapshot()

	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.lastSnapshotTerm,
		Data:              snapshotData,
	}

	go func() {
		reply := &InstallSnapshotReply{}
		ok := rf.sendInstallSnapshot(peer, args, reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if !ok || rf.serverState != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			return
		} else {
			go rf.replicateLogEntriesToPeer(
				peer,
				args.LastIncludedIndex,
				args.LastIncludedTerm,
			)
		}
	}()
}

func (rf *Raft) TruncateLog(commandIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if commandIndex < rf.commitIndex { // don't snapshot uncommited data
		return
	}

	logIndex := rf.getLogIndex(commandIndex)

	if logIndex != -1 {
		entry := rf.logs[logIndex]
		rf.lastSnapshotIndex = entry.Index
		rf.lastSnapshotTerm = entry.Term
		rf.logs = rf.logs[logIndex+1:]

		rf.persister.SaveStateAndSnapshot(rf.getState(), snapshot)
	}
}

func (rf *Raft) applyLogs() {
	for {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied {

			if rf.lastSnapshotIndex > rf.lastApplied {
				rf.mu.Unlock()
				rf.applyCh <- ApplyMsg{
					CommandValid: false,
					IsSnapshot:   true,
					SnapshotData: rf.persister.ReadSnapshot(),
				}
				rf.mu.Lock()
				rf.lastApplied = rf.lastSnapshotIndex
			} else {
				logs := make([]LogEntry, len(rf.logs))
				copy(logs, rf.logs)

				for _, entry := range logs {
					if entry.Index > rf.lastApplied && entry.Index <= rf.commitIndex {
						rf.mu.Unlock()
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      entry.Command,
							CommandIndex: entry.Index,
						}
						rf.mu.Lock()
						rf.lastApplied = entry.Index
					}
				}
			}

			rf.mu.Unlock()

		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.logger = log.New(os.Stdout, fmt.Sprintf("INFO (Node %d): ", rf.me), log.Ltime|log.Lshortfile)
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = make([]LogEntry, 1)

	rf.setLastContact()
	rf.serverState = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.applyLogs()
	go rf.run()

	return rf
}
