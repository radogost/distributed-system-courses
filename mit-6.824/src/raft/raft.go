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
	"labgob"
	"labrpc"
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
	lastContact time.Time     // time when follower has received last AppendEntries or RequestVoting

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

	heartbeatCh   chan bool
	electionEndCh chan bool

	receivedVotes int

	// stores the index and term of the last entry in the last snapshot
	lastSnapshotIndex int
	lastSnapshotTerm  int
}

func (rf *Raft) majority() int {
	return len(rf.peers)/2 + 1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
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

func (rf *Raft) convertToFollower(term int) {
	rf.votedFor = -1
	rf.serverState = Follower
	rf.currentTerm = term
	rf.persist()
}

// return last index and term in logs
func (rf *Raft) lastEntry() (int, int) {
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

	reply.Term = max(rf.currentTerm, args.Term)
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
	rf.lastApplied = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.getState(), args.Data)
	rf.applyCh <- ApplyMsg{
		CommandValid: false,
		IsSnapshot:   true,
		SnapshotData: args.Data,
	}

	rf.heartbeatCh <- true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.serverState != Leader || reply.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.serverState = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
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
	ConflictIndex int
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = max(args.Term, rf.currentTerm)
	if args.Term > rf.currentTerm { // §5.1, become follower
		rf.convertToFollower(args.Term)
	}

	if args.Term < rf.currentTerm { // §5.1
		reply.Success = false
		return
	}

	lastIndex, _ := rf.lastEntry()
	if args.PrevLogIndex > lastIndex { // §5.3, no entry at prevLogIndex
		reply.Success = false
		reply.ConflictIndex = lastIndex + 1
		return
	}

	logIndex := rf.getLogIndex(args.PrevLogIndex)
	if logIndex != -1 && rf.logs[logIndex].Term != args.PrevLogTerm { // §5.3, different terms at prevLogIndex
		conflictTerm := rf.logs[logIndex].Term
		conflictIndex := logIndex
		for conflictIndex >= 0 && rf.logs[conflictIndex].Term == conflictTerm {
			conflictIndex--
		}
		conflictIndex += 1

		reply.ConflictIndex = rf.logs[conflictIndex].Index
		reply.Success = false
		return
	}

	if args.PrevLogIndex == rf.lastSnapshotIndex && args.PrevLogTerm != rf.lastSnapshotTerm {
		reply.Success = false
		reply.ConflictIndex = args.PrevLogIndex
		return
	}

	rf.heartbeatCh <- true
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
		go rf.applyLogs()
	}

	rf.persist()
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.serverState != Leader || args.Term < rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.serverState = Follower
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.Success {
		var nextIndex, matchIndex int
		if len(args.Entries) == 0 {
			nextIndex = args.PrevLogIndex + 1
			matchIndex = args.PrevLogIndex
		} else {
			nextIndex = args.Entries[len(args.Entries)-1].Index + 1
			matchIndex = args.Entries[len(args.Entries)-1].Index
		}
		rf.nextIndex[server] = nextIndex
		rf.matchIndex[server] = matchIndex

		// §5.3, §5.4: Recompute commitIndex
		// Store matchIndexes in a new slice, sort it.
		// After sorting, we know that the middle element is replicated on majority
		matchIndexes := make([]int, len(rf.matchIndex))
		copy(matchIndexes, rf.matchIndex)
		sort.Ints(matchIndexes)
		majorityIndex := matchIndexes[(len(matchIndexes)-1)/2+1]

		idx := rf.getLogIndex(majorityIndex)
		var majorityTerm int
		if idx > 0 {
			majorityTerm = rf.logs[idx].Term
		} else {
			majorityTerm = rf.lastSnapshotTerm
		}
		if majorityIndex > rf.commitIndex && majorityTerm == rf.currentTerm {
			rf.commitIndex = majorityIndex
			go rf.applyLogs()
		}
	} else {
		rf.nextIndex[server] = reply.ConflictIndex
	}
}

type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // Candidate requesting vote
	LastLogTerm  int // Term of candidate's last log entry
	LastLogIndex int // Index of candidate's last log entry
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = max(rf.currentTerm, args.Term)
	if args.Term < rf.currentTerm { // §5.1
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId { // §5.2
		reply.VoteGranted = false
		return
	}

	lastLogIndex, lastLogTerm := rf.lastEntry()
	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) { // § 5.4.1
		reply.VoteGranted = false
		return
	}

	rf.heartbeatCh <- true
	rf.serverState = Follower
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Could not send, not candidate anymore or stale response from previous election
	if !ok || rf.serverState != Candidate || rf.currentTerm > reply.Term {
		return
	}

	if rf.currentTerm < reply.Term {
		rf.serverState = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}

	if reply.VoteGranted {
		rf.receivedVotes += 1
	}

	if rf.receivedVotes >= rf.majority() {
		rf.serverState = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))

		lastIndex, _ := rf.lastEntry()
		nextIndex := lastIndex + 1
		for peer := range rf.peers {
			rf.nextIndex[peer] = nextIndex
			rf.matchIndex[peer] = 0
		}
		rf.serverState = Leader
		rf.electionEndCh <- true
	}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term := -1, -1
	if rf.serverState != Leader {
		return index, term, false
	}

	lastIndex, _ := rf.lastEntry()
	index = lastIndex + 1
	term = rf.currentTerm
	entry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.logs = append(rf.logs, entry)
	rf.persist()

	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) run() {
	for {
		switch rf.ServerState() {
		case Follower:
			select {
			case <-rf.heartbeatCh:
			case <-time.After(randomTimeout(200, 500)):
				rf.mu.Lock()
				if rf.serverState == Follower {
					rf.serverState = Candidate
				}
				rf.mu.Unlock()
			}
		case Candidate:
			rf.startElection()

			select {
			case <-rf.heartbeatCh:
				rf.mu.Lock()
				if rf.serverState == Candidate {
					rf.serverState = Follower
				}
				rf.mu.Unlock()
			case <-rf.electionEndCh:
			case <-time.After(randomTimeout(200, 500)):
			}
		case Leader:
			go rf.replicateLogEntries()
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Candidate {
		return
	}

	rf.receivedVotes = 1
	rf.currentTerm += 1
	rf.votedFor = rf.me

	lastLogIndex, lastLogTerm := rf.lastEntry()
	requestArgs := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  lastLogTerm,
		LastLogIndex: lastLogIndex,
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.sendRequestVote(peer, requestArgs, &RequestVoteReply{})
	}
}

func (rf *Raft) replicateLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return
	}

	snapshotData := rf.persister.ReadSnapshot()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		nextIdx := rf.nextIndex[peer]
		if nextIdx > rf.lastSnapshotIndex {
			logIndex := rf.getLogIndex(nextIdx - 1)

			var prevLogIndex, prevLogTerm int
			if logIndex != -1 {
				entry := rf.logs[logIndex]
				prevLogIndex, prevLogTerm = entry.Index, entry.Term
			} else {
				prevLogIndex, prevLogTerm = rf.lastSnapshotIndex, rf.lastSnapshotTerm
			}

			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      rf.logs[logIndex+1 : len(rf.logs)],
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(peer, args, &AppendEntriesReply{})
		} else {
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastSnapshotIndex,
				LastIncludedTerm:  rf.lastSnapshotTerm,
				Data:              snapshotData,
			}
			go rf.sendInstallSnapshot(peer, args, &InstallSnapshotReply{})
		}
	}
}

func (rf *Raft) TruncateLog(commandIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if commandIndex > rf.commitIndex { // don't snapshot uncommited data
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for _, entry := range rf.logs {
			if entry.Index > rf.lastApplied && entry.Index <= rf.commitIndex {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
				rf.lastApplied = entry.Index
			}
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

	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = make([]LogEntry, 1)

	rf.serverState = Follower

	rf.applyCh = applyCh
	rf.heartbeatCh = make(chan bool, 100)
	rf.electionEndCh = make(chan bool, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}
