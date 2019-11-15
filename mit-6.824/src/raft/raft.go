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
	"fmt"
	"labrpc"
	"log"
	"os"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

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
}

// LogEntry stores state machine commands
type LogEntry struct {
	Index   int         // position in the log
	Term    int         // term number when the entry was received by the leader
	Command interface{} // state machine command
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

	logger      *log.Logger
	lastContact time.Time // time when follower has received last AppendEntries or RequestVoting

	// ServerState, one of {Follower, Candidate, Leader}
	serverState ServerState

	// Persistent state on all servers
	// TODO: Update on stable storage before responding to RPCs
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term
	logs        []LogEntry // log entries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be commited
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
	rf.setLastContact()
}

// return last index and term in logs
func (rf *Raft) LastEntry() (int, int) {
	logLength := len(rf.logs)
	if logLength == 0 {
		return -1, -1
	} else {
		lastLogEntry := rf.logs[logLength-1]
		return lastLogEntry.Term, lastLogEntry.Index
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm { // §5.1, become follower
		rf.convertToFollower(args.Term)
		reply.Term = args.Term
		reply.Success = false
		return
	}

	if args.Term < rf.currentTerm { // §5.1, state leader
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	reply.Term = rf.currentTerm
	rf.setLastContact()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	lastLogTerm, lastLogIndex := rf.LastEntry()
	if lastLogTerm > args.LastLogTerm || lastLogIndex > args.LastLogIndex { // §5.4
		reply.VoteGranted = false
		return
	}

	rf.setLastContact()
	rf.serverState = Follower
	rf.votedFor = args.CandidateId
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

	// Set last contact to now when we start being follower.
	rf.mu.Lock()
	rf.setLastContact()
	rf.mu.Unlock()

	for rf.ServerState() == Follower {
		select {
		case <-timeoutCh:

			rf.mu.Lock()
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
		}
	}
}

func (rf *Raft) runCandidate() {
	rf.logger.Printf("Starting election")

	replyCh := make(chan *RequestVoteReply)
	receivedVotes := 1

	// Start election:
	func(replyCh chan *RequestVoteReply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if rf.serverState != Candidate {
			return
		}

		//   1) Increment currentTerm
		rf.currentTerm += 1

		//   2) Vote for self
		rf.votedFor = rf.me

		//   3) Send RequestVote for all other servers
		lastLogTerm, lastLogIndex := rf.LastEntry()

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
	}(replyCh)

	//   4) Reset election timer
	electionTimeout := randomTimeout()
	timeoutCh := time.After(electionTimeout)

	// wait for majority of votes or election timeout
	for rf.ServerState() == Candidate {
		select {
		case reply := <-replyCh:
			if reply.VoteGranted {
				receivedVotes += 1
			}
		case <-timeoutCh:
			rf.logger.Printf("Election timeout after %d ms!\n", electionTimeout/1000/1000)
			return
		default:
		}

		if receivedVotes >= rf.Majority() {
			rf.mu.Lock()
			if rf.serverState == Candidate {
				rf.logger.Println("Election won, convert to leader!")
				rf.serverState = Leader
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) runLeader() {
	replyCh := make(chan *AppendEntriesReply)

	// §5.2 send initial empty AppendEntries RPCs to each server
	go rf.sendHeartbeats(replyCh)

	// Send heartbeats every 200ms
	heartbeatTicker := time.Tick(200 * time.Millisecond)

	for rf.ServerState() == Leader {
		select {
		case <-heartbeatTicker:
			go rf.sendHeartbeats(replyCh)
		case reply := <-replyCh:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
			}
			rf.mu.Unlock()
		default:
		}
	}
}

func (rf *Raft) sendHeartbeats(replyCh chan *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.serverState != Leader {
		return
	}

	prevLogTerm, prevLogIndex := rf.LastEntry()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(peer int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peer, args, reply)
			if ok {
				replyCh <- reply
			}
		}(i)
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
	rf.logger = log.New(os.Stdout, fmt.Sprintf("INFO (Node %d): ", rf.me), log.Ltime|log.Lshortfile)
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.logs = make([]LogEntry, 0)

	rf.serverState = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.setLastContact()
	go rf.run()

	return rf
}
