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
	//	"bytes"

	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type ServerState int

const (
	FOLLOWER ServerState = iota
	CANDIDATE
	LEADER
)

const (
	ELECTION_TIMEOUT_MIN = 241
	ELECTION_TIMEOUT_MAX = 300
	HEARTBEAT_TIMEOUT    = 120
)

func RandomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration((rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN))
}

func StableHeartbeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(HEARTBEAT_TIMEOUT)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state ServerState

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	term, isLeader = rf.currentTerm, rf.state == LEADER
	rf.mu.Unlock()
	return
}

// Check if state equals to rf.state
func (rf *Raft) CheckState(state ServerState) (isStateEqual bool) {
	rf.mu.Lock()
	isStateEqual = rf.state == state
	rf.mu.Unlock()
	return
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		log.Printf("Server %d declines to vote to server %d, because of term", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		log.Printf("Server %d votes to server %d", rf.me, args.CandidateId)
		rf.mu.Lock()
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.state = FOLLOWER
		rf.mu.Unlock()

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !rf.CheckState(LEADER) {
			log.Printf("Server %d votes to server %d", rf.me, args.CandidateId)
			rf.mu.Lock()
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		}
	}

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

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		log.Printf("Server %d refuses the requeset to append entries from server %d, because of term ", rf.me, args.LeaderId)

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.mu.Lock()
	switch rf.state {
	case LEADER:
		// leader
		log.Printf("Server %d loses leadership, because of rejoining the cluster and receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.nextIndex = nil
		rf.matchIndex = nil

		rf.state = FOLLOWER
		rf.electionTimer.Reset(RandomElectionTimeout())
	case CANDIDATE:
		// candidate
		log.Printf("Server %d quits election, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.electionTimer.Reset(RandomElectionTimeout())
	default:
		// follower
		/*
			if args.PrevLogIndex == 0 || rf.log[args.PrevLogIndex].Index != args.PrevLogIndex {
				reply.Success = false
				return
			} else if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
				rf.log = rf.log[:args.PrevLogIndex]
			}
		*/
		if rf.votedFor != -1 {
			log.Printf("Server %d resets votedFor, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
			rf.votedFor = -1
		}
		rf.electionTimer.Reset(RandomElectionTimeout())
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	if rf.CheckState(FOLLOWER) {
		log.Printf("Server %d first enters election", rf.me)
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
	} else {
		log.Printf("Server %d enters election again", rf.me)
	}
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	grantedVoteNum := 1
	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}

		go func(serverIdx int) {
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}

			args.Term = rf.currentTerm
			args.CandidateId = rf.me

			if rf.sendRequestVote(serverIdx, &args, &reply) {
				if reply.Term == rf.currentTerm && reply.VoteGranted {
					grantedVoteNum += 1
					if grantedVoteNum >= len(rf.peers)/2+1 && !rf.CheckState(LEADER) {
						// candidate -> leader
						log.Printf("Server %d wins election", rf.me)
						rf.mu.Lock()
						rf.state = LEADER
						rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
						rf.mu.Unlock()
					}
				}
				if reply.Term > rf.currentTerm {
					log.Printf("Server %d quits election, because of receiving a lager term from server %d when requesting votes", rf.me, serverIdx)
					// candidate -> follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					rf.mu.Unlock()
				}
			}
		}(i)
	}

}

func (rf *Raft) BroadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}

		go func(serverIdx int) {
			args := AppendEntriesArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				PrevLogIndex:      rf.lastApplied,
				PrevLogTerm:       rf.log[rf.lastApplied].Term,
				Entries:           rf.log[rf.lastApplied:],
				LeaderCommitIndex: rf.commitIndex,
			}
			reply := AppendEntriesReply{}

			if rf.sendAppendEntries(serverIdx, &args, &reply) {
				if reply.Term > rf.currentTerm {
					log.Printf("Server %d loses leadership, because of receiving a lager term from server %d when appending entries", rf.me, serverIdx)

					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1

					rf.nextIndex = nil
					rf.matchIndex = nil

					rf.electionTimer.Reset(RandomElectionTimeout())
					rf.state = FOLLOWER
					rf.mu.Unlock()
				}
			}
		}(i)

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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (2B).
	if !rf.CheckState(LEADER) {
		return -1, -1, false
	}

	rf.mu.Lock()
	term = rf.currentTerm
	index = len(rf.log)
	isLeader = true

	nLogEntry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, nLogEntry)
	// rf.lastApplied = index

	rf.mu.Unlock()
	return
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			// follower -> candidate
			if !rf.CheckState(LEADER) {
				rf.StartElection()
				rf.electionTimer.Reset(RandomElectionTimeout())
			}
		case <-rf.heartbeatTimer.C:
			// leader
			if rf.CheckState(LEADER) {
				rf.BroadcastHeartbeat()
				rf.mu.Lock()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
				rf.mu.Unlock()
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
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,

		// Your initialization code here (2A, 2B, 2C).
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 1), // guarantee valid log entries' index begin with 1

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  nil,
		matchIndex: nil,

		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),

		state: FOLLOWER,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
