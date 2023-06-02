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

	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

type ServerState string

const (
	FOLLOWER  ServerState = "follower"
	CANDIDATE ServerState = "candiadate"
	LEADER    ServerState = "leader"
)

const (
	ELECTION_TIMEOUT_MIN = 70
	ELECTION_TIMEOUT_MAX = 100
	HEARTBEAT_TIMEOUT    = 30
)

func randomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration((rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN))
}

func stableHeartbeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(HEARTBEAT_TIMEOUT)
}

func minInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
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
	CommandTerm  int

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

	applyCond *sync.Cond
	applyCh   chan ApplyMsg
}

func (rf *Raft) Me() int {
	return rf.me
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
func (rf *Raft) checkState(state ServerState) (isStateEqual bool) {
	rf.mu.Lock()
	isStateEqual = rf.state == state
	rf.mu.Unlock()
	return
}

func (rf *Raft) encodeState() []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(rf.currentTerm) != nil || encoder.Encode(rf.votedFor) != nil || encoder.Encode(rf.log) != nil {
		log.Fatalf("State Persistence: Server %d fails to store its state", rf.me)
	}
	return buffer.Bytes()
}

// return the index of first log entry
func (rf *Raft) getFirstLogIndex() int {
	return rf.log[0].Index
}

// return the index of last log entry
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// return the log term at index
// valid index begins at 1
func (rf *Raft) getLogTermAt(logIndex int) int {
	firstLogIndex := rf.getFirstLogIndex()
	lastLogIndex := rf.getLastLogIndex()
	if logIndex < firstLogIndex || logIndex > lastLogIndex || rf.log[logIndex-firstLogIndex].Index != logIndex {
		DPrintf("Server %d fails to get term at %d with firstlogindex = %d and lastlogindex = %d)", rf.me, logIndex, firstLogIndex, lastLogIndex)
	}
	return rf.log[logIndex-firstLogIndex].Term
}

// return the log entries [index, )
func (rf *Raft) getSubLogFrom(index int) []LogEntry {
	firstLogIndex := rf.getFirstLogIndex()
	return append([]LogEntry{}, rf.log[index-firstLogIndex:]...)
}

// return log entries [1, index)
func (rf *Raft) getSubLogTo(index int) []LogEntry {
	firstLogIndex := rf.getFirstLogIndex()
	return append([]LogEntry{}, rf.log[:index-firstLogIndex]...)
}

// return the log entries [index1, index2)
func (rf *Raft) getSubLogFrom1To2(index1 int, index2 int) []LogEntry {
	firstLogIndex := rf.getFirstLogIndex()
	return append([]LogEntry{}, rf.log[index1-firstLogIndex:index2-firstLogIndex]...)
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

	rf.persister.SaveRaftState(rf.encodeState())
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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var currentTerm, votedFor int
	var logEntries []LogEntry
	if decoder.Decode(&currentTerm) != nil || decoder.Decode(&votedFor) != nil || decoder.Decode(&logEntries) != nil {
		log.Fatalf("State Persistence: Server %d fails to read state", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
		rf.commitIndex = rf.getFirstLogIndex()
		rf.lastApplied = rf.getFirstLogIndex()
		DPrintf("State Persistence: Server %d reads state successfully", rf.me)
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("{Node %v} service calls CondInstallSnapshot with lastIncludedTerm %v and lastIncludedIndex %v to check whether snapshot is still valid in term %v", rf.me, lastIncludedTerm, lastIncludedIndex, rf.currentTerm)

	// outdated snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects the outdated snapshot because lastIncludedIndex = %v,commitIndex = %v, firstLogIndex = %v", rf.me, lastIncludedIndex, rf.commitIndex, rf.getFirstLogIndex())
		return false
	}

	if lastIncludedIndex > rf.getLastLogIndex() {
		rf.log = make([]LogEntry, 1)
	} else {
		rf.log = rf.getSubLogFrom(lastIncludedIndex)
		rf.log[0].Command = nil
	}
	// update dummy entry with lastIncludedTerm and lastIncludedIndex
	rf.log[0].Term, rf.log[0].Index = lastIncludedTerm, lastIncludedIndex
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)

	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLogIndex(), rf.getLastLogIndex(), lastIncludedTerm, lastIncludedIndex)
	// Previously, this lab recommended that you implement a function called CondInstallSnapshot to avoid the requirement that snapshots and log entries sent on applyCh are coordinated. This vestigal API interface remains, but you are discouraged from implementing it: instead, we suggest that you simply have it return true.
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	// rf.log[0] is the place where snapshot info is kept
	snapshotIndex := rf.getFirstLogIndex()
	if index <= snapshotIndex {
		DPrintf("Log Compaction: Server %d has already compacted log entries before %d", rf.me, index)
		return
	}
	DPrintf("Log Compaction: Server %d compacted log entries (%d , %d] successfully", rf.me, rf.getFirstLogIndex(), index)
	rf.log = append([]LogEntry{}, rf.log[index-snapshotIndex:]...)
	rf.log[0].Command = nil
	rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
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
	// check if vote multiple times
	IsDuplicate bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1) {
		if args.Term < rf.currentTerm {
			DPrintf("Leader Election: Server %d declines to vote to server %d, because of term", rf.me, args.CandidateId)
			reply.VoteGranted, reply.IsDuplicate = false, false
		} else {
			if args.CandidateId != rf.votedFor {
				DPrintf("Leader Election: Server %d declines to vote to server %d, because it has already voted for server %d", rf.me, args.CandidateId, rf.votedFor)
				reply.VoteGranted, reply.IsDuplicate = false, false
			} else {
				DPrintf("Leader Election: Server %d has already voted to server %d in this term %d", rf.me, rf.votedFor, rf.currentTerm)
				reply.VoteGranted, reply.IsDuplicate = true, true
			}
		}
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.mu.Unlock()
		if !rf.checkState(FOLLOWER) {
			if rf.checkState(LEADER) {
				//rf.matchIndex = nil
				//rf.nextIndex = nil
				rf.electionTimer.Reset(randomElectionTimeout())
			}
			rf.state = FOLLOWER
		}
	}
	// check on whether candidate's log entries are up-to-date without commitIndex because of persistence does not contain commitIndex
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.getLogTermAt(lastLogIndex)
	if args.LastLogTerm < lastLogTerm || args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex {
		DPrintf("Leader Election: Server %d declines to vote to server %d, request{LastLogIndex %d, LastLogTerm %d} rf{LastLogIndex %d, LastLogTerm %d}", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, lastLogIndex, lastLogTerm)
		reply.Term, reply.VoteGranted, reply.IsDuplicate = rf.currentTerm, false, false
		return
	}

	DPrintf("Leader Election: Server %d votes to server %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.mu.Unlock()
	rf.persist()
	reply.Term, reply.VoteGranted, reply.IsDuplicate = rf.currentTerm, true, false
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
	// follower current term
	Term int
	// check if match
	Success bool
	// help leader find match index(unvalid if Success == true)
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term != rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, false, -1, -1
		return
	}

	switch rf.state {
	case LEADER:
		// leader
		rf.mu.Lock()
		DPrintf("Leader Election: Server %d loses leadership, because of rejoining the cluster and receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		//rf.nextIndex = nil
		//rf.matchIndex = nil
		rf.state = FOLLOWER
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.mu.Unlock()
	case CANDIDATE:
		// candidate
		rf.mu.Lock()
		DPrintf("Leader Election: Server %d quits election, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.mu.Unlock()
	default:
		// follower
		rf.mu.Lock()
		if rf.votedFor != -1 {
			DPrintf("Leader Election: Server %d resets votedFor, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
			rf.votedFor = -1
		}
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.mu.Unlock()
	}

	// check if follower's log entries match with the leader
	lastLogIndex := rf.getLastLogIndex()
	// firstLogIndex := rf.getFirstLogIndex()
	if args.PrevLogIndex > lastLogIndex || rf.getLogTermAt(args.PrevLogIndex) != args.PrevLogTerm {
		if args.PrevLogIndex > lastLogIndex {
			reply.ConflictIndex, reply.ConflictTerm = lastLogIndex, rf.getLogTermAt(lastLogIndex)
		} else {
			reply.ConflictTerm = rf.getLogTermAt(args.PrevLogIndex)
			reply.ConflictIndex = args.PrevLogIndex - 1
			for reply.ConflictIndex >= 1 && reply.ConflictTerm == rf.getLogTermAt(args.PrevLogIndex) {
				reply.ConflictIndex -= 1
			}
			reply.ConflictTerm = rf.getLogTermAt(args.PrevLogIndex)
		}
		reply.Term, reply.Success = rf.currentTerm, false
		DPrintf("Log Replication: Server %d declines to append log entries, request{PrevLogIndex %d, PrevLogTerm %d} rf{LastLogIndex %d, LastLogTerm %d}, reply{ConflictIndex %d, ConflictTerm %d}", rf.me, args.PrevLogIndex, args.PrevLogTerm, lastLogIndex, rf.getLogTermAt(lastLogIndex), reply.ConflictIndex, reply.ConflictTerm)
		return
	}

	for offset := 1; offset <= len(args.Entries); offset += 1 {
		if offset+args.PrevLogIndex > rf.getLastLogIndex() || rf.getLogTermAt(args.PrevLogIndex+offset) != args.Entries[offset-1].Term {
			if offset+args.PrevLogIndex <= rf.getLastLogIndex() {
				DPrintf("Log Replication: Server %d truncates log entries after %d successfully", rf.me, args.PrevLogIndex+offset)
				rf.log = rf.getSubLogTo(args.PrevLogIndex + offset)
			}
			args.Entries = args.Entries[offset-1:]
			rf.log = append(rf.log, args.Entries...)
			DPrintf("Log Replication: Server %d appends log entries [%d , %d) successfully, logs = %v", rf.me, args.Entries[0].Index, args.Entries[0].Index+len(args.Entries), rf.log)
			break
		}
	}

	reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, true, -1, -1

	if args.LeaderCommitIndex > rf.commitIndex && rf.getLastLogIndex() > rf.commitIndex {
		rf.mu.Lock()
		DPrintf("Log Replication: Server %d commits (%d , %d] log entries successfully", rf.me, rf.commitIndex, minInt(args.LeaderCommitIndex, rf.getLastLogIndex()))
		rf.commitIndex = minInt(args.LeaderCommitIndex, rf.getLastLogIndex())
		rf.mu.Unlock()
		// wake up applier
		rf.applyCond.Broadcast()
	}

	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.state = FOLLOWER
	rf.electionTimer.Reset(randomElectionTimeout())

	// outdated snapshots
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// asynchronously send info to clients(service layer)
	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotTerm:  args.LastIncludedTerm,
		}
	}()

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	if rf.checkState(FOLLOWER) {
		DPrintf("Leader Election: Server %d first enters election", rf.me)
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
	} else {
		DPrintf("Leader Election: Server %d enters election again", rf.me)
	}
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.mu.Unlock()
	rf.persist()

	grantedVoteNum := 1
	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.getLastLogIndex(),
				LastLogTerm:  rf.getLogTermAt(rf.getLastLogIndex()),
			}
			reply := RequestVoteReply{}

			if rf.sendRequestVote(server, &args, &reply) {
				// check if follower grants only valid vote for each term
				if reply.Term == rf.currentTerm && reply.VoteGranted && !reply.IsDuplicate {
					grantedVoteNum += 1
					if grantedVoteNum >= len(rf.peers)/2+1 && !rf.checkState(LEADER) {
						// candidate -> leader
						DPrintf("Leader Election: Server %d wins election", rf.me)
						rf.mu.Lock()
						rf.votedFor = -1
						rf.matchIndex = make([]int, len(rf.peers))
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i += 1 {
							rf.nextIndex[i] = rf.getLastLogIndex() + 1
						}
						rf.state = LEADER
						rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
						rf.mu.Unlock()
						rf.persist()
						// must send out heartbeats immediately after winning an election
						rf.BroadcastHeartbeat()
					}
				}
				if reply.Term > rf.currentTerm && !reply.VoteGranted {
					DPrintf("Leader Election: Server %d quits election, because of receiving a larger term of %d from server %d when requesting votes", rf.me, reply.Term, server)
					// candidate -> follower
					rf.mu.Lock()
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					rf.mu.Unlock()
					rf.persist()
				}
			}
		}(i)
	}

}

// update leader's commitIndex then wakes up the applier go routinue if needed
func (rf *Raft) UpdateCommitIndexAndWakeApplier() {
	// leader's possible commitIndex
	newCommitIndex := rf.commitIndex
	for {
		// the number of followers who append the log entries successfully
		appendedLogServerNum := 0
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me || rf.matchIndex[i] >= newCommitIndex {
				appendedLogServerNum += 1
			}
			if appendedLogServerNum >= len(rf.peers)/2+1 {
				newCommitIndex += 1
				break
			}
		}
		if appendedLogServerNum < len(rf.peers)/2+1 {
			break
		}
	}
	newCommitIndex -= 1
	if newCommitIndex < rf.getFirstLogIndex() {
		return
	}
	// only when log entries in current term have been committed, the commitIndex can be updated(figure 8 in enxtended raft paper)
	if rf.getLogTermAt(newCommitIndex) == rf.currentTerm && newCommitIndex > rf.commitIndex {
		rf.mu.Lock()
		DPrintf("Log Replication: Server %d commits (%d , %d] log entries successfully *LEADER*", rf.me, rf.commitIndex, newCommitIndex)
		rf.commitIndex = newCommitIndex
		rf.mu.Unlock()
		// wake up applier
		rf.applyCond.Broadcast()
	}
}

// Broacast the heartbeats
func (rf *Raft) BroadcastHeartbeat() {
	for i := 0; i < len(rf.peers); i += 1 {
		if i == rf.me {
			continue
		}

		go func(server int) {
			// the index of which log entries are needed to replicate starts at
			var prevLogIndex int
			if rf.matchIndex[server] == 0 && rf.nextIndex[server] > rf.matchIndex[server]+1 {
				prevLogIndex = rf.nextIndex[server] - 1
			} else {
				prevLogIndex = rf.matchIndex[server]
			}

			if prevLogIndex < rf.getFirstLogIndex() {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.getFirstLogIndex(),
					LastIncludedTerm:  rf.getLogTermAt(rf.getFirstLogIndex()),
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSnapshotReply{}
				if rf.sendInstallSnapshot(server, &args, &reply) {
					if reply.Term > rf.currentTerm && rf.checkState(LEADER) {
						DPrintf("Log Compaction: Server %d loses leadership, because of receiving a larger term from server %d when sending snapshot", rf.me, server)
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.electionTimer.Reset(randomElectionTimeout())
						rf.state = FOLLOWER
						rf.mu.Unlock()
						rf.persist()
					} else {
						if !rf.checkState(LEADER) {
							// one go routinue may get a larger term when appending entries and change rf.state, other go routinues need to check that and quit if no longer a leader
							return
						}
						rf.matchIndex[server] = args.LastIncludedIndex
						rf.nextIndex[server] = args.LastIncludedIndex + 1
					}
				}
			} else {
				args := AppendEntriesArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      prevLogIndex,
					PrevLogTerm:       rf.getLogTermAt(prevLogIndex),
					Entries:           rf.getSubLogFrom(rf.nextIndex[server]),
					LeaderCommitIndex: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				// reply.Success == true means follower's log entries match the leader's therefore jump out of the loop whose function is to find matchIndex
				for rf.sendAppendEntries(server, &args, &reply) {
					if reply.Term > rf.currentTerm && rf.checkState(LEADER) {
						DPrintf("Leader Election: Server %d loses leadership, because of receiving a larger term from server %d when appending entries", rf.me, server)
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.electionTimer.Reset(randomElectionTimeout())
						rf.state = FOLLOWER
						rf.mu.Unlock()
						rf.persist()
						break
					} else {
						if !rf.checkState(LEADER) {
							// one go routinue may get a larger term when appending entries and change rf.state, other go routinues need to check that and quit if no longer a leader
							return
						}
						if reply.Success {
							// update matchIndex, nextIndex and commitIndex then wakes up applier if needed
							rf.mu.Lock()
							// TODO: 检查nextIndex和matchIndex更新是否正确，尤其是当有truncate情况发生时
							rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
							rf.mu.Unlock()
							rf.UpdateCommitIndexAndWakeApplier()
							break
						}
						// according to reply.ConflictIndex, try to find matchIndex again
						if reply.ConflictIndex != -1 {
							if reply.ConflictIndex < rf.getFirstLogIndex() {
								// unable to match beacause leader has already compacted the needed log entries
								// wait next round to send snapshot
								rf.nextIndex[server] = reply.ConflictIndex + 1
								rf.matchIndex[server] = reply.ConflictIndex
								break
							}
							newPrevLogIndex := reply.ConflictIndex
							// newPrevLogIndex cannot equal to first log index because the first log index in the rf.log slice is the dummy entry which stores the snapshot info or invalid log entry
							if newPrevLogIndex > rf.getFirstLogIndex() && rf.getLogTermAt(newPrevLogIndex) != reply.ConflictTerm {
								conflictTerm := rf.getLogTermAt(newPrevLogIndex)
								DPrintf("Log Replication: Server %d finds its term %d at index %d mismatch with reply %d", rf.me, conflictTerm, newPrevLogIndex, reply.ConflictTerm)
								newPrevLogIndex -= 1
								// make sure newPrevLogIndex > first log index
								for newPrevLogIndex > rf.getFirstLogIndex()+1 && conflictTerm == rf.getLogTermAt(newPrevLogIndex) {
									newPrevLogIndex -= 1
								}
							}
							args = AppendEntriesArgs{
								Term:              rf.currentTerm,
								LeaderId:          rf.me,
								PrevLogIndex:      newPrevLogIndex,
								PrevLogTerm:       rf.getLogTermAt(newPrevLogIndex),
								Entries:           rf.getSubLogFrom(newPrevLogIndex + 1),
								LeaderCommitIndex: rf.commitIndex,
							}
							reply = AppendEntriesReply{}
						}
					}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if !rf.checkState(LEADER) {
		return -1, -1, false
	}
	rf.mu.Lock()
	newLogEntry := LogEntry{
		Term:    rf.currentTerm,
		Index:   rf.getLastLogIndex() + 1,
		Command: command,
	}
	rf.log = append(rf.log, newLogEntry)
	DPrintf("Log Replication: Server %d appends log entries [%d , %d) successfully *LEADER*, log = %v", rf.me, newLogEntry.Index, newLogEntry.Index+1, rf.log)
	rf.mu.Unlock()
	rf.persist()

	return newLogEntry.Index, newLogEntry.Term, true
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
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			// follower -> candidate
			if !rf.checkState(LEADER) {
				rf.StartElection()
				if !rf.checkState(LEADER) {
					rf.electionTimer.Reset(randomElectionTimeout())
				}
			}
		case <-rf.heartbeatTimer.C:
			// leader
			if rf.checkState(LEADER) {
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(stableHeartbeatTimeout())
			}

		}
	}
}

// The applier will be woken up when commitIndex of a server updates in order to asynchronously apply the newly commited log entries or snapshots and send the concerned message to client by channel
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		// newly committed log entries
		newCommittedLogEntries := rf.getSubLogFrom1To2(lastApplied+1, commitIndex+1)

		rf.mu.Unlock()
		for _, logEntry := range newCommittedLogEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
				CommandTerm:  logEntry.Term,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = maxInt(rf.lastApplied, commitIndex)
		rf.mu.Unlock()

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

		state: FOLLOWER,

		electionTimer:  time.NewTimer(randomElectionTimeout()),
		heartbeatTimer: time.NewTimer(stableHeartbeatTimeout()),

		applyCh: applyCh,
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier goroutine to asynchronously apply log entries to state machine and send the message back to client
	go rf.applier()

	return rf
}
