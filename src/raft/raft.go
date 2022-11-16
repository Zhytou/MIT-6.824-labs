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
	"fmt"
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
	ELECTION_TIMEOUT_MIN = 270
	ELECTION_TIMEOUT_MAX = 400
	HEARTBEAT_TIMEOUT    = 130
)

func RandomElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration((rand.Intn(ELECTION_TIMEOUT_MAX-ELECTION_TIMEOUT_MIN) + ELECTION_TIMEOUT_MIN))
}

func StableHeartbeatTimeout() time.Duration {
	return time.Millisecond * time.Duration(HEARTBEAT_TIMEOUT)
}

func MinInt(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt(a int, b int) int {
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

func (le *LogEntry) IsEqual(other *LogEntry) bool {
	return le.Term == other.Term && le.Index == other.Index
}

func (le *LogEntry) IsEqualInfo(Term int, Index int) bool {
	return le.Term == Term && le.Index == Index
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

func (rf *Raft) PrintState() {
	log.Printf("                            Server %d", rf.me)
	log.Printf("currentTerm %d\t votedfor %d\t commitIndex %d\t state %v\t ", rf.currentTerm, rf.votedFor, rf.commitIndex, rf.state)

	logStr := "log:"
	for _, logEntry := range rf.log {
		logStr += fmt.Sprintf("Index %d Term %d\t", logEntry.Index, logEntry.Term)
	}
	log.Println(logStr)

	if rf.CheckState(LEADER) {
		matchIndexStr := "matchIndex:"
		nextIndexStr := "nextIndex:"

		for server := range rf.peers {
			matchIndexStr += fmt.Sprintf(" %d\t", rf.matchIndex[server])
			nextIndexStr += fmt.Sprintf(" %d\t", rf.nextIndex[server])
		}
		log.Println(matchIndexStr)
		log.Println(nextIndexStr)
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

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	if encoder.Encode(rf.currentTerm) != nil || encoder.Encode(rf.votedFor) != nil || encoder.Encode(rf.log) != nil {
		log.Fatalf("State Persistence: Server %d fails to store its state", rf.me)
	}
	data := buffer.Bytes()
	// log.Printf("State Persistence: Server %d saves state successfully", rf.me)
	rf.persister.SaveRaftState(data)
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
		log.Printf("State Persistence: Server %d reads state successfully", rf.me)
	}
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
			log.Printf("Leader Election: Server %d declines to vote to server %d, because of term", rf.me, args.CandidateId)
			reply.VoteGranted, reply.IsDuplicate = false, false
		} else {
			if args.CandidateId != rf.votedFor {
				log.Printf("Leader Election: Server %d declines to vote to server %d, because it has already voted for server %d", rf.me, args.CandidateId, rf.votedFor)
				reply.VoteGranted, reply.IsDuplicate = false, false
			} else {
				log.Printf("Leader Election: Server %d has already voted to server %d in this term %d", rf.me, rf.votedFor, rf.currentTerm)
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
		if !rf.CheckState(FOLLOWER) {
			if rf.CheckState(LEADER) {
				//rf.matchIndex = nil
				//rf.nextIndex = nil
				rf.electionTimer.Reset(RandomElectionTimeout())
			}
			rf.state = FOLLOWER
		}
	}
	// check on whether candidate's log entries are up-to-date without commitIndex because of persistence does not contain commitIndex
	if args.LastLogTerm < rf.log[len(rf.log)-1].Term || args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < rf.log[len(rf.log)-1].Index {
		log.Printf("Leader Election: Server %d declines to vote to server %d, request{LastLogIndex %d, LastLogTerm %d} rf{LastLogIndex %d, LastLogTerm %d}", rf.me, args.CandidateId, args.LastLogIndex, args.LastLogTerm, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term)
		reply.Term, reply.VoteGranted, reply.IsDuplicate = rf.currentTerm, false, false
		return
	}

	log.Printf("Leader Election: Server %d votes to server %d", rf.me, args.CandidateId)
	rf.mu.Lock()
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(RandomElectionTimeout())
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
	// check if duplicate request
	IsDuplicate bool
	// help leader find match index(unvalid if Success == true)
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term != rf.currentTerm {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
		}
		reply.Term, reply.Success, reply.IsDuplicate, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, false, false, -1, -1
		return
	}

	switch rf.state {
	case LEADER:
		// leader
		rf.mu.Lock()
		log.Printf("Leader Election: Server %d loses leadership, because of rejoining the cluster and receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		//rf.nextIndex = nil
		//rf.matchIndex = nil
		rf.state = FOLLOWER
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.mu.Unlock()
	case CANDIDATE:
		// candidate
		rf.mu.Lock()
		log.Printf("Leader Election: Server %d quits election, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.mu.Unlock()
	default:
		// follower
		rf.mu.Lock()
		if rf.votedFor != -1 {
			log.Printf("Leader Election: Server %d resets votedFor, because of receiving heartbeats from server %d", rf.me, args.LeaderId)
			rf.votedFor = -1
		}
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.mu.Unlock()
	}

	// check if follower's log entries match with the leader
	if args.PrevLogIndex >= len(rf.log) || !rf.log[args.PrevLogIndex].IsEqualInfo(args.PrevLogTerm, args.PrevLogIndex) {
		if args.PrevLogIndex >= len(rf.log) {
			reply.ConflictIndex, reply.ConflictTerm = rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			reply.ConflictIndex = args.PrevLogIndex - 1
			for reply.ConflictIndex >= 1 && reply.ConflictTerm == rf.log[args.PrevLogIndex].Term {
				reply.ConflictIndex -= 1
			}
			reply.ConflictTerm = rf.log[reply.ConflictIndex].Term
		}
		reply.Term, reply.Success, reply.IsDuplicate = rf.currentTerm, false, false
		log.Printf("Log Replication: Server %d declines to append log entries, request{PrevLogIndex %d, PrevLogTerm %d} rf{LastLogIndex %d, LastLogTerm %d}, reply{ConflictIndex %d, ConflictTerm %d}", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term, reply.ConflictIndex, reply.ConflictTerm)
		return
	} else {
		if args.Entries == nil {
			reply.Term, reply.Success, reply.IsDuplicate, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, true, false, -1, -1
		} else {
			truncateIndex := -1
			isMultiAppend := true
			for offset := 1; offset <= len(args.Entries) && offset+args.PrevLogIndex < len(rf.log); offset += 1 {
				if !rf.log[args.PrevLogIndex+offset].IsEqual(&args.Entries[offset-1]) {
					truncateIndex = offset + args.PrevLogIndex
					isMultiAppend = false
					break
				}
			}
			if args.PrevLogIndex+len(args.Entries) >= len(rf.log) {
				isMultiAppend = false
			}

			// check if multiple append
			if !isMultiAppend {
				if truncateIndex != -1 {
					log.Printf("Log Replication: Server %d truncates log entries after %d", rf.me, truncateIndex)
				}
				log.Printf("Log Replication: Server %d appends log entries [%d , %d) successfully", rf.me, args.Entries[0].Index, args.Entries[0].Index+len(args.Entries))
				rf.log = rf.log[:args.PrevLogIndex+1]
				rf.log = append(rf.log, args.Entries...)
				reply.Term, reply.Success, reply.IsDuplicate, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, true, false, -1, -1
			} else {
				log.Printf("Log Replication: Server %d declines to append log entries [%d , %d), because of multiple request", rf.me, args.Entries[0].Index, args.Entries[0].Index+len(args.Entries))
				reply.Term, reply.Success, reply.IsDuplicate, reply.ConflictIndex, reply.ConflictTerm = rf.currentTerm, true, true, -1, -1
			}
		}

		// only commits already matched log entries [0, argsPrevLogIndex + 1) when follower finds larger leader commitIndex
		if args.LeaderCommitIndex > rf.commitIndex && args.PrevLogIndex+len(args.Entries) > rf.commitIndex {
			rf.mu.Lock()
			log.Printf("Log Replication: Server %d commits (%d , %d] log entries successfully", rf.me, rf.commitIndex, MinInt(args.LeaderCommitIndex, args.PrevLogIndex))
			rf.commitIndex = MinInt(args.LeaderCommitIndex, args.PrevLogIndex+len(args.Entries))
			rf.mu.Unlock()
			// wake up applier
			rf.applyCond.Broadcast()
		}
	}
	rf.persist()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	if rf.CheckState(FOLLOWER) {
		log.Printf("Leader Election: Server %d first enters election", rf.me)
		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
	} else {
		log.Printf("Leader Election: Server %d enters election again", rf.me)
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
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			reply := RequestVoteReply{}

			if rf.sendRequestVote(server, &args, &reply) {
				// check if follower grants only valid vote for each term
				if reply.Term == rf.currentTerm && reply.VoteGranted && !reply.IsDuplicate {
					grantedVoteNum += 1
					if grantedVoteNum >= len(rf.peers)/2+1 && !rf.CheckState(LEADER) {
						// candidate -> leader
						log.Printf("Leader Election: Server %d wins election", rf.me)
						rf.mu.Lock()
						rf.votedFor = -1
						rf.matchIndex = make([]int, len(rf.peers))
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i += 1 {
							rf.nextIndex[i] = len(rf.log)
						}
						rf.state = LEADER
						rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
						rf.mu.Unlock()
						rf.persist()
						// must send out heartbeats immediately after winning an election
						rf.BroadcastHeartbeat()
					}
				}
				if reply.Term > rf.currentTerm && !reply.VoteGranted {
					log.Printf("Leader Election: Server %d quits election, because of receiving a larger term of %d from server %d when requesting votes", rf.me, reply.Term, server)
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
	// only when log entries in current term have been committed, the commitIndex can be updated(figure 8 in enxtended raft paper)
	if rf.log[newCommitIndex].Term == rf.currentTerm && newCommitIndex > rf.commitIndex {
		rf.mu.Lock()
		log.Printf("Log Replication: Server %d commits (%d , %d] log entries successfully *LEADER*", rf.me, rf.commitIndex, newCommitIndex)
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
			if rf.matchIndex[server] == 0 && rf.nextIndex[server] > rf.matchIndex[server]+1 {
				// leader just steps on and needs to find out match index for each follower
				args := AppendEntriesArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      rf.nextIndex[server] - 1,
					PrevLogTerm:       rf.log[rf.nextIndex[server]-1].Term,
					Entries:           nil,
					LeaderCommitIndex: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				// reply.Success == true means follower's log entries match the leader's therefore jump out of the loop whose function is to find matchIndex
				for rf.sendAppendEntries(server, &args, &reply) && !reply.Success {
					if reply.Term > rf.currentTerm && rf.CheckState(LEADER) {
						log.Printf("Leader Election: Server %d loses leadership, because of receiving a larger term from server %d when appending entries", rf.me, server)
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						//rf.nextIndex = nil
						//rf.matchIndex = nil
						rf.electionTimer.Reset(RandomElectionTimeout())
						rf.state = FOLLOWER
						rf.mu.Unlock()
						rf.persist()
					} else {
						if !rf.CheckState(LEADER) {
							// one go routinue may get a larger term when appending entries and change rf.state, other go routinues need to check that and quit if no longer a leader
							return
						}
						// according to reply.ConflictIndex, try to find matchIndex again
						if reply.ConflictIndex != -1 && reply.ConflictTerm != -1 {
							var newPrevLogIndex int = reply.ConflictIndex
							if rf.log[newPrevLogIndex].Index != reply.ConflictIndex || rf.log[newPrevLogIndex].Term != reply.ConflictTerm {
								ConflictTerm := rf.log[args.PrevLogIndex].Term
								for newPrevLogIndex >= 1 && ConflictTerm == rf.log[newPrevLogIndex].Term {
									newPrevLogIndex -= 1
								}
							}
							args = AppendEntriesArgs{
								Term:              rf.currentTerm,
								LeaderId:          rf.me,
								PrevLogIndex:      newPrevLogIndex,
								PrevLogTerm:       rf.log[newPrevLogIndex].Term,
								Entries:           nil,
								LeaderCommitIndex: rf.commitIndex,
							}
							reply = AppendEntriesReply{}
						}
					}
				}
				if reply.Success {
					// leader just steps on and successfully finds out match index of this follower by decrement of args.PrevLogIndex (with a initial value of the rf.log length)
					rf.mu.Lock()
					rf.nextIndex[server] = args.PrevLogIndex + 1
					rf.matchIndex[server] = args.PrevLogIndex
					rf.mu.Unlock()
				}
			} else {
				// only the follower's log entries have matched the leader's can enter this branch
				args := AppendEntriesArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					PrevLogIndex:      rf.matchIndex[server],
					PrevLogTerm:       rf.log[rf.matchIndex[server]].Term,
					Entries:           rf.log[rf.nextIndex[server]:],
					LeaderCommitIndex: rf.commitIndex,
				}
				reply := AppendEntriesReply{}

				if rf.sendAppendEntries(server, &args, &reply) {
					if reply.Term > rf.currentTerm && rf.CheckState(LEADER) {
						log.Printf("Leader Election: Server %d loses leadership, because of receiving a larger term from server %d when appending entries", rf.me, server)
						rf.mu.Lock()
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.electionTimer.Reset(RandomElectionTimeout())
						rf.state = FOLLOWER
						rf.mu.Unlock()
						rf.persist()
					} else {
						if !rf.CheckState(LEADER) {
							// one go routinue may get a larger term when appending entries and change rf.state, other go routinues need to check that and quit if no longer a leader
							return
						}
						// reply.Success == true means follower's log entries match the leader's therefore jump out of the loop whose function is to find matchIndex
						if reply.Success {
							if args.Entries != nil && !reply.IsDuplicate {
								// update matchIndex, nextIndex and commitIndex then wakes up applier if needed
								// check if multiple append entries(重复复制日志)
								rf.mu.Lock()
								rf.matchIndex[server] += len(args.Entries)
								rf.nextIndex[server] += len(args.Entries)
								rf.mu.Unlock()
								rf.UpdateCommitIndexAndWakeApplier()
							}
						} else {
							rf.matchIndex[server] = 0
							rf.nextIndex[server] = len(rf.log)
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
	if !rf.CheckState(LEADER) {
		return -1, -1, false
	}

	rf.mu.Lock()
	newLogEntry := LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log),
		Command: command,
	}
	log.Printf("Log Replication: Server %d appends log entries [%d , %d) successfully *LEADER*", rf.me, len(rf.log), len(rf.log)+1)
	rf.log = append(rf.log, newLogEntry)
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
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			// follower -> candidate
			if !rf.CheckState(LEADER) {
				rf.StartElection()
				if !rf.CheckState(LEADER) {
					rf.electionTimer.Reset(RandomElectionTimeout())
				}
			}
		case <-rf.heartbeatTimer.C:
			// leader
			if rf.CheckState(LEADER) {
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}

		}
	}
}

// The applier will be woken up when commitIndex of a server updates in order to asynchronously apply the newly commited log entries and send the concerned message to client by channel
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// if there is no need to apply entries, just release CPU and wait other goroutine's signal if they commit new entries
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		//firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		// newly committed log entries
		var newCommittedLogEntries []LogEntry
		if commitIndex+1 >= len(rf.log) {
			newCommittedLogEntries = rf.log[lastApplied+1:]
		} else {
			newCommittedLogEntries = rf.log[lastApplied+1 : commitIndex+1]
		}

		rf.mu.Unlock()
		for _, logEntry := range newCommittedLogEntries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      logEntry.Command,
				CommandIndex: logEntry.Index,
			}
		}
		rf.mu.Lock()
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		// use Max(rf.lastApplied, commitIndex) rather than commitIndex directly to avoid concurrently InstallSnapshot rpc causing lastApplied to rollback
		rf.lastApplied = MaxInt(rf.lastApplied, commitIndex)
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

		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),

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
