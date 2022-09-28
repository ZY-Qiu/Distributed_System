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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

var HeartbeatTimeout = time.Millisecond * 100

type State int

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

type LogEntry struct {
	Command string
	Term    int
}

type VoteState int

const (
	UPDATE VoteState = iota
	NORMAL
	VOTED
	KILLED
)

type PeerState int

const (
	PEERUPDATE PeerState = iota
	PEERNORMAL
	PEERKILLED
)

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

	// Persistent
	currentTerm int
	votedFor    int
	log         []LogEntry
	// Volatile on all servers
	commmitIndex    int // index of the highest log that the majority has applied and replied to be known
	lastApplied     int // index of the highest log that the server itself has applied
	startTime       time.Time
	electionTimeout int // 500 - 800 ms
	state           State
	applyCh         chan ApplyMsg
	// Volatile on leader, Reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones to be appened
	PrevLogTerm  int // term of prevLogIndex entry, rf.currentTerm
	Entries      []LogEntry
	LeaderCommit int // rf.commitIndex
}
type AppendEntriesReply struct {
	Term    int
	Success bool
	State   PeerState
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// called by leader
	// will reset the receiving peers's election timeout
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	switch reply.State {
	case PEERKILLED:
		return false
	case PEERUPDATE:
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
	case PEERNORMAL:
		return true
	}
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// peer's RPC handler method, called by leader
	// will reset the election timeout
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.State = PEERKILLED
		reply.Success = false
		reply.Term = -1
	} else if args.Term < rf.currentTerm {
		reply.State = PEERUPDATE
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		// now args at least as up to date
		// update self
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = FOLLOWER // not sure about, but possibily all servers become followers doesn't matter, they can recover
		rf.startTime = time.Now()
		rand.Seed(time.Now().UnixNano())
		rf.electionTimeout = rand.Intn(300) + 500

		reply.State = PEERNORMAL
		reply.Success = true
		reply.Term = rf.currentTerm
	}
	return
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	func() {
		term = rf.currentTerm
		isleader = (rf.state == LEADER)
	}()
	return term, isleader
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
	State       VoteState
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// on peer side
	// 1. Reply false if args.Term < currentTerm
	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	rf.mu.Lock()
	if rf.killed() {
		reply.State = KILLED
		reply.Term = -1
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// candidata outdated
	if rf.currentTerm > args.Term {
		reply.State = UPDATE
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// self outdated
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	// now all in the same term
	// already voted for other candidates other than the requested one
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.State = VOTED
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// now check if the candidate’s log is at least as up-to-date as receiver’s log
	logIndex := len(rf.log)
	logTerm := 0
	if logIndex > 0 {
		logTerm = rf.log[logIndex-1].Term
	}
	if args.LastLogIndex < logIndex || args.LastLogTerm < logTerm {
		reply.State = UPDATE
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	// now vote for the candidate
	rf.votedFor = args.CandidateId
	reply.State = NORMAL
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// Should reset the electionTimeout if grants vote to candidate
	rf.startTime = time.Now()
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(300) + 500
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ballot int) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm != args.Term {
		// not the same term after sending a RequestVote
		if rf.currentTerm < reply.Term {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
		}
		return ok
	} else {
		// in the same term, thus still candidate state
		switch reply.State {
		case UPDATE:
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
			}
			return ok
		case NORMAL, VOTED:
			// peers' term == candidate's term
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				ballot++
			}
			if ballot >= (len(rf.peers)/2)+1 {
				ballot = 0
				// setting oneself as leader, reinitialize nextIndex
				if rf.state == LEADER {
					return ok
				}
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + 1
				}
			}
		case KILLED:
			return false
		}
	}
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
	// fmt.Printf("Server %v killed by tester.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Heartbeat() {
	// a background goroutine to periodically check and send out RequestVote when it hasn't hear from others for a while
	// use time.Sleep() to periodically check time.Since(rf.startTime) > time
	for !rf.killed() {
		rf.mu.Lock()
		// if electionTimeout
		if time.Since(rf.startTime).Milliseconds() > int64(rf.electionTimeout) {
			switch rf.state {
			case FOLLOWER:
				// make oneself a cnadidate
				rf.state = CANDIDATE
				fallthrough
			case CANDIDATE:
				// Increment currentTerm
				rf.currentTerm++
				// Vote for self
				rf.votedFor = rf.me
				ballot := 1
				// Reset election timer
				rf.startTime = time.Now()
				rand.Seed(time.Now().UnixNano())
				rf.electionTimeout = rand.Intn(300) + 500
				// Send RequestVote RPCs to all other servers
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					var lastLogTerm int
					if len(rf.log) > 0 {
						lastLogTerm = rf.log[len(rf.log)-1].Term
					} else {
						lastLogTerm = 0
					}
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log),
						LastLogTerm:  lastLogTerm,
					}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply, ballot)

				}
			case LEADER:
				// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
			}
		}
		// if LEADER STATE, send heartbeat
		if rf.state == LEADER {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				var lastLogTerm int
				if len(rf.log) > 0 {
					lastLogTerm = rf.log[len(rf.log)-1].Term
				} else {
					lastLogTerm = 0
				}
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: len(rf.log),
					PrevLogTerm:  lastLogTerm,
					Entries:      nil,
					LeaderCommit: rf.commmitIndex,
				}
				reply := AppendEntriesReply{}
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}

		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
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
	rf.currentTerm = 0
	rf.votedFor = -1      // means null
	rf.log = []LogEntry{} // first index in log should be 1, cuz other index intialized to 0, marking non
	// Volatile on all servers
	rf.commmitIndex = 0
	rf.lastApplied = 0
	rf.startTime = time.Now()
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rand.Intn(300) + 500 // 500 - 800 ms
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	// Volatile on leader, Reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Heartbeat()

	return rf
}