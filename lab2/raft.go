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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
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
	Command interface{}
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
	PEERMISMATCH
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
	electionTimeout int // 200 - 400 ms
	state           State
	applyCh         chan ApplyMsg
	timer           *time.Timer
	startTime       time.Time
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
	XTerm   int // term of the conflicting term at PrevLogIndex
	XIndex  int // index of the first entry in XTerm
	XLen    int // length of the entire log, used if follower has no log entry at all
}

func (rf *Raft) leaderSend(isHB bool) {
	// sendAppendEntries to every other servers
	// If new entries appended into leader's log
	// Periodically send heartbeat
	// If AppendEntries fails because of log inconsistency, retry
	if !rf.killed() && rf.state == LEADER {
		//DPrintf("Leader %v sending in term %v\n", rf.me, rf.currentTerm)
		// should already holding the lock when calling this function
		// voteSum initially set to 1
		voteSum := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      nil,
				LeaderCommit: rf.commmitIndex,
			}
			reply := AppendEntriesReply{}
			// rf.nextIndex[i] is at least 1, index starts at 1
			args.PrevLogIndex = rf.nextIndex[i] - 1
			//DPrintf("Server %d choose PrevLogIndex %d/%d for server %d\n", rf.me, args.PrevLogIndex, len(rf.log), i)
			if args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
			}
			// have no chance but have to periodically check to send new logs, heartbeat can serve to be a good chance
			//if !isHB {
			args.Entries = rf.log[args.PrevLogIndex:]
			//}
			go rf.sendAppendEntries(i, &args, &reply, &voteSum)
		}
	}
}

func (rf *Raft) commit() {
	for rf.lastApplied < rf.commmitIndex {
		//DPrintf("Sever %d applying log index %d in term %d\n", rf.me, rf.lastApplied+1, rf.currentTerm)
		apply := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[rf.lastApplied].Command,
			CommandIndex: rf.lastApplied + 1,
		}
		rf.lastApplied++
		rf.applyCh <- apply
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, sum *int) bool {
	// called by leader
	// will reset the receiving peers's election timeout
	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		// not the same term after sending a RequestVote
		if rf.currentTerm < reply.Term {
			DPrintf("Sender %d increase its term from %d to %d in AppendEntries from %d\n", rf.me, rf.currentTerm, reply.Term, server)
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		return ok
	}
	switch reply.State {
	case PEERKILLED:
		return false
	case PEERUPDATE:
		if rf.currentTerm < reply.Term {
			DPrintf("Sender %d increase its term from %d to %d in AppendEntries from %d\n", rf.me, rf.currentTerm, reply.Term, server)
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}
	case PEERMISMATCH:
		if rf.currentTerm == args.Term {
			// if XTerm == -1, backup to XLen
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				// has conflicting term at PrevLogIndex
				// if leader doesn't have reply.XTerm, backup to XIndex
				// if leader has XTerm, backup to the last enrty that has the conflicting XTerm
				backupIndex := args.PrevLogIndex
				for i := args.PrevLogIndex - 1; i > 0; i-- {
					if rf.log[i].Term == reply.XTerm {
						backupIndex = i + 1 + 1
						break
					}
					if rf.log[i].Term < reply.XTerm {
						// XIndex is the starting index of the followers' conflicting term
						backupIndex = reply.XIndex
						break
					}
				}
				rf.nextIndex[server] = backupIndex
			}
			//DPrintf("Leader %d set nextIndex of server %d to %d/%d\n", rf.me, server, rf.nextIndex[server], len(rf.log))
			// retry
			// costing too much bandwidth as well as cpu power, unable to pass the test
			//rf.leaderSend(false)
		}
	case PEERNORMAL:
		// now reply.Term has to be equal to curretTerm
		if reply.Success && *sum < (len(rf.peers)/2)+1 {
			*sum++
		}
		if rf.nextIndex[server] > len(rf.log)+1 {
			return ok
		}
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		//DPrintf("Leader %d set nextIndex of server %d to %d/%d in term %d\n", rf.me, server, rf.nextIndex[server], len(rf.log), rf.currentTerm)

		//DPrintf("Sever %d hears reply from %d, sum=%d in term %d.\n", rf.me, server, *sum, rf.currentTerm)
		if *sum >= (len(rf.peers)/2)+1 {
			*sum = 0
			if len(rf.log) == 0 {
				return ok
			}
			//DPrintf("Sever %d commitIndex=%d, inc to %d in term %d.\n", rf.me, rf.commmitIndex, rf.matchIndex[server], rf.currentTerm)
			//DPrintf("Log's length is %d, will commit to %d\n", len(rf.log), rf.nextIndex[server]-1)
			if rf.commmitIndex < rf.matchIndex[server] && rf.currentTerm == rf.log[rf.matchIndex[server]-1].Term {
				// up till the commit place
				rf.commmitIndex = rf.matchIndex[server]
				rf.commit()
			}
		}

	}
	return true
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
		return
	}
	if args.Term < rf.currentTerm {
		// not really needed
		//DPrintf("Server %d reject AppendEntries for leader %d's term %d is outdated in term %d", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.State = PEERUPDATE
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// now args have a term that is at least as up to date
	// update self
	needPersist := (rf.currentTerm != args.Term) || (rf.votedFor != args.LeaderId) || (rf.state != FOLLOWER)
	if args.Term > rf.currentTerm {
		DPrintf("Receiver %d increase its term from %d to %d in AppendEntries from %d\n", rf.me, rf.currentTerm, args.Term, args.LeaderId)
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.LeaderId
	rf.state = FOLLOWER // not sure about, but possibily all servers become followers doesn't matter, they can recover
	rf.RestartTimer()
	//DPrintf("Server %d receives heartbeat in term %d\n", rf.me, rf.currentTerm)
	reply.Term = rf.currentTerm
	// check
	// args.PrevLogIndex can be 0
	if args.PrevLogIndex > 0 && (len(rf.log) < args.PrevLogIndex || rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm) {
		if len(rf.log) >= args.PrevLogIndex {
			reply.XTerm = rf.log[args.PrevLogIndex-1].Term
			reply.XIndex = func(term, index int) int {
				for index > 0 && (rf.log[index-1].Term == term) {
					index--
				}
				return index + 1
			}(reply.XTerm, args.PrevLogIndex)
			reply.XLen = len(rf.log)
		} else {
			// log entry doesn't exist at PrevLogIndex
			reply.XTerm = -1
			reply.XIndex = -1
			reply.XLen = len(rf.log)
		}
		reply.Success = false
		reply.State = PEERMISMATCH
	} else {
		//DPrintf("Sever %d accepts AL in term %d\n", rf.me, rf.currentTerm)
		//DPrintf("PrevLogIndex = %d, length = %d\n", args.PrevLogIndex, len(args.Entries))
		reply.State = PEERNORMAL
		reply.Success = true
		// delet any conflict logs
		for i := 0; i < len(args.Entries); i++ {
			// delete conflicts and all that follows if exists
			// append the new entries
			if i+args.PrevLogIndex >= len(rf.log) {
				//DPrintf("Sever %d appends log in term %d\n", rf.me, rf.currentTerm)
				rf.log = append(rf.log, args.Entries[i:]...)
				needPersist = true
				break
			}
			if rf.log[i+args.PrevLogIndex].Term != args.Entries[i].Term {
				//DPrintf("Sever %d appends log in term %d\n", rf.me, rf.currentTerm)
				rf.log = rf.log[:i+args.PrevLogIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				needPersist = true
				break
			}
		}
		// update the commitIndex
		if args.LeaderCommit > rf.commmitIndex {
			rf.commmitIndex = func(a, b int) int {
				// commitIndex = min(leaderCommit, index of last new entry)
				if a < b {
					return a
				}
				return b
			}(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		}
	}
	if needPersist {
		rf.persist()
	}
	rf.commit()
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	//DPrintf("Server %d persisting state in term %d\n", rf.me, rf.currentTerm)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cT int
	var vF int
	var log []LogEntry
	if d.Decode(&cT) != nil ||
		d.Decode(&vF) != nil ||
		d.Decode(&log) != nil {
		DPrintf("Server %d read broken persistence in term %d\n", rf.me, rf.currentTerm)
		return
	} else {
		rf.currentTerm = cT
		rf.votedFor = vF
		rf.log = log
	}
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
	defer rf.mu.Unlock()
	if rf.killed() {
		//DPrintf("Server %d killed\n", rf.me)
		reply.State = KILLED
		reply.Term = -1
		reply.VoteGranted = false
		return
	}
	// candidata outdated
	if rf.currentTerm > args.Term {
		DPrintf("Server %d reject vote for candidata %d's term %d is outdated in term %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
		reply.State = UPDATE
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// self outdated
	if rf.currentTerm < args.Term {
		DPrintf("Receiver %d increase its term from %d to %d in candidata %d's RequestVote\n", rf.me, rf.currentTerm, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
	}
	// now all in the same term
	// already voted for other candidates other than the requested one
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.State = VOTED
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// now check if the candidate’s log is at least as up-to-date as receiver’s log
	logIndex := len(rf.log)
	logTerm := 0
	if logIndex > 0 {
		logTerm = rf.log[logIndex-1].Term
	}
	// Incorrect if args.LastLogIndex < logIndex || args.LastLogTerm < logTerm
	// then maybe 2 outof 3 servers may never elect leader, if one has smaller term, one has smaller index
	if args.LastLogTerm < logTerm || (args.LastLogTerm == logTerm && args.LastLogIndex < logIndex) {
		if args.LastLogTerm < logTerm {
			DPrintf("Server %d reject vote from %d for candidata's logTerm %d < it own lowTerm %d in term %d", rf.me, args.CandidateId, args.LastLogTerm, logTerm, rf.currentTerm)
		} else {
			DPrintf("Server %d reject vote from %d for candidata's logIndex %d < it own lowIndex %d in term %d", rf.me, args.CandidateId, args.LastLogIndex, logIndex, rf.currentTerm)
		}
		reply.State = UPDATE
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// now vote for the candidate
	DPrintf("Server %d vote for %d as leader in term %d", rf.me, args.CandidateId, rf.currentTerm)
	rf.votedFor = args.CandidateId
	reply.State = NORMAL
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// Should reset the electionTimeout if grants vote to candidate
	rf.RestartTimer()
	// persist
	rf.persist()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, ballot *int) bool {
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
			DPrintf("Sender %d increase its term from %d to %d in RequestVote from %d\n", rf.me, rf.currentTerm, reply.Term, server)
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			rf.persist()
		}
		return ok
	} else {
		// in the same term, thus still candidate state
		switch reply.State {
		case UPDATE:
			if rf.currentTerm < reply.Term {
				DPrintf("Sender %d increase its term from %d to %d in RequestVote from %d\n", rf.me, rf.currentTerm, reply.Term, server)
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
				rf.votedFor = -1
				rf.persist()
			}
			return ok
		case NORMAL, VOTED:
			// peers' term == candidate's term
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				*ballot++
			}
			if *ballot >= (len(rf.peers)/2)+1 {
				*ballot = 0
				// setting oneself as leader, reinitialize nextIndex
				if rf.state == LEADER {
					return ok
				}
				rf.state = LEADER
				rf.nextIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.log) + 1
					rf.matchIndex[i] = 0
				}
				DPrintf("Server %d/%d become leader in term %d and set nextIndex to %d.\n", rf.me, len(rf.peers), rf.currentTerm, len(rf.log)+1)
				rf.leaderSend(false)
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
	if rf.killed() {
		return index, term, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = (rf.state == LEADER)
	if isLeader {
		// append the command to the log
		log := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		rf.log = append(rf.log, log)
		term = rf.currentTerm
		index = len(rf.log)
		rf.persist()
		//DPrintf("Command written into leader %d's log\n", rf.me)
		rf.leaderSend(false)
	}
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
	// DPrintf("Server %v killed by tester.\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Heartbeat() {
	// a background goroutine to periodically check and send out RequestVote when it hasn't hear from others for a while
	// use time.Sleep() to periodically check
	for !rf.killed() {
		rf.mu.Lock()
		// if LEADER STATE, send heartbeat
		if rf.state == LEADER {
			//DPrintf("Server %d sending heartbeat in term %d\n", rf.me, rf.currentTerm)
			//sum := 1
			rf.leaderSend(true)
		}
		rf.mu.Unlock()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) RestartTimer() {
	rf.startTime = time.Now()
	rf.electionTimeout = rand.Intn(200) + 200
	//rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
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
	rf.votedFor = -1             // means null
	rf.log = make([]LogEntry, 0) // first index in log should be 1, cuz other index intialized to 0, marking non
	// Volatile on all servers
	rf.commmitIndex = 0
	rf.lastApplied = 0
	//rf.electionTimeout = rand.Intn(200) + 200
	//rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	rf.RestartTimer()
	rf.state = FOLLOWER
	rf.applyCh = applyCh
	// Volatile on leader, Reinitialized after election
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Heartbeat()
	go rf.ElectionTimeout()

	return rf
}

func (rf *Raft) ElectionTimeout() {
	//defer rf.timer.Stop()
	for !rf.killed() {
		// if electionTimeout
		//select {
		//case <-rf.timer.C:
		st := time.Now()
		// return immediately if negative time
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.startTime.Before(st) {
			switch rf.state {
			case FOLLOWER:
				// make oneself a cnadidate
				rf.state = CANDIDATE
				fallthrough
			case CANDIDATE:
				// Increment currentTerm
				rf.currentTerm++
				logIndex := len(rf.log)
				logTerm := 0
				if logIndex > 0 {
					logTerm = rf.log[logIndex-1].Term
				}
				DPrintf("Server %d starts a vote in term %d with log size %d, logTerm %d, logIndex %d.\n", rf.me, rf.currentTerm, len(rf.log), logTerm, logIndex)
				// Vote for self
				rf.votedFor = rf.me
				rf.persist()
				ballot := 1
				// Reset election timer
				rf.RestartTimer()
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
					go rf.sendRequestVote(i, &args, &reply, &ballot)

				}
			case LEADER:
				// Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server
			}
		}
		rf.mu.Unlock()
		//}
	}
}
