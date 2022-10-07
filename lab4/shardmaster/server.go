package shardmaster

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	// probably a channel to get the message from the listener which listen on the raft's channel
	chMap   map[int]chan Op
	seqMap  map[int64]int // map a client to its sequenceId, to deal with duplicate
	configs []Config      // indexed by config num
	dead    int32         // set by Kill()
}

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

type Op struct {
	// Your data here.
	OpType      string
	SequenceId  int
	ClientId    int64
	JoinServers map[int][]string
	LeaveGids   []int
	MoveShard   int
	MoveGid     int
	QueryNum    int
	QueryConfig Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sm.killed() {
		return
	}
	DPrintf("Join called\n")
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: JOIN, SequenceId: args.SequenceId, ClientId: args.ClientId, JoinServers: args.Servers}
	index, _, _ := sm.rf.Start(op)

	ch := sm.getChannel(index)
	defer func() {
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case newOp := <-ch:
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sm.killed() {
		return
	}
	DPrintf("Leave called\n")
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: LEAVE, SequenceId: args.SequenceId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	index, _, _ := sm.rf.Start(op)

	ch := sm.getChannel(index)
	defer func() {
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case newOp := <-ch:
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sm.killed() {
		return
	}
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: MOVE, SequenceId: args.SequenceId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	index, _, _ := sm.rf.Start(op)

	ch := sm.getChannel(index)
	defer func() {
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case newOp := <-ch:
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sm.killed() {
		return
	}
	_, isLeader := sm.rf.GetState()
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{OpType: QUERY, SequenceId: args.SequenceId, ClientId: args.ClientId, QueryNum: args.Num}
	index, _, _ := sm.rf.Start(op)

	ch := sm.getChannel(index)
	defer func() {
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
	}()

	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case newOp := <-ch:
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.WrongLeader = true
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			reply.Config = newOp.QueryConfig
			return
		}
	case <-timer.C:
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		return
	}
}

func (sm *ShardMaster) JoinHandler(Servers map[int][]string) *Config {
	lastConfig := sm.configs[len(sm.configs)-1]
	newGroup := make(map[int][]string)
	for gid, servers := range lastConfig.Groups {
		newGroup[gid] = servers
	}
	DPrintf("JoinHandler called on group:")
	for gid, servers := range Servers {
		DPrintf(" %d ", gid)
		newGroup[gid] = servers
	}
	// record each group contains how many shards in the previous configuration
	g2s := make(map[int]int)
	for gid, _ := range newGroup {
		// record the group to increase the size of the map
		g2s[gid] = 0
	}
	for _, group := range lastConfig.Shards {
		if group != 0 {
			g2s[group]++
		}
	}
	// load balance the shards to group map
	config := Config{}
	config.Num = len(sm.configs)
	config.Groups = newGroup
	config.Shards = sm.loadBalance(g2s, lastConfig.Shards)
	return &config
}
func (sm *ShardMaster) LeaveHandler(GIDs []int) *Config {
	// use a map to quickly check if one gid is left
	DPrintf("LeaveHandler called on group:")
	for _, gid := range GIDs {
		DPrintf(" %d ", gid)
	}
	leaveMap := make(map[int]bool)
	for _, gid := range GIDs {
		leaveMap[gid] = true
	}
	// new group = lastConfig.Groups - leaveGroups
	lastConfig := sm.configs[len(sm.configs)-1]
	newGroup := make(map[int][]string, 0)
	for gid, servers := range lastConfig.Groups {
		newGroup[gid] = servers
	}
	// delete the leave groups
	for _, GID := range GIDs {
		delete(newGroup, GID)
	}
	g2s := make(map[int]int)
	for gid, _ := range newGroup {
		// record the group to increase the size of the map
		g2s[gid] = 0
	}
	newShards := lastConfig.Shards
	for shard, group := range lastConfig.Shards {
		if group != 0 {
			if leaveMap[group] {
				newShards[shard] = 0
			} else {
				g2s[group]++
			}
		}
	}
	config := Config{}
	config.Num = len(sm.configs)
	config.Groups = newGroup
	config.Shards = sm.loadBalance(g2s, newShards)
	return &config
}
func (sm *ShardMaster) MoveHandler(Shard int, GID int) *Config {
	// move the shard to a new group
	lastConfig := sm.configs[len(sm.configs)-1]
	newGroup := make(map[int][]string, 0)
	newShards := lastConfig.Shards
	for gid, servers := range lastConfig.Groups {
		newGroup[gid] = servers
	}
	newShards[Shard] = GID
	return &Config{
		Num:    len(sm.configs),
		Groups: newGroup,
		Shards: newShards,
	}
}

func (sm *ShardMaster) loadBalance(g2s map[int]int, shards [NShards]int) [NShards]int {
	if len(g2s) == 0 {
		return shards
	}
	DPrintf("Before loadbalancing:\n")
	for shard, gid := range shards {
		DPrintf("Shard %d -> Group %d\n", shard, gid)
	}
	// the shards should be partitioned on each group as even as possible
	sortedGid := sortGroup(g2s)
	length := len(g2s)
	avg := NShards / length
	remainder := NShards % length
	DPrintf("Average load = %d\n", avg)

	// free the shards from overloaded groups
	for i := length - 1; i >= 0; i-- {
		target := avg
		if i < length-remainder && remainder != 0 {
			// first part that should have more load due to uneven number
			target = avg + 1
		}
		if g2s[sortedGid[i]] > target {
			// overloaded
			dec := g2s[sortedGid[i]] - target
			for shard, gid := range shards {
				if dec == 0 {
					break
				}
				if gid == sortedGid[i] {
					dec--
					shards[shard] = 0
				}
			}
			g2s[sortedGid[i]] = target
		}
	}
	// assign the freed load to underloaded groups
	for i := 0; i < length; i++ {
		target := avg
		if i < length-remainder && remainder != 0 {
			// first part that should have more load due to uneven number
			target = avg + 1
		}
		if g2s[sortedGid[i]] < target {
			inc := target - g2s[sortedGid[i]]
			for shard, gid := range shards {
				if inc == 0 {
					break
				}
				if gid == 0 {
					shards[shard] = sortedGid[i]
					inc--
				}
			}
			g2s[sortedGid[i]] = target
		}
	}
	DPrintf("After loadbalancing:\n")
	for shard, gid := range shards {
		DPrintf("Shard %d -> Group %d\n", shard, gid)
	}
	return shards
}
func sortGroup(g2s map[int]int) []int {
	// sort the load of each group by the number of shards each holds
	// return the sorted array of gid in ascending order
	sortedGid := make([]int, 0, len(g2s))
	for gid, _ := range g2s {
		sortedGid = append(sortedGid, gid)
	}
	for i := 0; i < len(g2s)-1; i++ {
		for j := len(g2s) - 1; j > i; j-- {
			// bubble sort
			// sort on gid as well to avoid inconsistency
			if g2s[sortedGid[j-1]] == g2s[sortedGid[j]] {
				if sortedGid[j-1] > sortedGid[j] {
					sortedGid[j], sortedGid[j-1] = sortedGid[j-1], sortedGid[j]
				}
			}
			if g2s[sortedGid[j-1]] > g2s[sortedGid[j]] {
				sortedGid[j], sortedGid[j-1] = sortedGid[j-1], sortedGid[j]
			}
		}
	}
	return sortedGid
}

func (sm *ShardMaster) Listen() {
	// listen for the chan, upon receiving something, apply that command to its state machine by calling Get()/PutAppend()
	for !sm.killed() {
		select {
		case cmd := <-sm.applyCh: // is blocking, so be fast but also serial
			//DPrintf("Listener <- Raft channel\n")
			// send the processed data to the main waiting thread that just sent out it command to the raft and waiting
			if cmd.CommandValid {
				// after installing snapshot, command first time seen, but outdated
				index := cmd.CommandIndex
				op := cmd.Command.(Op)
				sm.mu.Lock()
				if !sm.isDup(op.ClientId, op.SequenceId) {
					// read or write to the kvserver's storage
					if op.OpType == JOIN {
						sm.configs = append(sm.configs, *sm.JoinHandler(op.JoinServers))
					} else if op.OpType == LEAVE {
						sm.configs = append(sm.configs, *sm.LeaveHandler(op.LeaveGids))
					} else if op.OpType == MOVE {
						sm.configs = append(sm.configs, *sm.MoveHandler(op.MoveShard, op.MoveGid))
					} else if op.OpType == QUERY {
						// actually a read
						if op.QueryNum == -1 || op.QueryNum >= len(sm.configs) {
							op.QueryConfig = sm.configs[len(sm.configs)-1]
						} else {
							op.QueryConfig = sm.configs[op.QueryNum]
						}
					}
					sm.seqMap[op.ClientId] = op.SequenceId
					// may need to snapshot if log size too large
				}
				sm.mu.Unlock()
				sm.getChannel(index) <- op
				//DPrintf("Listener -> index channel\n")
			}
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sm.dead, 1)
}
func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) getChannel(index int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	ch, ok := sm.chMap[index]
	if !ok {
		// buffered channel, inorder to help in performance from asynchronous
		sm.chMap[index] = make(chan Op, 1)
		ch = sm.chMap[index]
	}
	return ch
}
func (sm *ShardMaster) isDup(clientId int64, sequenceId int) bool {
	sid, ok := sm.seqMap[clientId]
	if !ok {
		return false
	}
	return sequenceId <= sid
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.seqMap = make(map[int64]int, 0)
	sm.chMap = make(map[int]chan Op, 0)

	go sm.Listen()

	return sm
}
