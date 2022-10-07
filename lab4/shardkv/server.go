package shardkv

// import "../shardmaster"
import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const (
	GET          = "Get"
	PUT          = "Put"
	APPEND       = "Append"
	InstallShard = "InstallShard"
	UpdataConfig = "UpdataConfig"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType     string
	SequenceId int
	ClientId   int64
	OpKey      string
	OpValue    string
	Index      int // raft's log index
	Err        Err
}
type Shard map[string]string

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int   // snapshot if log grows this big
	dead         int32 // set by Kill()

	// Your definitions here.
	chMap  map[int]chan Op
	seqMap map[int64]int // map a client to its sequenceId, to deal with duplicate
	// we have to devide the storage in to different shards, then we can move the shard to other servers
	shardStorage map[string]string     // basic storage
	toOutShards  map[int]map[int]Shard // configNum -> shard, the shards needed by other groups to be migrated
	comeInShards map[int]int           // sharad -> config number, which shards it needs from other group in which configNum
	myShards     map[int]bool          // to record which shard it can offer
	commitIndex  int
	smk          *shardmaster.Clerk
	Config       shardmaster.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.matchShard(args.Key) {
		DPrintf("Get() wrong group, return\n")
		reply.Err = ErrWrongGroup
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:     GET,
		SequenceId: args.SeqId,
		ClientId:   args.ClientId,
		OpKey:      args.Key,
	}
	//DPrintf("Server's Get calls raft.Start()\n")
	op.Index, _, isLeader = kv.rf.Start(op)

	// the server will listen to a channel correspond to this index for a reply from the raft to the listener, then itself
	// the client is allowed to receive data after itself's concurrent operation, subject to linearizablity
	ch := kv.getChannel(op.Index, true)
	// should we delete this entry in the chMap after get the data from this channel?
	defer func(index int) {
		kv.mu.Lock()
		delete(kv.chMap, index)
		kv.mu.Unlock()
	}(op.Index)
	// it will wait for some time then give up
	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()

	DPrintf("Get waiting for reply on server %d on index channel %d", kv.me, op.Index)
	select {
	case newOp := <-ch:
		close(ch)
		DPrintf("Get <- index channel %d", op.Index)
		// able to reply to client
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.Err = ErrWrongLeader
			DPrintf("Get ErrWrongLeader")
			return
		} else {
			if newOp.Err == ErrWrongGroup {
				DPrintf("Get ErrWrongGroup")
				reply.Err = ErrWrongGroup
			} else if newOp.Err == ErrNoKey {
				DPrintf("Get ErrNoKey")
				reply.Err = ErrNoKey
			} else {
				reply.Value = newOp.OpValue
				reply.Err = OK
			}
		}
	case <-timer.C:
		DPrintf("Server's Get on channel %d timeout\n", op.Index)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	if !kv.matchShard(args.Key) {
		reply.Err = ErrWrongGroup
		DPrintf("PutAppend() wrong group, return\n")
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:     args.Op,
		SequenceId: args.SeqId,
		ClientId:   args.ClientId,
		OpKey:      args.Key,
		OpValue:    args.Value,
	}
	//DPrintf("Server's PutAppend calls raft.Start()\n")
	op.Index, _, isLeader = kv.rf.Start(op)
	// the server will listen to a channel correspond to this index for a reply from the raft to the listener, then itself
	ch := kv.getChannel(op.Index, true)
	// should we delete this entry in the chMap after get the data from this channel?
	defer func(index int) {
		kv.mu.Lock()
		delete(kv.chMap, index)
		kv.mu.Unlock()
	}(op.Index)
	// it will wait for some time then give up
	timer := time.NewTicker(200 * time.Millisecond)
	defer timer.Stop()

	DPrintf("Put/Append waiting for reply on server %d on index channel %d", kv.me, op.Index)
	select {
	case newOp := <-ch:
		close(ch)
		DPrintf("Put/Append <- index channel %d", op.Index)
		// able to reply to client
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			// the log entry of the same index in the raft's log is overwritten by other leader's log
			//DPrintf("Server's raft switched leader during PutAppend\n")
			reply.Err = ErrWrongLeader
			DPrintf("Put/Append ErrWrongLeader")
			return
		} else if newOp.Err == ErrWrongGroup {
			reply.Err = ErrWrongGroup
			DPrintf("Put/Append ErrWrongGroup")
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		DPrintf("Server's PutAppend on channel %d timeout\n", op.Index)
		reply.Err = ErrWrongLeader
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}
func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) Listen() {
	// listen for the chan, upon receiving something, apply that command to its state machine by calling Get()/PutAppend()
	for !kv.killed() {
		select {
		case cmd := <-kv.applyCh: // is blocking, so be fast but also serial
			DPrintf("Goroutine number: %v", runtime.NumGoroutine())
			DPrintf("Group %d listener %d <- Raft channel\n", kv.gid, kv.me)
			// send the processed data to the main waiting thread that just sent out it command to the raft and waiting
			if cmd.CommandValid {
				kv.apply(cmd)
			}
			if cmd.SnapshotValid {
				// is a snapshot, should install the snapshot because the server is lagged behind
				kv.mu.Lock()
				if kv.commitIndex < cmd.SnapshotIndex {
					kv.DecodeSnapshot(cmd.Snapshot)
					kv.commitIndex = cmd.SnapshotIndex
				}
				kv.mu.Unlock()
			}
		}
	}
}
func (kv *ShardKV) updateShard(reply MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("updateShard() called\n")
	if reply.ConfigNum != kv.Config.Num-1 {
		// enter the new config, the shards needed is stored in other groups' toOutShard[kv.Config.Num - 1]
		return
	}
	delete(kv.comeInShards, reply.Shard)
	// if already has the shard, do nothing
	if _, ok := kv.myShards[reply.Shard]; !ok {
		kv.myShards[reply.Shard] = true
		for k, v := range reply.Data {
			kv.shardStorage[k] = v
		}
		for k, v := range reply.SeqMap {
			kv.seqMap[k] = max(kv.seqMap[k], v)
		}
	}
	DPrintf("updateShard() exited\n")
}
func (kv *ShardKV) updateConfig(config shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("updataConfig() called\n")
	// update the current config,
	if config.Num <= kv.Config.Num {
		return
	}
	oldcfg, toOutShard := kv.Config, kv.myShards
	kv.myShards, kv.Config = make(map[int]bool), config
	// record which shard needs to be migrated
	for shard, gid := range config.Shards {
		if gid != kv.gid {
			continue
		}
		if _, ok := toOutShard[shard]; ok || oldcfg.Num == 0 {
			// already exists in itself storage
			kv.myShards[shard] = true
			delete(toOutShard, shard)
		} else {
			// need shards from other groups
			kv.comeInShards[shard] = oldcfg.Num // why? ask in old configNum, itself not updated until migration
		}
	}
	// also has to record which shards are ready to serve after migration
	if len(toOutShard) > 0 {
		// left to be migrated to other groups
		kv.toOutShards[oldcfg.Num] = make(map[int]Shard)
		for shard := range toOutShard {
			out := make(Shard)
			for k, v := range kv.shardStorage {
				if key2shard(k) == shard {
					out[k] = v
					delete(kv.shardStorage, k)
				}
			}
			kv.toOutShards[oldcfg.Num][shard] = out
		}
	}
	DPrintf("updataConfig() exited\n")
}

func (kv *ShardKV) checkConfig() {
	// detect if the config has changed from the shardmaster every 100ms
	// query the shardmaster for the config, compare the configNum
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.comeInShards) > 0 {
		// only leader ask the shardmaster
		// Process re-configurations one at a time, in order
		kv.mu.Unlock()
		return
	}
	next := kv.Config.Num + 1
	kv.mu.Unlock()
	config := kv.smk.Query(next)
	if config.Num == next {
		DPrintf("Learder %d in group %d sending Config to all followers\n", kv.me, kv.gid)
		kv.rf.Start(config)
	}
}
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	// first compare the configNum
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.ConfigNum >= kv.Config.Num {
		// should be Config.Num - 1 or smaller
		return
	}
	// than deep copy the shard to the reply
	reply.Data, reply.SeqMap = kv.deepCopy(args.ConfigNum, args.Shard)
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
	reply.Err = OK
	// finally change itself's shard? not really needed
	return
}
func (kv *ShardKV) SendShardMigration() {
	if kv.killed() {
		return
	}
	// reverse order of the follow 2 lines causes deadlock
	// here locks kv.mu, then rf.GetState() locks kv.rf.mu,
	// but kv.rf.commit() blocks on channel waiting for Listen() which acquires kv.mu
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if len(kv.comeInShards) == 0 || !isLeader {
		kv.mu.Unlock()
		return
	}
	DPrintf("Learder %d in group %d requesting shard migration from other groups\n", kv.me, kv.gid)
	//var wait sync.WaitGroup
	for shard, configNum := range kv.comeInShards {
		// possibly in toOutShard as well, avoid two groups sending shards in both direction
		cfg := kv.smk.Query(configNum)
		gid := cfg.Shards[shard]
		if _, ok := kv.toOutShards[configNum]; ok && kv.gid < gid {
			continue
		}
		//wait.Add(1)
		func(shard int, config shardmaster.Config) {
			// in another thread without lock held
			// config corrospond to the configNum
			//defer wait.Done()
			// gid is the group that holds the shard in this config
			gid := config.Shards[shard]
			args := MigrateArgs{Shard: shard, ConfigNum: config.Num}
			for _, server := range config.Groups[gid] {
				srv := kv.make_end(server)
				reply := MigrateReply{}
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("Learder %d in group %d sending shard %d to all followers\n", kv.me, kv.gid, shard)
					kv.rf.Start(reply)
				}
				kv.mu.Lock()
			}
		}(shard, cfg)
	}
	kv.mu.Unlock()
	//wait.Wait()
}
func (kv *ShardKV) deamon(do func(), sleepMS int) {
	for !kv.killed() {
		do()
		time.Sleep(time.Millisecond * time.Duration(sleepMS))
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(MigrateReply{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.chMap = make(map[int]chan Op)
	kv.seqMap = make(map[int64]int)
	kv.shardStorage = make(map[string]string)
	kv.toOutShards = make(map[int]map[int]Shard)
	kv.comeInShards = make(map[int]int)
	kv.myShards = make(map[int]bool)
	// Use something like this to talk to the shardmaster:
	kv.smk = shardmaster.MakeClerk(kv.masters)

	// snapshot
	kv.commitIndex = -1
	snapshot := persister.ReadSnapshot()
	if snapshot != nil || len(snapshot) > 0 {
		kv.DecodeSnapshot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Listen()
	go kv.deamon(kv.checkConfig, 50)
	go kv.deamon(kv.SendShardMigration, 80)

	return kv
}
