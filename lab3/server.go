package kvraft

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

type OpType int

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

// this is the operation that will be appended to the raft's log
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
	Term       int // raft's log term
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	seqMap    map[int64]int     // map a client to its sequenceId, to deal with duplicate
	KvStorage map[string]string // store the key/value from the client
	// probably a channel to get the message from the listener which listen on the raft's channel
	chMap map[int]chan Op
}

func (kv *KVServer) getChannel(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chMap[index]
	if !ok {
		// buffered channel, inorder to help in performance from asynchronous
		kv.chMap[index] = make(chan Op, 1)
		ch = kv.chMap[index]
	}
	return ch
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:     GET,
		SequenceId: args.SequenceId,
		ClientId:   args.ClientId,
		OpKey:      args.Key,
	}
	//DPrintf("Server's Get calls raft.Start()\n")
	op.Index, op.Term, isLeader = kv.rf.Start(op)

	// the server will listen to a channel correspond to this index for a reply from the raft to the listener, then itself
	// the client is allowed to receive data after itself's concurrent operation, subject to linearizablity
	ch := kv.getChannel(op.Index)
	// should we delete this entry in the chMap after get the data from this channel?
	// defer func(index int) {
	// 	kv.mu.Lock()
	// 	delete(kv.chMap, index)
	// 	kv.mu.Unlock()
	// }(op.Index)
	// it will wait for some time then give up
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	DPrintf("Get waiting for reply from listener")
	select {
	case newOp := <-ch:
		DPrintf("Get <- index channel")
		// able to reply to client
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			DPrintf("Server's raft switched leader during PutAppend\n")
			reply.Err = ErrWrongLeader
			return
		} else {
			kv.mu.Lock()
			Value, ok := kv.KvStorage[args.Key]
			kv.mu.Unlock()
			if ok {
				reply.Value = Value
				reply.Err = OK
			} else {
				reply.Err = ErrNoKey
			}
		}
	case <-timer.C:
		DPrintf("Server's Get timeout\n")
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		OpType:     args.Op,
		SequenceId: args.SequenceId,
		ClientId:   args.ClientId,
		OpKey:      args.Key,
		OpValue:    args.Value,
	}
	//DPrintf("Server's PutAppend calls raft.Start()\n")
	op.Index, op.Term, isLeader = kv.rf.Start(op)
	// the server will listen to a channel correspond to this index for a reply from the raft to the listener, then itself
	ch := kv.getChannel(op.Index)
	// should we delete this entry in the chMap after get the data from this channel?
	// defer func(index int) {
	// 	kv.mu.Lock()
	// 	delete(kv.chMap, index)
	// 	kv.mu.Unlock()
	// }(op.Index)
	// it will wait for some time then give up
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	DPrintf("Put/Append waiting for reply from listener")
	select {
	case newOp := <-ch:
		DPrintf("Put/Append <- index channel")
		// able to reply to client
		if newOp.ClientId != op.ClientId || newOp.SequenceId != op.SequenceId {
			// the log entry of the same index in the raft's log is overwritten by other leader's log
			DPrintf("Server's raft switched leader during PutAppend\n")
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
		}
	case <-timer.C:
		DPrintf("Server's PutAppend timeout\n")
		reply.Err = ErrWrongLeader
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.KvStorage = make(map[string]string)
	kv.chMap = make(map[int]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	// run a background thread that check for the chan of the commit message
	go kv.Listen()

	return kv
}

func (kv *KVServer) isDup(clientId int64, sequenceId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sid, ok := kv.seqMap[clientId]
	if !ok {
		return false
	}
	return sequenceId <= sid
}

func (kv *KVServer) Listen() {
	// listen for the chan, upon receiving something, apply that command to its state machine by calling Get()/PutAppend()
	for !kv.killed() {
		select {
		case cmd := <-kv.applyCh: // is blocking, so be fast but also serial
			DPrintf("Listener <- Raft channel\n")
			// send the processed data to the main waiting thread that just sent out it command to the raft and waiting
			index := cmd.CommandIndex
			op := cmd.Command.(Op)
			if !kv.isDup(op.ClientId, op.SequenceId) {
				// read or write to the kvserver's storage
				kv.mu.Lock()
				if op.OpType == GET {
					// the apply of the raft means we have a read quruom
				} else if op.OpType == PUT {
					kv.KvStorage[op.OpKey] = op.OpValue
				} else if op.OpType == APPEND {
					kv.KvStorage[op.OpKey] += op.OpValue
				}
				kv.seqMap[op.ClientId] = op.SequenceId
				kv.mu.Unlock()
			}
			kv.getChannel(index) <- op
			DPrintf("Listener -> index channel\n")
		}
	}
}
