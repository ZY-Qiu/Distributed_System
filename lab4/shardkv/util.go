package shardkv

import (
	"bytes"
	"log"

	"../labgob"
	"../raft"
	"../shardmaster"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) DecodeSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	// snapshot stores the kvStorage, seqMap
	var kvStorage map[string]string
	var seqMap map[int64]int
	var maxraftstate int
	var comeInShards map[int]int
	var toOutShards map[int]map[int]Shard
	var myShards map[int]bool
	var cfg shardmaster.Config
	if d.Decode(&kvStorage) != nil ||
		d.Decode(&seqMap) != nil ||
		d.Decode(&maxraftstate) != nil ||
		d.Decode(&comeInShards) != nil ||
		d.Decode(&toOutShards) != nil ||
		d.Decode(&myShards) != nil ||
		d.Decode(&cfg) != nil {
		DPrintf("KVServer %d read broken persistence after reboot\n", kv.me)
		return
	} else {
		kv.shardStorage = kvStorage
		kv.seqMap = seqMap
		kv.maxraftstate = maxraftstate
		kv.comeInShards = comeInShards
		kv.toOutShards = toOutShards
		kv.myShards = myShards
		kv.Config = cfg
	}
}
func (kv *ShardKV) MakeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.Lock()
	e.Encode(kv.shardStorage)
	e.Encode(kv.seqMap)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.comeInShards)
	e.Encode(kv.toOutShards)
	e.Encode(kv.myShards)
	e.Encode(kv.Config)
	data := w.Bytes()
	kv.mu.Unlock()
	return data
}
func (kv *ShardKV) isDup(clientId int64, sequenceId int) bool {
	sid, ok := kv.seqMap[clientId]
	if !ok {
		return false
	}
	return sequenceId <= sid
}
func (kv *ShardKV) getChannel(index int, create bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.chMap[index]
	if !ok {
		if !create {
			return nil
		}
		// buffered channel, inorder to help in performance from asynchronous
		kv.chMap[index] = make(chan Op, 1)
		ch = kv.chMap[index]
	}
	return ch
}
func (kv *ShardKV) matchShard(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.Config.Shards[key2shard(key)] == kv.gid
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	// after installing snapshot, command first time seen, but outdated
	index := applyMsg.CommandIndex
	// kv.mu.Lock()
	// if index <= kv.commitIndex {
	// 	// it may be an error? terminate?
	// 	kv.mu.Unlock()
	// 	continue
	// }
	// kv.mu.Unlock()
	if config, ok := applyMsg.Command.(shardmaster.Config); ok {
		kv.updateConfig(config)
	} else if migrateReply, ok := applyMsg.Command.(MigrateReply); ok {
		kv.updateShard(migrateReply)
	} else {
		op := applyMsg.Command.(Op)
		kv.normal(&op)
		kv.commitIndex = index
		if ch := kv.getChannel(index, false); ch != nil {
			DPrintf("Group %d listener %d -> index channel %d\n", kv.gid, kv.me, index)
			select {
			case <-ch:
			default:
			}
			ch <- op
		}
	}
	// may need to snapshot if log size too large
	if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
		snapshot := kv.MakeSnapshot()
		kv.rf.Snapshot(index, snapshot)
	}
}
func (kv *ShardKV) normal(op *Op) {
	kv.mu.Lock()
	if _, ok := kv.myShards[key2shard(op.OpKey)]; !ok {
		// should we wait for the shard to be migrated?
		_, ok = kv.comeInShards[key2shard(op.OpKey)]
		if ok {
			DPrintf("Shard %d absent to be migrated\n", key2shard(op.OpKey))
		} else {
			DPrintf("Shard %d should be in the wrrong group\n", key2shard(op.OpKey))
		}
		op.Err = ErrWrongGroup
	} else {
		if !kv.isDup(op.ClientId, op.SequenceId) {
			// read or write to the kvserver's storage
			if op.OpType == PUT {
				kv.shardStorage[op.OpKey] = op.OpValue
			} else if op.OpType == APPEND {
				kv.shardStorage[op.OpKey] += op.OpValue
			}
			kv.seqMap[op.ClientId] = op.SequenceId
		}
		if op.OpType == GET {
			// the apply of the raft means we have a read quruom
			op.OpValue, ok = kv.shardStorage[op.OpKey]
			// if !ok {
			// 	op.Err = ErrNoKey
			// }
		}
	}
	kv.mu.Unlock()
}
func (kv *ShardKV) deepCopy(configNum int, shard int) (Shard, map[int64]int) {
	data := make(Shard)
	seq := make(map[int64]int)
	for k, v := range kv.toOutShards[configNum][shard] {
		data[k] = v
	}
	for k, v := range kv.seqMap {
		seq[k] = v
	}
	return data, seq
}
