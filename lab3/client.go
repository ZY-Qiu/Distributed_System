package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderId   int   // choose which server is the leader to send request to
	SequenceId int   // used to tag its request in monotonicly increasing order
	ClientId   int64 // used to identify itself to the server to check its sequenceId
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.ClientId = nrand()
	ck.LeaderId = int(ck.ClientId) % len(ck.servers)
	ck.SequenceId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	// it can be asynchronous call
	ck.SequenceId++
	args := GetArgs{
		Key:        key,
		SequenceId: ck.SequenceId,
		ClientId:   ck.ClientId,
	}
	// client may sit in a loop continuously retry to get a reply
	//DPrintf("Client calling Get\n")
	serverId := ck.LeaderId
	for {
		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.LeaderId = serverId
				value := reply.Value
				return value
			} else if reply.Err == ErrNoKey {
				//DPrintf("Client's Get error: NoKey\n")
				ck.LeaderId = serverId
				return ""
			} else if reply.Err == ErrWrongLeader {
				//DPrintf("Client's Get going to wrrong leader, retry\n")
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// if not ok, meaning RPC call failed, maybe leader crashed
		serverId = (serverId + 1) % len(ck.servers)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.SequenceId++
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		SequenceId: ck.SequenceId,
		ClientId:   ck.ClientId,
	}
	// client may sit in a loop continuously retry to get a reply
	//DPrintf("Client calling PutAppend\n")
	serverId := ck.LeaderId
	for {
		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.LeaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				//DPrintf("Client's PutAppend going to wrrong leader, retry\n")
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		// if not ok, meaning RPC call failed, maybe leader crashed
		serverId = (serverId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
