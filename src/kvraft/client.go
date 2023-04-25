package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

const RETRY_INTERVAL = 200 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd

	// a cluster consists of kv servers and a clerk
	// for example, there are 3 kv servers(0,1,2), then clerkID is 3
	clerkID   int
	curLeader int
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

	ck.clerkID = len(ck.servers)
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := GetArgs{
		Key: key,
	}

	for {
		reply := ck.issueGetRPC(args)
		if reply.Err == OK {
			raft.DebugLog(raft.DCallGet, ck.clerkID, "Get SUCCESS: {%s: %s}", key, reply.Value)
			return reply.Value
		} else if reply.Err == ErrNoKey {
			raft.DebugLog(raft.DCallGet, ck.clerkID, "GET FAIL: No Such Key(%s) in DB", key)
			return reply.Value
		}

		raft.DebugLog(raft.DCallGet, ck.clerkID, "Get FAIL: %s", reply.Err)
		if reply.Err == ErrWrongLeader {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}
		time.Sleep(RETRY_INTERVAL)
	}
}

func (ck *Clerk) issueGetRPC(args GetArgs) GetReply {
	raft.DebugLog(raft.DCallGet, ck.clerkID, "Get %s", args.Key)
	rpcInfo := raft.RPCInfo{
		Peer:  ck.curLeader,
		Name:  "KVServer.Get",
		Args:  args,
		Reply: GetReply{},
	}

	replyCh := make(chan interface{}, 1)
	rpcFinished := make(chan bool, 1)

	go ck.RPCWrapper(rpcInfo, replyCh)
	go ck.RPCTimeoutHandler(replyCh, rpcInfo, rpcFinished)

	getReply := (<-replyCh).(GetReply)
	rpcFinished <- true

	return getReply
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}

	for {
		reply := ck.issuePutAppendRPC(args)
		if reply.Err == OK {
			raft.DebugLog(raft.DCallPutOrAppend, ck.clerkID, "%s {%s:%s} SUCCESS", op, key, value)
			break
		}

		raft.DebugLog(raft.DCallPutOrAppend, ck.clerkID, "%s {%s:%s} FAIL: %s", op, key, value, reply.Err)
		if reply.Err == ErrWrongLeader {
			ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		}

		time.Sleep(RETRY_INTERVAL)
	}
}

func (ck *Clerk) issuePutAppendRPC(args PutAppendArgs) PutAppendReply {
	raft.DebugLog(raft.DCallPutOrAppend, ck.clerkID, "%s {%s:%s}", args.Op, args.Key, args.Value)
	rpcInfo := raft.RPCInfo{
		Peer:  ck.curLeader,
		Name:  "KVServer.PutAppend",
		Args:  args,
		Reply: PutAppendReply{},
	}

	replyCh := make(chan interface{}, 1)
	rpcFinished := make(chan bool, 1)

	go ck.RPCWrapper(rpcInfo, replyCh)
	go ck.RPCTimeoutHandler(replyCh, rpcInfo, rpcFinished)

	putAppendReply := (<-replyCh).(PutAppendReply)
	rpcFinished <- true

	return putAppendReply
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
