package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
	"6.824/raft"
)

const RETRY_INTERVAL = 200 * time.Millisecond

type Clerk struct {
	servers  []*labrpc.ClientEnd
	nServers int

	clerkID   int // a cluster consists of n kv servers and n clerks
	curLeader int // current leader of kv servers
	RPCID     int // ID of RPC call(to prevent duplicate)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var nClerk int
var nClerkMu sync.Mutex

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.nServers = len(servers)
	ck.RPCID = 1

	nClerkMu.Lock()
	ck.clerkID = nClerk
	nClerk++
	nClerkMu.Unlock()

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
		Key:     key,
		ClerkID: ck.clerkID,
		RPCID:   ck.RPCID,
	}

	var val string
	for {
		reply := ck.issueGetRPC(args)
		if reply.Err == OK {
			raft.DebugLog(raft.DCallGet, ck.nServers, "CK %d: Get SUCCESS: {%s: %s}", ck.clerkID, key, reply.Value)
			val = reply.Value
			break
		} else if reply.Err == ErrNoKey {
			raft.DebugLog(raft.DCallGet, ck.nServers, "CK %d: GET FAIL: No Such Key(%s) in DB", ck.clerkID, key)
			val = reply.Value
			break
		}

		raft.DebugLog(raft.DCallGet, ck.nServers, "CK %d: Get FAIL: %s", ck.clerkID, reply.Err)
		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)
		time.Sleep(RETRY_INTERVAL)
	}

	ck.RPCID++
	return val
}

func (ck *Clerk) issueGetRPC(args GetArgs) GetReply {
	raft.DebugLog(raft.DCallGet, ck.nServers, "CK %d: Get %s; ID:%d", ck.clerkID, args.Key, args.RPCID)
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
		Key:     key,
		Value:   value,
		Op:      op,
		RPCID:   ck.RPCID,
		ClerkID: ck.clerkID,
	}

	for {
		reply := ck.issuePutAppendRPC(args)
		if reply.Err == OK {
			raft.DebugLog(raft.DCallPutOrAppend, ck.nServers, "CK %d: %s {%s:%s} SUCCESS", ck.clerkID, op, key, value)
			break
		}

		raft.DebugLog(raft.DCallPutOrAppend, ck.nServers, "CK %d: %s {%s:%s} FAIL: %s", ck.clerkID, op, key, value, reply.Err)
		ck.curLeader = (ck.curLeader + 1) % len(ck.servers)

		time.Sleep(RETRY_INTERVAL)
	}

	ck.RPCID++
}

func (ck *Clerk) issuePutAppendRPC(args PutAppendArgs) PutAppendReply {
	raft.DebugLog(raft.DCallPutOrAppend, ck.nServers, "CK %d: %s {%s:%s}; ID: %d",
		ck.clerkID, args.Op, args.Key, args.Value, args.RPCID)

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
