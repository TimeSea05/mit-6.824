package kvraft

import (
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string // Get, Put or Append
	Key     string
	Value   string
	ClerkID int
	RPCID   int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cond *sync.Cond
	db   map[string]string

	// clerkID -> `RPCID` of lastly executed RPC call
	executed map[int][]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	idx := sort.Search(len(kv.executed[args.ClerkID]), func(i int) bool {
		return kv.executed[args.ClerkID][i] == args.SuccessRPCID
	})
	if idx != len(kv.executed[args.ClerkID]) {
		kv.executed[args.ClerkID] = kv.executed[args.ClerkID][idx+1:]
	}

	op := Op{
		Type:    "Get",
		Key:     args.Key,
		ClerkID: args.ClerkID,
		RPCID:   args.RPCID,
	}

	kv.rf.Start(op)
	kv.cond.Wait()

	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	reply.Value = kv.db[args.Key]
	reply.Err = OK
	raft.DebugLog(raft.DReplyGet, kv.me, "Reply Get %s: %s", args.Key, reply.Value)

	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	idx := sort.Search(len(kv.executed[args.ClerkID]), func(i int) bool {
		return kv.executed[args.ClerkID][i] == args.SuccessRPCID
	})
	if idx != len(kv.executed[args.ClerkID]) {
		kv.executed[args.ClerkID] = kv.executed[args.ClerkID][idx+1:]
	}

	op := Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkID,
		RPCID:   args.RPCID,
	}

	kv.rf.Start(op)
	kv.cond.Wait()

	_, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	reply.Err = OK
	raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reply %s: SUCCESS; db[%s]:%s", args.Op, args.Key, kv.db[args.Key])
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) readApplyCh() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}

		kv.mu.Lock()
		op := msg.Command.(Op)

		// search if this op has already been executed
		idx := sort.Search(len(kv.executed[op.ClerkID]), func(i int) bool {
			return kv.executed[op.ClerkID][i] == op.RPCID
		})

		// not executed
		if idx == len(kv.executed[op.ClerkID]) {
			switch op.Type {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
			raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "(CK:%d,RPC:%d,Op:%s,Key:%s,Value:%s): db[%s]==%s",
				op.ClerkID, op.RPCID, op.Type, op.Key, op.Value, op.Key, kv.db[op.Key])
			kv.executed[op.ClerkID] = append(kv.executed[op.ClerkID], op.RPCID)
		}
		kv.mu.Unlock()
		kv.cond.Signal()
	}
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.cond = sync.NewCond(&kv.mu)
	kv.db = make(map[string]string)
	kv.executed = make(map[int][]int)

	go kv.readApplyCh()

	return kv
}
