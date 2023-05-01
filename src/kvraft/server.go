package kvraft

import (
	"bytes"
	"fmt"
	"log"
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

type ClientRequest struct {
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
	term        int // current term of raft peer
	commitIndex int // committed log index of raft peer
	cond        *sync.Cond
	db          map[string]string

	// clerkID -> `RPCID` of lastly executed RPC call
	lastExecuted map[int]int

	// waiting RPC calls
	waitingReqs []ClientRequest
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	term, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	// If a change in the term is detected, it means that this server has become the new leader.
	// Regardless of whether this server has been a leader before, it should broadcast
	// on the conditional variable to clear the threads that were sleeping on the conditional variable before
	// (even if these threads have already timed out).
	// At the same time, slice `kv.waitingReqs` should also be cleared
	if kv.term < term {
		kv.term = term
		kv.cond.Broadcast()
		kv.waitingReqs = nil
	}

	op := Op{
		Type:    "Get",
		Key:     args.Key,
		ClerkID: args.ClerkID,
		RPCID:   args.RPCID,
	}

	kv.waitingReqs = append(kv.waitingReqs, ClientRequest{ClerkID: args.ClerkID, RPCID: args.RPCID})
	// logging about RPC calls waiting on this key-value server
	waitingReqsStr := "waitingReqs:["
	for _, r := range kv.waitingReqs {
		waitingReqsStr += fmt.Sprintf("%d:%d;", r.ClerkID, r.RPCID)
	}
	raft.DebugLog(raft.DReplyGet, kv.me, "%s]", waitingReqsStr)

	kv.rf.Start(op)
	kv.cond.Wait()

	term, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	if kv.term < term {
		kv.term = term
		kv.cond.Broadcast()
		kv.waitingReqs = nil
	}

	reply.Value = kv.db[args.Key]
	reply.Err = OK
	raft.DebugLog(raft.DReplyGet, kv.me, "Reply Get %s: %s", args.Key, reply.Value)

	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	if kv.term < term {
		kv.term = term
		kv.cond.Broadcast()
		kv.waitingReqs = nil
	}

	op := Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		ClerkID: args.ClerkID,
		RPCID:   args.RPCID,
	}

	kv.waitingReqs = append(kv.waitingReqs, ClientRequest{ClerkID: args.ClerkID, RPCID: args.RPCID})
	// logging about RPC calls waiting on this key-value server
	waitingReqsStr := "waitingReqs:["
	for _, r := range kv.waitingReqs {
		waitingReqsStr += fmt.Sprintf("%d:%d;", r.ClerkID, r.RPCID)
	}
	raft.DebugLog(raft.DReplyGet, kv.me, "%s]", waitingReqsStr)

	kv.rf.Start(op)
	kv.cond.Wait()

	term, isLeader = kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reject: %s", reply.Err)
		kv.mu.Unlock()
		return
	}

	if kv.term < term {
		kv.term = term
		kv.cond.Broadcast()
		kv.waitingReqs = nil
	}

	reply.Err = OK
	raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "Reply %s{%s:%s}: SUCCESS; db[%s]:%s",
		args.Op, args.Key, args.Value, args.Key, kv.db[args.Key])
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

		if msg.SnapshotValid { // install a snapshot
			kv.ingestSnapshot(msg.Snapshot)
		} else if msg.CommandValid { // commit a log entry
			op := msg.Command.(Op)
			kv.commitIndex = msg.CommandIndex

			// check if this op has already been executed
			if kv.lastExecuted[op.ClerkID] < op.RPCID {
				switch op.Type {
				case "Put":
					kv.db[op.Key] = op.Value
				case "Append":
					kv.db[op.Key] += op.Value
				}
				raft.DebugLog(raft.DReplyPutOrAppend, kv.me, "(CK:%d,RPC:%d,Op:%s,Key:%s,Value:%s): db[%s]==%s",
					op.ClerkID, op.RPCID, op.Type, op.Key, op.Value, op.Key, kv.db[op.Key])
				raft.DebugLog(raft.DReplyGet, kv.me, "lastExecuted[%d]: %d -> %d", op.ClerkID,
					kv.lastExecuted[op.ClerkID], op.RPCID)
				kv.lastExecuted[op.ClerkID] = op.RPCID
			}

			if kv.maxraftstate != -1 && kv.rf.Persister.RaftStateSize() > kv.maxraftstate {
				raft.DebugLog(raft.DSnapshot, kv.me, "Take Snapshot, Index:%d", kv.commitIndex)
				kv.rf.Snapshot(kv.commitIndex, kv.takeSnapshot())
			}

			// if this kv server is not leader, then it should not wake up
			// threads waiting on its conditional variable
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.mu.Unlock()
				continue
			}

			// Only when there are threads waiting on `kv.cond`: len(kv.waitingReqs > 0)
			// and `ClerkID` && `RPCID` of the log entry commited just now is the same as
			// the first thread waiting on `kv.cond`'s queue,
			// can we use `kv.cond.Signal` to wake up the first thread waiting on `kv.cond`
			if len(kv.waitingReqs) > 0 && kv.waitingReqs[0].ClerkID == op.ClerkID && kv.waitingReqs[0].RPCID == op.RPCID {
				kv.cond.Signal()
				kv.waitingReqs = kv.waitingReqs[1:]
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) takeSnapshot() []byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)

	encoder.Encode(kv.term)
	encoder.Encode(kv.db)
	encoder.Encode(kv.lastExecuted)

	return buf.Bytes()
}

// make sure that you hold the lock `kv.mu` when you call this function
// or data race will be detected
func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	buf := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buf)

	var term int
	db := make(map[string]string)
	lastExecuted := make(map[int]int)

	if err := decoder.Decode(&term); err != nil {
		log.Fatalf("Decode field `term` failed: %v", err)
	}
	if err := decoder.Decode(&db); err != nil {
		log.Fatalf("Decode field `db` failed: %v", err)
	}
	if err := decoder.Decode(&lastExecuted); err != nil {
		log.Fatalf("Decode field `lastExecuted` failed: %v", err)
	}

	kv.term = term
	kv.db = db
	kv.lastExecuted = lastExecuted
	raft.DebugLog(raft.DSnapshot, kv.me, "LOAD from snapshot; kv.term:%d", kv.term)
	raft.DebugLog(raft.DSnapshot, kv.me, "kv.db: %v", kv.db)
	raft.DebugLog(raft.DSnapshot, kv.me, "kv.lastExecuted:%v", kv.lastExecuted)
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

	kv.lastExecuted = make(map[int]int)
	kv.waitingReqs = make([]ClientRequest, 0)

	snapshot := kv.rf.Persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ingestSnapshot(snapshot)
	}

	go kv.readApplyCh()

	return kv
}
