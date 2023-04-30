package kvraft

import (
	"time"

	"6.824/raft"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrRPCTimeout  = "ErrTimeout"
	ErrRPCFail     = "ErrRPCFail"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	RPCID   int
	ClerkID int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID      int
	RPCID        int
	SuccessRPCID int
}

type GetReply struct {
	Err   Err
	Value string
}

func (ck *Clerk) RPCWrapper(info raft.RPCInfo, replyCh chan interface{}) {
	info.StartTime = time.Now()

	switch args := info.Args.(type) {
	case GetArgs:
		reply := info.Reply.(GetReply)
		ok := ck.servers[info.Peer].Call(info.Name, &args, &reply)
		if ok {
			info.Reply = reply
		} else {
			info.Reply = GetReply{Err: ErrRPCFail}
		}
	case PutAppendArgs:
		reply := info.Reply.(PutAppendReply)
		ok := ck.servers[info.Peer].Call(info.Name, &args, &reply)

		if ok {
			info.Reply = reply
		} else {
			info.Reply = PutAppendReply{Err: ErrRPCFail}
		}
	}

	if time.Since(info.StartTime) < raft.RPCTimeout {
		replyCh <- info.Reply
	}
}

func (ck *Clerk) RPCTimeoutHandler(replyCh chan interface{}, info raft.RPCInfo, rpcFinished chan bool) {
	time.Sleep(raft.RPCTimeout)
	if len(replyCh) == 0 && len(rpcFinished) == 0 {
		switch info.Reply.(type) {
		case GetReply:
			replyCh <- GetReply{Err: ErrRPCTimeout}
		case PutAppendReply:
			replyCh <- PutAppendReply{Err: ErrRPCTimeout}
		}
	}
}
