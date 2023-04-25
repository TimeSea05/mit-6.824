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
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
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
		ck.servers[info.Peer].Call(info.Name, &args, &reply)
		info.Reply = reply
	case PutAppendArgs:
		reply := info.Reply.(PutAppendReply)
		ck.servers[info.Peer].Call(info.Name, &args, &reply)
		info.Reply = reply
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
