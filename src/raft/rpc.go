package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // candidate's term
	CandidateID int // candidate requesting vote
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

/*
	Deal With Network Delays and Failures when Calling RPC

	When there is network latency, it may take a long time to wait for the return of an RPC call
	To avoid this, creates a goroutine to make an RPC call, and pass a channel to it
	(here the channel it wrapped in RPCThreadInfo struct)
	when RPC finished, the goroutine will first check if the time exceed `RPCTimeout`
	if not, put RPC reply into that channel, else exit
*/

const RPCTimeout = 250 * time.Millisecond

// All you need to make an RPC call
// type of args and reply is `interface{}`
// to support all kinds of RPCArgs and RPCReply
type RPCThreadInfo struct {
	name      string
	peer      int       // which raft peer to send RPC to
	startTime time.Time // time the RPC is called

	args  interface{}
	reply interface{}
}

// This wrapper is used to deal with RPC network latency issues
// Every time you need to make an RPC call, put all info you need into a RPCThreadInfo struct,
// start a new `RPCTimeoutWapper` goroutine, pass that struct to the wrapper.
// The wrapper will make an RPC call, wait for its return
// When it returns, it will check if timeout occurs
// if not, put RPC reply into channel `replyCh`, else exit
func (rf *Raft) RPCTimeoutWrapper(info RPCThreadInfo, replyCh chan interface{}) {
	info.startTime = time.Now()

	// DebugLog(dRPC, rf.me, "%s RPC Thread(id: %d) call PEER %d", info.name, info.id, info.peer)
	switch args := info.args.(type) {
	case RequestVoteArgs:
		reply := info.reply.(RequestVoteReply)
		rf.peers[info.peer].Call(info.name, &args, &reply)
		info.reply = reply
	case AppendEntriesArgs:
		reply := info.reply.(AppendEntriesReply)
		rf.peers[info.peer].Call(info.name, &args, &reply)
		info.reply = reply
	}

	if time.Since(info.startTime) < RPCTimeout {
		replyCh <- info.reply
		// 	DebugLog(dRPC, rf.me, "%s RPC Thread(id: %d) call success", info.name, info.id)
		// } else {
		// 	DebugLog(dRPC, rf.me, "Timeout %s RPC thread(id: %d) exited", info.name, info.id)
	}
}

// Every time you make an RPC call, you need to start a `RPCTimeoutTicker` thread
// if the RPC call you make time out, this thread will send an empty reply to `replyCh`
// which means the return value of RPC call made before is ignored
// If RPC call finished successfully, main thread(who start this ticker thread) will
// send a value to channel `rpcFinished` to tell the thread not to send a empty reply to `replyCh`
func (rf *Raft) RPCTimeoutTicker(replyCh chan interface{}, info RPCThreadInfo, rpcFinished chan bool) {
	time.Sleep(RPCTimeout)
	if len(replyCh) == 0 && len(rpcFinished) == 0 {
		switch info.reply.(type) {
		case RequestVoteReply:
			replyCh <- RequestVoteReply{}
		case AppendEntriesReply:
			replyCh <- AppendEntriesReply{}
		}
	}
}
