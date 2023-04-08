package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
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
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
	CommitTerm   int        // the term of leader's highest committed entry
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// works when `Success` is false
	// false -> args.Term < rf.currentterm;
	// true -> log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	InConsist bool

	// Optimization
	XTerm  int // the term of the conflicting entry
	XIndex int // the first index it stores for that term
}

type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk
}

// example code to send a RequestVote RPC to a peer.
// peer is the index of the target peer in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead peer, a live peer that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the peer side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

// func (rf *Raft) sendRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[peer].Call("Raft.RequestVote", args, reply)
// 	return ok
// }

/*
	Deal With Network Delays and Failures when Calling RPC

	When there is network latency, it may take a long time to wait for the return of an RPC call
	To avoid this, creates a goroutine to make an RPC call, and pass a channel to it
	(here the channel it wrapped in RPCThreadInfo struct)
	when RPC finished, the goroutine will first check if the time exceed `RPCTimeout`
	if not, put RPC reply into that channel, else exit
*/

const RPCTimeout = 400 * time.Millisecond

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

	switch args := info.args.(type) {
	case RequestVoteArgs:
		reply := info.reply.(RequestVoteReply)
		rf.peers[info.peer].Call(info.name, &args, &reply)
		info.reply = reply
	case AppendEntriesArgs:
		reply := info.reply.(AppendEntriesReply)
		rf.peers[info.peer].Call(info.name, &args, &reply)
		info.reply = reply
	case InstallSnapshotArgs:
		reply := info.reply.(int)
		rf.peers[info.peer].Call(info.name, &args, &reply)
		info.reply = reply
	}

	if time.Since(info.startTime) < RPCTimeout {
		replyCh <- info.reply
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
		case int:
			replyCh <- 0
		}
	}
}
