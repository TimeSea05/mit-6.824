package raft

import (
	"math/rand"
	"sync"
	"time"
)

// states of raft peers
const (
	FOLLOWER = iota
	LEADER
	CANDIDATE
)

// time constants, in milliseconds
const (
	HeartBeatInterval       = 200 * time.Millisecond
	ElectionTimeoutLeftEnd  = 500
	ElectionTimeoutInterval = 750
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DebugLog(dVote, rf.me, "CANDIDATE's TERM %d < PEER's TERM %d; No vote", args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DebugLog(dTermChange, rf.me, "SET TERM -> %d", rf.currentTerm)

		if rf.state != FOLLOWER {
			DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", rf.stateStr())
			rf.state = FOLLOWER
		}
		rf.persist()
	}

	// rf.vote.CandidateID == -1: this peer has not voted for any other candidate
	// rf.vote.Term < args.Term: a new term begins
	// and in the new term, this peer has not voted for any other candidate
	hasNotVoted := rf.vote.CandidateID == -1 || rf.vote.Term < args.Term

	// Raft determines which of two logs is more up-to-date by comparing the index
	// and term of the last entries in the logs
	// To grant vote, the candidate's log must be at least up-to-date as follower's log
	// if the logs have last entries with different terms, then the log with the later term
	// is more up-to-date
	// if the logs end with the same term, then whichever log is longer is more up-to-date
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	moreUpTodate := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= rf.lastIncludedIdx+len(rf.log))

	if hasNotVoted && moreUpTodate {
		rf.vote = Vote{
			CandidateID: args.CandidateID,
			Term:        args.Term,
		}
		reply.VoteGranted = true
		rf.tickerStartTime = time.Now()
		rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))

		DebugLog(dVote, rf.me, "Vote -> PEER %d; TERM: %d", args.CandidateID, args.Term)
		rf.persist()
	} else if !hasNotVoted {
		DebugLog(dVote, rf.me, "Has voted -> PEER %d; No vote", rf.vote.CandidateID)
	} else if !moreUpTodate {
		DebugLog(dVote, rf.me, "My log Newer than PEER %d's", args.CandidateID)
	}
}

// make an RPC call in case of network latency
// create a channel for threads to send and receive RPC reply
// 1. use a wrapper to wrap you rpc call(single thread)
// 2. start a ticker thread
// if the rpc call in the wrapper failed to get reply from remote and timed out(RPCTimeout)
// the ticker thread will send an empty reply to `replyCh`
// if the rpc call finished successfully, the main thread(who starts ticker and wrapper) will
// send a value to `rpcFinished` to tell the ticker do not send a empty value to `replyCh`
func (rf *Raft) issueRequestVoteRPC(peer int, wg *sync.WaitGroup, votes *int) {
	rf.mu.Lock()
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.lastIncludedIdx + len(rf.log),
		LastLogTerm:  lastLogTerm,
	}
	rf.mu.Unlock()

	var reply RequestVoteReply
	replyCh := make(chan interface{}, 1)
	rpcInfo := RPCInfo{
		Peer:  peer,
		Name:  "Raft.RequestVote",
		Args:  args,
		Reply: reply,
	}

	rpcFinished := make(chan bool, 1)
	go rf.RPCWrapper(rpcInfo, replyCh)
	go rf.RPCTimeoutHandler(replyCh, rpcInfo, rpcFinished)
	voteReply := (<-replyCh).(RequestVoteReply)
	rpcFinished <- true

	rf.mu.Lock()
	if voteReply.VoteGranted {
		*votes++
	}

	if rf.currentTerm < voteReply.Term {
		rf.currentTerm = voteReply.Term
		DebugLog(dTermChange, rf.me, "TERM -> %d", rf.currentTerm)
		rf.persist()

		if rf.state != FOLLOWER {
			DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", rf.stateStr())
			rf.state = FOLLOWER
		}
	}
	wg.Done()
	rf.mu.Unlock()
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// time.Sleep().
		// be started and to randomize sleeping time using rand
		rf.mu.Lock()
		sleepTime := rf.electionTimeout - time.Since(rf.tickerStartTime)
		rf.mu.Unlock()
		if sleepTime > 0 {
			time.Sleep(sleepTime)
		}

		// if this peer does not receive heart beat from leader
		// which means rf.tickerStartTime and rf.electionTimout are not updated
		// then timeout will occur and start leader election
		rf.mu.Lock()
		if rf.state != LEADER && time.Since(rf.tickerStartTime) > rf.electionTimeout {
			// to begin an election, a follower increments its current term
			// and transitions to candidate state
			rf.currentTerm++
			DebugLog(dElection, rf.me, "Election by PEER %d, TERM %d", rf.me, rf.currentTerm)
			rf.persist()

			// if this peer does not vote for anyone in this term
			// the peer will first vote for itself
			votedInThisTerm := rf.vote.Term == rf.currentTerm && rf.vote.CandidateID != -1
			if !votedInThisTerm {
				rf.state = CANDIDATE
				DebugLog(dStateChange, rf.me, "FOLLOWER -> CANDIDATE")

				votes := 1
				DebugLog(dVote, rf.me, "Vote -> PEER %d(SELF)", rf.me)
				var wg sync.WaitGroup

				// then issues RequestVoteRPC in parallel
				for peer := 0; peer < len(rf.peers); peer++ {
					if peer == rf.me {
						rf.vote = Vote{
							CandidateID: rf.me,
							Term:        rf.currentTerm,
						}
						rf.persist()
						continue
					}

					wg.Add(1)
					go rf.issueRequestVoteRPC(peer, &wg, &votes)
				}
				rf.mu.Unlock()
				wg.Wait()

				// check if this raft peer wins the election
				// candidate who win the election will become leader, else they become follower
				// RPCs may delay, so before make this peer a new leader
				// first check if a leader has already been elected by checking if `rf.tickerTimeStart` and `rf.electionTimeout` has been updated
				rf.mu.Lock()
				if votes > len(rf.peers)/2 && rf.state == CANDIDATE {
					rf.state = LEADER
					rf.leaderId = rf.me
					DebugLog(dStateChange, rf.me, "CANDIDATE -> LEADER")
					go rf.sendHeartBeats()
				} else {
					rf.state = FOLLOWER
					DebugLog(dStateChange, rf.me, "CANDIDATE -> FOLLOWER")
				}
			} else {
				DebugLog(dElection, rf.me, "Has Voted -> %d; Election STOP", rf.vote.CandidateID)
			}
		}
		rf.tickerStartTime = time.Now()
		rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))
		rf.mu.Unlock()
	}
}

func (rf *Raft) issueHeartBeatRPC(peer int) {
	rf.mu.Lock()
	replyCh := make(chan interface{}, 1)

	var commitTerm int
	if rf.commitIndex == rf.lastIncludedIdx {
		commitTerm = rf.lastIncludedTerm
	} else {
		commitTerm = rf.log[rf.commitIndex-(rf.lastIncludedIdx+1)].Term
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		LeaderCommit: rf.commitIndex,
		CommitTerm:   commitTerm,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	rpcInfo := RPCInfo{
		Peer:  peer,
		Name:  "Raft.AppendEntries",
		Args:  args,
		Reply: reply,
	}

	rpcFinished := make(chan bool, 1)
	go rf.RPCWrapper(rpcInfo, replyCh)
	go rf.RPCTimeoutHandler(replyCh, rpcInfo, rpcFinished)
	hbeatReply := (<-replyCh).(AppendEntriesReply)
	rpcFinished <- true

	if !hbeatReply.Success && hbeatReply.Term != 0 {
		rf.mu.Lock()
		rf.currentTerm = hbeatReply.Term
		DebugLog(dTermChange, rf.me, "TERM -> %d", rf.currentTerm)
		rf.persist()

		DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", rf.stateStr())
		rf.state = FOLLOWER
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendHeartBeats() {
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}

		go func(peer int) {
			for {
				startTime := time.Now()
				rf.mu.Lock()
				// before sending heart beats, check if current PEER is an *living* *LEADER*
				if rf.killed() || rf.state != LEADER {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				DebugLog(dHeartBeart, rf.me, "HEART BEAT -> PEER %d", peer)
				rf.issueHeartBeatRPC(peer)

				sleepTime := HeartBeatInterval - time.Since(startTime)
				if sleepTime > 0 {
					time.Sleep(sleepTime)
				}
			}
		}(peer)
	}
}
