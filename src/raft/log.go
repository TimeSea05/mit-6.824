package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func (rf *Raft) handleHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}
	DebugLog(dHeartBeart, rf.me, "Recv HEART BEAT <- %d", args.LeaderID)

	// if current state of this peer is LEADER or CANDIDATE receives heartbeat from another peer
	// this peer should become follower
	if rf.state == LEADER || rf.state == CANDIDATE {
		curState := "LEADER"
		if rf.state == CANDIDATE {
			curState = "CANDIDATE"
		}
		DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", curState)
		rf.state = FOLLOWER
	}

	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderID

	// every time a raft peer receives heart beat from leader
	// it should reset `tickerStartTime` and `electionTimeout`
	rf.tickerStartTime = time.Now()
	rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))

	/********** Commit Entries Accoring to Leader's `commitIndex` **********/
	// leaderCommit > commitIndex, set commmitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
		DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
	}

	// if commitIndex > lastApplied; increment lastApplied
	// apply log[lastApplied] to state machine
	for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx].Command,
			CommandIndex: idx,
		}
		rf.applyCh <- applyMsg
		DebugLog(dCommit, rf.me, "APPLY Entry: I: %d, T: %d", idx, rf.log[idx].Term)
		rf.lastApplied++
	}
	/***********************************************************************/

	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 { // HeartBeat
		rf.handleHeartBeat(args, reply)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Every time a raft peer receives `AppendEntriesRPC` from leader
	// it should also reset timer
	rf.tickerStartTime = time.Now()
	rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))

	// Logging about entries received from leader
	newEntriesStart := args.PrevLogIndex + 1
	newEntriesString := "Recv New Entry: "
	for idx, entry := range args.Entries {
		newEntriesString += fmt.Sprintf("I: %d, T: %d; ", newEntriesStart+idx, entry.Term)
	}
	DebugLog(dAppend, rf.me, "%s", newEntriesString)

	// Process log entries from leader
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		DebugLog(dAppend, rf.me, "REJECT: Leader's TERM %d < PEER %d's TERM %d",
			args.Term, rf.me, rf.currentTerm)
		return
	}

	// 2. reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		*reply = AppendEntriesReply{
			Term:      rf.currentTerm,
			Success:   false,
			InConsist: true,
		}
		DebugLog(dAppend, rf.me, "REJECT: Log doesn't Match PrevLog")
		return
	}

	// 3. if an existing entry conflicts with a new one(same index but different terms)
	// delete the existing entry and all that following it
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		rf.log = rf.log[0:args.PrevLogIndex]
		DebugLog(dAppend, rf.me, "DISCARD All Entries after %d", args.PrevLogIndex)
	}

	// 4. append any new entries not already in the log
	appendEntriesStart := len(rf.log) - args.PrevLogIndex - 1
	if appendEntriesStart < len(args.Entries) {
		originLogLen := len(rf.log)
		rf.log = append(rf.log, args.Entries[appendEntriesStart:]...)

		entriesStr := "ACCEPT New Entry: "
		for idx, entry := range args.Entries[appendEntriesStart:] {
			entriesStr += fmt.Sprintf("I: %d, T: %d; ", originLogLen+appendEntriesStart+idx, entry.Term)
		}
		DebugLog(dAppend, rf.me, "%s", entriesStr)
	}

	// 5. if leaderCommit > commitIndex, set commmitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
		DebugLog(dCommit, rf.me, "SET CommitIndex -> %d", rf.commitIndex)
	}

	// if commitIndex > lastApplied; increment lastApplied
	// apply log[lastApplied] to state machine
	for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx].Command,
			CommandIndex: idx,
		}
		rf.applyCh <- applyMsg
		DebugLog(dCommit, rf.me, "APPLY Entry: I: %d, T: %d", idx, rf.log[idx].Term)
		rf.lastApplied++
	}

	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}
}

// `rf.Start` uses this function as a single thread to reach
// agreement between leader and followers
func (rf *Raft) startAgreement(index int) {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	// `replicas` represents how many servers have replicated the log entry successfully
	// initialized to 1 because the leader already appended this entry to its log
	replicas := 1

	// `safelyReplicated` represents if the log entry has been safely replicated
	// which means this log entry has been replicated on a majority of servers
	safelyReplicated := false

	mu.Lock()
	// issue `AppendEntries` RPCs in parallel to
	// each of the other servers to replicate the entry
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.reachAgreementPeer(peer, index, &mu, cond, &replicas, &safelyReplicated)
	}

	// wait for the log entry to be safely replicated
	cond.Wait()
	safelyReplicated = true

	rf.mu.Lock()

	if rf.state == LEADER {
		// update `commitIndex`
		if index > rf.commitIndex {
			rf.commitIndex = index
			DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
		}

		rf.lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: rf.lastApplied,
			Command:      rf.log[rf.lastApplied].Command,
		}
		DebugLog(dCommit, rf.me, "COMMIT Entry; I: %d, T: %d", rf.lastApplied, rf.log[index].Term)
		rf.applyCh <- applyMsg
	}
	rf.mu.Unlock()

	// wait for the log entry to be replicated on all followers
	cond.Wait()
	mu.Unlock()
}

func (rf *Raft) reachAgreementPeer(peer int, index int, mu *sync.Mutex, cond *sync.Cond, replicas *int, safelyReplicated *bool) {
	for {
		// if this peer is killed, then stop reaching agreement with peer
		if rf.killed() {
			return
		}

		// if a follower disconnects from network, every time the leader received a command
		// the leader will start a thread to reach agreement with the disconnected follower
		// if one thread reach agreement with the rejoined follower successfully, then it will set
		// rf.nextIndex[peer] to len(rf.log)
		// at this time, all the other threads don't need to loop again and again because agreement has
		// been reached
		// they just need to log about that infomation and exit loop
		rf.mu.Lock()
		if rf.nextIndex[peer] >= len(rf.log) || rf.state != LEADER {
			if rf.nextIndex[peer] >= len(rf.log) {
				DebugLog(dAgree, rf.me, "REACH Agreement - PEER %d", peer)
			}
			rf.mu.Unlock()

			mu.Lock()
			*replicas++
			mu.Unlock()
			break
		}

		// log about entries leader gonna send to the raft peer
		sendEntriesStart := rf.nextIndex[peer]
		sendEntriesStr := fmt.Sprintf("SEND Entry -> PEER %d; ", peer)
		for idx, entry := range rf.log[sendEntriesStart:] {
			sendEntriesStr += fmt.Sprintf("I: %d, T: %d; ", sendEntriesStart+idx, entry.Term)
		}
		DebugLog(dSendEntry, rf.me, "%s", sendEntriesStr)
		rf.mu.Unlock()

		// Issue AppendEntries RPC to the raft peer
		// Every time you issue an RPC, you need to release the lock
		// in case the RPC timeout
		reply := rf.issueAppendEntriesRPC(peer)

		// Every time the leader receive a new entry from client, it will start a
		// `startAgreement` thread, and `index` is just the index of that new entry
		if reply.Success {
			rf.mu.Lock()
			if rf.nextIndex[peer] <= index {
				rf.nextIndex[peer] = index + 1
				DebugLog(dSendEntry, rf.me, "SEND Entry -> PEER %d SUCCESS; INC nextIndex[%d] -> %d",
					peer, peer, rf.nextIndex[peer])
			}
			rf.mu.Unlock()

			mu.Lock()
			DebugLog(dAgree, rf.me, "REACH Agreement - PEER %d", peer)
			*replicas++
			mu.Unlock()
			break
		}

		// AppendEntries RPC did not time out and failed
		// which means AppendEntries consistency check failed
		// the leader should decrement nextIndex and retry
		if reply.Term != 0 {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				DebugLog(dTermChange, rf.me, "TERM -> %d", rf.currentTerm)
				rf.state = FOLLOWER
				DebugLog(dStateChange, rf.me, "LEADER -> FOLLOWER")

				rf.tickerStartTime = time.Now()
				rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))
			} else if reply.InConsist {
				rf.nextIndex[peer]--
				DebugLog(dSendEntry, rf.me, "SEND Entry -> PEER %d FAIL; DEC nextIndex[%d] -> %d",
					peer, peer, rf.nextIndex[peer])
			}
			rf.mu.Unlock()
		}

		time.Sleep(RPCTimeout)
	}

	mu.Lock()
	if (!*safelyReplicated && *replicas > len(rf.peers)/2) || // safely replicated
		(*safelyReplicated && *replicas == len(rf.peers)) { // replicated on all
		mu.Unlock()
		cond.Signal()
	} else {
		mu.Unlock()
	}
}

func (rf *Raft) issueAppendEntriesRPC(peer int) AppendEntriesReply {
	rf.mu.Lock()
	// if rf.log is empty before appending the new log entry
	// then PrevLogIndex should be -1, and PrevLogIndex should be 0
	var prevLogIndex int
	var prevLogTerm int

	prevLogIndex = rf.nextIndex[peer] - 1
	prevLogTerm = rf.log[prevLogIndex].Term

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.log[prevLogIndex+1:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	rpcInfo := RPCThreadInfo{
		peer:  peer,
		name:  "Raft.AppendEntries",
		args:  args,
		reply: reply,
	}

	replyCh := make(chan interface{}, 1)
	rpcFinished := make(chan bool, 1)
	go rf.RPCTimeoutWrapper(rpcInfo, replyCh)
	go rf.RPCTimeoutTicker(replyCh, rpcInfo, rpcFinished)

	appendEntryReplyIface := <-replyCh
	appendEntryReply := appendEntryReplyIface.(AppendEntriesReply)
	rpcFinished <- true

	return appendEntryReply
}
