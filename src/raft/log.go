package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// The function commits log entries and sends them to the apply channel.
// All log entries from `rf.lastApplied+1` to `rf.commitIndex`(updated by other functions)
// will be commmited
func (rf *Raft) commitEntries() {
	rf.commitMu.Lock()
	rf.mu.Lock()
	DebugLog(dCommit, rf.me, "COMMIT FROM LA:%d -> CI:%d", rf.lastApplied, rf.commitIndex)
	entriesToApply := make([]LogEntry, rf.commitIndex-rf.lastApplied)
	copy(entriesToApply, rf.log[rf.lastApplied-rf.lastIncludedIdx:rf.commitIndex-rf.lastIncludedIdx])
	lastApplied := rf.lastApplied
	rf.lastApplied = rf.commitIndex
	rf.mu.Unlock()

	for _, entry := range entriesToApply {
		lastApplied++
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: lastApplied,
			Command:      entry.Command,
		}
		DebugLog(dCommit, rf.me, "COMMIT Entry: [I:%d,T:%d]", lastApplied, entry.Term)
		rf.applyCh <- applyMsg
	}
	rf.commitMu.Unlock()
}

func (rf *Raft) handleHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	// reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		rf.mu.Unlock()
		return
	}
	DebugLog(dHeartBeart, rf.me, "Recv HEART BEAT <- %d; {T:%d,PLI:%d,PLT:%d,LC:%d,CT:%d}",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CommitTerm)
	DebugLog(dRaftState, rf.me, "RF STATE: {T:%d,LL:%d,CI:%d,NI:%v,LII:%d,LIT:%d}",
		rf.currentTerm, len(rf.log), rf.commitIndex, rf.nextIndex,
		rf.lastIncludedIdx, rf.lastIncludedTerm)

	// if current state of this peer is LEADER or CANDIDATE receives heartbeat from another peer
	// this peer should become follower
	if rf.state != FOLLOWER {
		DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", rf.stateStr())
		rf.state = FOLLOWER
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		DebugLog(dTermChange, rf.me, "SET TERM -> %d", rf.currentTerm)
		rf.persist()
	}
	rf.leaderId = args.LeaderID

	// every time a raft peer receives heart beat from leader
	// it should reset `tickerStartTime` and `electionTimeout`
	rf.tickerStartTime = time.Now()
	rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))

	// leaderCommit > commitIndex, set commmitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// Case 1: args.LeaderCommit <= len(rf.log)-1
		// After updating the commitIndex of the raft peer, the updated value is args.LeaderCommit.
		// Afterwards, this raft peer needs to commit all Log entries before args.LeaderCommit.
		// However, if the term of some of these Log entries is inconsistent with the term of
		// the Leader raft peer(rf.log[arg.LeaderCommit].Term != args.Term), an error will occur.
		// Therefore, in this inconsistent situation, the commitIndex of the raft peer cannot be updated.

		// Case 2: args.LeaderCommit > len(rf.log-1)
		// After updating the commitIndex of the raft peer, the updated value is len(rf.log)-1
		// But if args.LeaderCommit > rf.commitIndex, which means this raft peer has it's own
		// uncommitted log entries and these log entries are not from the leader.
		// If these entries are committed, an error will occur
		// Therefore, in this inconsistent situation, the commitIndex of the raft peer cannot be updated
		if (args.LeaderCommit <= rf.lastIncludedIdx+len(rf.log) && rf.log[args.LeaderCommit-(rf.lastIncludedIdx+1)].Term != args.CommitTerm) ||
			(args.LeaderCommit > rf.lastIncludedIdx+len(rf.log) && rf.commitIndex < rf.lastIncludedIdx+len(rf.log)) {
			rf.mu.Unlock()
			return
		}

		rf.commitIndex = minInt(args.LeaderCommit, rf.lastIncludedIdx+len(rf.log))
		DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
	}

	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}

	// if commitIndex > lastApplied; increment lastApplied to commitIndex
	// and apply all the log entries before commitIndex
	rf.mu.Unlock()
	rf.commitEntries()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(args.Entries) == 0 { // HeartBeat
		rf.handleHeartBeat(args, reply)
		return
	}

	rf.mu.Lock()
	// Every time a raft peer receives `AppendEntriesRPC` from leader
	// it should also reset timer
	rf.tickerStartTime = time.Now()
	rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))

	// Logging about entries received from leader
	DebugLog(dAppend, rf.me, "Recv New Entries <- %d; {T:%d,PLI:%d,PLT:%d,LC:%d,CT:%d}",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CommitTerm)

	newEntriesStart := args.PrevLogIndex + 1
	newEntriesString := "New Entries: ["
	for idx, entry := range args.Entries {
		newEntriesString += fmt.Sprintf("I:%d,T:%d;", newEntriesStart+idx, entry.Term)
	}
	DebugLog(dAppend, rf.me, "%s]", newEntriesString)

	DebugLog(dRaftState, rf.me, "RF STATE: {T:%d,LL:%d,CI:%d,NI:%v,LII:%d,LIT:%d}",
		rf.currentTerm, len(rf.log), rf.commitIndex, rf.nextIndex,
		rf.lastIncludedIdx, rf.lastIncludedTerm)

	// Process log entries from leader
	// 1. reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		*reply = AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		DebugLog(dAppend, rf.me, "REJECT: Leader's TERM %d < PEER %d's TERM %d",
			args.Term, rf.me, rf.currentTerm)
		rf.mu.Unlock()
		return
	}

	// 2. reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	prevLogIndexOutOfBounds := args.PrevLogIndex-(rf.lastIncludedIdx+1) >= len(rf.log)
	prevLogIndexBeforeFirst := args.PrevLogIndex-(rf.lastIncludedIdx+1) < -1
	prevLogIndexMismatchSnapshot := args.PrevLogIndex == rf.lastIncludedIdx && rf.lastIncludedTerm != args.PrevLogTerm
	prevLogIndexMismatchLogTerm := args.PrevLogIndex-rf.lastIncludedIdx > 0 && rf.log[args.PrevLogIndex-(rf.lastIncludedIdx+1)].Term != args.PrevLogTerm

	if prevLogIndexOutOfBounds || prevLogIndexBeforeFirst ||
		prevLogIndexMismatchSnapshot || prevLogIndexMismatchLogTerm {
		xTerm := rf.log[args.PrevLogIndex-(rf.lastIncludedIdx+1)].Term
		xIndex := args.PrevLogIndex
		for rf.log[xIndex-(rf.lastIncludedIdx+1)-1].Term == xTerm && xIndex-(rf.lastIncludedIdx+1) > 0 {
			xIndex--
		}

		*reply = AppendEntriesReply{
			Term:      rf.currentTerm,
			Success:   false,
			InConsist: true,
			XTerm:     xTerm,
			XIndex:    xIndex,
		}

		DebugLog(dAppend, rf.me, "REJECT: Log doesn't Match PrevLog;{XT:%d;XI:%d}", xTerm, xIndex)
		rf.mu.Unlock()
		return
	}

	// 3. if an existing entry conflicts with a new one(same index but different terms)
	// delete the existing entry and all that following it
	followerLogIndex := args.PrevLogIndex - rf.lastIncludedIdx
	entriesIndex := 0
	for followerLogIndex < len(rf.log) && entriesIndex < len(args.Entries) {
		if rf.log[followerLogIndex].Term != args.Entries[entriesIndex].Term {
			DebugLog(dAppend, rf.me, "DISCARD All Entries after %d", followerLogIndex+rf.lastIncludedIdx)
			break
		}
		followerLogIndex++
		entriesIndex++
	}

	// 4. append any new entries not already in the log
	if entriesIndex < len(args.Entries) {
		rf.log = rf.log[:followerLogIndex]
		rf.log = append(rf.log, args.Entries[entriesIndex:]...)

		// logging about newly appended entries
		entriesStr := "ACCEPT Entries: ["
		for idx, entry := range args.Entries[entriesIndex:] {
			entriesStr += fmt.Sprintf("I:%d,T:%d;", followerLogIndex+(rf.lastIncludedIdx+1)+idx, entry.Term)
		}
		DebugLog(dAppend, rf.me, "%s]", entriesStr)

		rf.persist()
	}

	// 5. if leaderCommit > commitIndex, set commmitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.lastIncludedIdx+len(rf.log))
		DebugLog(dCommit, rf.me, "SET CommitIndex -> %d", rf.commitIndex)
	}

	*reply = AppendEntriesReply{
		Term:    rf.currentTerm,
		Success: true,
	}

	// if commitIndex > lastApplied; increment lastApplied
	// apply log[lastApplied] to state machine
	rf.mu.Unlock()
	rf.commitEntries()
}

// `rf.Start` uses this function as a single thread to reach
// agreement between leader and followers
func (rf *Raft) startAgreement(index int) {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	// `replicas` represents how many servers have replicated the log entry successfully
	// initialized to 1 because the leader already appended this entry to its log
	replicas := 1

	// when the leader trying to reach agreement with followers on log entry `index`
	// became follower, all the threads trying to reach agreement with followers should exit
	exits := 0

	// whether or not this thread has been awakened once by `reachAgreementPeer` threads
	awakened := false

	mu.Lock()
	// issue `AppendEntries` RPCs in parallel to
	// each of the other servers to replicate the entry
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go rf.reachAgreementPeer(peer, index, &mu, cond, &replicas, &exits, &awakened)
	}

	// wait for the log entry to be safely replicated
	cond.Wait()
	awakened = true

	if replicas > len(rf.peers)/2 {
		// update `commitIndex`
		rf.mu.Lock()
		if index > rf.commitIndex {
			rf.commitIndex = index
			DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
		}

		rf.mu.Unlock()
		rf.commitEntries()
	}

	// wait for the log entry to be replicated on all followers
	cond.Wait()
	mu.Unlock()
}

func (rf *Raft) reachAgreementPeer(peer int, index int, mu *sync.Mutex, cond *sync.Cond, replicas *int, exits *int, awakened *bool) {
	for {
		rf.mu.Lock()

		// if this peer is killed, then stop reaching agreement with peer
		if rf.killed() || rf.state != LEADER || index-rf.lastIncludedIdx < len(rf.log) {
			rf.mu.Unlock()

			mu.Lock()
			*exits++
			mu.Unlock()
			break
		}

		if rf.nextIndex[peer] > rf.lastIncludedIdx {
			// log about entries leader gonna send to the raft peer
			sendEntriesStart := rf.nextIndex[peer] - (rf.lastIncludedIdx + 1)
			sendEntriesStr := fmt.Sprintf("SEND -> PEER %d; [", peer)
			for idx, entry := range rf.log[sendEntriesStart:] {
				sendEntriesStr += fmt.Sprintf("I:%d,T:%d;", (rf.lastIncludedIdx+1)+sendEntriesStart+idx, entry.Term)
			}
			DebugLog(dSendEntry, rf.me, "%s]", sendEntriesStr)

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
					DebugLog(dSendEntry, rf.me, "SEND -> PEER %d SUCCESS; nextIndex[%d] -> %d; NI:%v",
						peer, peer, rf.nextIndex[peer], rf.nextIndex)
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
					rf.persist()

					DebugLog(dTermChange, rf.me, "%s -> FOLLOWER; TERM -> %d", rf.stateStr(), rf.currentTerm)
					rf.state = FOLLOWER

					rf.tickerStartTime = time.Now()
					rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))
				} else if reply.InConsist {
					rf.nextIndex[peer] = reply.XIndex
					DebugLog(dSendEntry, rf.me, "SEND -> PEER %d FAIL; nextIndex[%d] -> %d; NI:%v",
						peer, peer, rf.nextIndex[peer], rf.nextIndex)
				}
				rf.mu.Unlock()
			}
		} else {
			lastIncludedIdx := rf.lastIncludedIdx
			replyTerm := rf.issueInstallSnapshotRPC(peer)
			if replyTerm == 0 {
				continue
			}

			rf.mu.Lock()
			if replyTerm > rf.currentTerm {
				DebugLog(dSnapshot, rf.me, "INSTALL Snapshot -> PEER %d FAIL", peer)
				rf.currentTerm = replyTerm
				DebugLog(dTermChange, rf.me, "TERM -> %d", rf.currentTerm)
				rf.persist()

				DebugLog(dStateChange, rf.me, "%s -> FOLLOWER", rf.stateStr())
				rf.state = FOLLOWER
				rf.tickerStartTime = time.Now()
				rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))
			} else {
				rf.nextIndex[peer] = lastIncludedIdx + 1
				DebugLog(dSnapshot, rf.me, "INSTALL Snapshot SUCCESS; nextIndex[%d] -> %d", peer, rf.nextIndex[peer])
			}
			rf.mu.Unlock()
		}

		time.Sleep(RPCTimeout)
	}

	mu.Lock()
	if (!*awakened && *replicas+*exits > len(rf.peers)/2) || // safely replicated or partially exits
		(*awakened && *replicas+*exits == len(rf.peers)) { // replicated on all	or all exits
		mu.Unlock()
		cond.Signal()
	} else {
		mu.Unlock()
	}
}

// Make sure that every time you call this function
// you must hold `rf.mu`
func (rf *Raft) issueAppendEntriesRPC(peer int) AppendEntriesReply {
	prevLogIndex := rf.nextIndex[peer] - 1
	var prevLogTerm int
	if prevLogIndex == rf.lastIncludedIdx {
		prevLogTerm = rf.lastIncludedTerm
	} else {
		prevLogTerm = rf.log[prevLogIndex-(rf.lastIncludedIdx+1)].Term
	}

	var commitTerm int
	if rf.commitIndex == rf.lastIncludedIdx {
		commitTerm = rf.lastIncludedTerm
	} else {
		commitTerm = rf.log[rf.commitIndex-(rf.lastIncludedIdx+1)].Term
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]LogEntry, len(rf.log)-prevLogIndex+rf.lastIncludedIdx),
		LeaderCommit: rf.commitIndex,
		CommitTerm:   commitTerm,
	}
	copy(args.Entries, rf.log[prevLogIndex-rf.lastIncludedIdx:])
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
