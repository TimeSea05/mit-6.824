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
	DebugLog(dHeartBeart, rf.me, "Recv HEART BEAT <- %d; {T:%d,PLI:%d,PLT:%d,LC:%d,CT:%d}",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CommitTerm)
	DebugLog(dRaftState, rf.me, "RF STATE: {T:%d,LL:%d,CI:%d,LA:%d,NI:%v}",
		rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied, rf.nextIndex)

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

	/********** Commit Entries Accoring to Leader's `commitIndex` **********/
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
		if (args.LeaderCommit <= len(rf.log)-1 && rf.log[args.LeaderCommit].Term != args.Term) ||
			args.LeaderCommit > len(rf.log)-1 && rf.commitIndex < len(rf.log)-1 {
			return
		}

		rf.commitIndex = minInt(args.LeaderCommit, len(rf.log)-1)
		DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
	}

	// if commitIndex > lastApplied; increment lastApplied to commitIndex
	// and apply all the log entries before commitIndex
	for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[idx].Command,
			CommandIndex: idx,
		}
		rf.applyCh <- applyMsg
		DebugLog(dCommit, rf.me, "APPLY Entry: [I:%d,T:%d]", idx, rf.log[idx].Term)
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
	DebugLog(dAppend, rf.me, "Recv New Entries <- %d; {T:%d,PLI:%d,PLT:%d,LC:%d,CT:%d}",
		args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.CommitTerm)

	newEntriesStart := args.PrevLogIndex + 1
	newEntriesString := "New Entries: ["
	for idx, entry := range args.Entries {
		newEntriesString += fmt.Sprintf("I:%d,T:%d;", newEntriesStart+idx, entry.Term)
	}
	DebugLog(dAppend, rf.me, "%s]", newEntriesString)

	DebugLog(dRaftState, rf.me, "RF STATE: {T:%d,LL:%d,CI:%d,LA:%d,NI:%v}",
		rf.currentTerm, len(rf.log), rf.commitIndex, rf.lastApplied, rf.nextIndex)

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
	followerLogIndex := args.PrevLogIndex + 1
	entriesIndex := 0
	for followerLogIndex < len(rf.log) && entriesIndex < len(args.Entries) {
		if rf.log[followerLogIndex].Term != args.Entries[entriesIndex].Term {
			DebugLog(dAppend, rf.me, "DISCARD All Entries after %d", followerLogIndex-1)
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
			entriesStr += fmt.Sprintf("I:%d,T:%d;", followerLogIndex+idx, entry.Term)
		}
		DebugLog(dAppend, rf.me, "%s]", entriesStr)

		rf.persist()
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
		DebugLog(dCommit, rf.me, "APPLY Entry: [I:%d,T:%d]", idx, rf.log[idx].Term)
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

	rf.mu.Lock()
	if replicas > len(rf.peers)/2 {
		// update `commitIndex`
		if index > rf.commitIndex {
			rf.commitIndex = index
			DebugLog(dCommit, rf.me, "SET commitIndex -> %d", rf.commitIndex)
		}

		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied,
				Command:      rf.log[rf.lastApplied].Command,
			}
			DebugLog(dCommit, rf.me, "COMMIT Entry: [I:%d,T:%d]", rf.lastApplied, rf.log[index].Term)
			rf.applyCh <- applyMsg
		}
	}

	rf.mu.Unlock()

	// wait for the log entry to be replicated on all followers
	cond.Wait()
	mu.Unlock()
}

func (rf *Raft) reachAgreementPeer(peer int, index int, mu *sync.Mutex, cond *sync.Cond, replicas *int, exits *int, awakened *bool) {
	for {
		rf.mu.Lock()

		// if this peer is killed, then stop reaching agreement with peer
		if rf.killed() || rf.state != LEADER || len(rf.log) > index+1 {
			rf.mu.Unlock()

			mu.Lock()
			*exits++
			mu.Unlock()
			break
		}

		// if a follower disconnects from network, every time the leader received a command
		// the leader will start a thread to reach agreement with the disconnected follower
		// if one thread reach agreement with the rejoined follower successfully, then it will set
		// rf.nextIndex[peer] to len(rf.log)
		// at this time, all the other threads don't need to loop again and again because agreement has
		// been reached
		// they just need to log about that infomation and exit loop
		if rf.nextIndex[peer] >= len(rf.log) {
			DebugLog(dAgree, rf.me, "REACH Agreement - PEER %d", peer)
			rf.mu.Unlock()

			mu.Lock()
			*replicas++
			mu.Unlock()
			break
		}

		// log about entries leader gonna send to the raft peer
		sendEntriesStart := rf.nextIndex[peer]
		sendEntriesStr := fmt.Sprintf("SEND -> PEER %d; [", peer)
		for idx, entry := range rf.log[sendEntriesStart:] {
			sendEntriesStr += fmt.Sprintf("I:%d,T:%d;", sendEntriesStart+idx, entry.Term)
		}
		DebugLog(dSendEntry, rf.me, "%s]", sendEntriesStr)
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

				rf.state = FOLLOWER
				DebugLog(dTermChange, rf.me, "LEADER -> FOLLOWER; TERM -> %d", rf.currentTerm)

				rf.tickerStartTime = time.Now()
				rf.electionTimeout = time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval))
			} else if reply.InConsist {
				rf.nextIndex[peer]--
				DebugLog(dSendEntry, rf.me, "SEND -> PEER %d FAIL; nextIndex[%d] -> %d; NI:%v",
					peer, peer, rf.nextIndex[peer], rf.nextIndex)
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

func (rf *Raft) issueAppendEntriesRPC(peer int) AppendEntriesReply {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[peer] - 1
	prevLogTerm := rf.log[prevLogIndex].Term

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]LogEntry, len(rf.log)-1-prevLogIndex),
		LeaderCommit: rf.commitIndex,
		CommitTerm:   rf.log[rf.commitIndex].Term,
	}
	copy(args.Entries, rf.log[prevLogIndex+1:])
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
