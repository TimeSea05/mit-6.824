package raft

import (
	"bytes"

	"6.824/labgob"
)

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.lastIncludedTerm = rf.log[index-(rf.lastIncludedIdx+1)].Term
	rf.log = rf.log[index-rf.lastIncludedIdx:]
	rf.lastIncludedIdx = index
	DebugLog(dSnapshot, rf.me, "Take Snapshot; LL:%d,LII:%d,LIT:%d", len(rf.log), rf.lastIncludedIdx, rf.lastIncludedTerm)

	stateBuf := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateBuf)
	stateEncoder.Encode(rf.currentTerm)
	stateEncoder.Encode(rf.vote)
	stateEncoder.Encode(rf.lastIncludedIdx)
	stateEncoder.Encode(rf.lastIncludedTerm)
	stateEncoder.Encode(rf.log)
	state := stateBuf.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, replyTerm *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	*replyTerm = rf.currentTerm
	// Reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	// If existing log entry has same index and term as snapshot's
	// last included entry, retain log entries following it and reply
	// Else, discard the entire log
	if rf.lastIncludedIdx+len(rf.log) > args.LastIncludedIndex {
		rf.log = rf.log[args.LastIncludedIndex-rf.lastIncludedIdx:]
		DebugLog(dSnapshot, rf.me, "DISCARD all entries before %d", args.LastIncludedIndex)
	} else {
		rf.log = nil
		DebugLog(dSnapshot, rf.me, "DISCARD the whole log")
	}

	// Reset state machine using snapshot contents
	rf.lastIncludedIdx = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.currentTerm = args.Term

	rf.commitIndex = maxInt(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = maxInt(rf.lastApplied, args.LastIncludedIndex)
	DebugLog(dRaftState, rf.me, "LII:%d,LIT:%d,CT:%d,CI:%d,LA:%d",
		rf.lastIncludedIdx, rf.lastIncludedTerm, rf.currentTerm,
		rf.commitIndex, rf.lastApplied)

	applyMsg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.Term,
	}
	rf.applyCh <- applyMsg
}

// Make sure that every time you call this function
// you must hold `rf.mu`
func (rf *Raft) issueInstallSnapshotRPC(peer int) int {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.lastIncludedIdx,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	var reply int
	replyCh := make(chan interface{}, 1)
	rpcInfo := RPCThreadInfo{
		peer:  peer,
		name:  "Raft.InstallSnapshot",
		args:  args,
		reply: reply,
	}
	rpcFinished := make(chan bool, 1)

	go rf.RPCTimeoutWrapper(rpcInfo, replyCh)
	go rf.RPCTimeoutTicker(replyCh, rpcInfo, rpcFinished)

	replyTerm := (<-replyCh).(int)
	rpcFinished <- true

	return replyTerm
}

// Previously, this lab recommended that you implement a function called CondInstallSnapshot
// to avoid the requirement that snapshots and log entries sent on applyCh are coordinated.
// This vestigal API interface remains, but you are discouraged from implementing it
// instead, we suggest that you simply have it return true.
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}
