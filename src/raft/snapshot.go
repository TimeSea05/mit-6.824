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
	DebugLog(dSnapshot, rf.me, "Take Snapshot; Index: %d", index)

	rf.firstEntryTerm = rf.log[index-rf.firstEntryIndex].Term
	rf.log = rf.log[index-rf.firstEntryIndex:]
	rf.firstEntryIndex = index
	rf.mu.Unlock()

	stateBuf := new(bytes.Buffer)
	stateEncoder := labgob.NewEncoder(stateBuf)
	stateEncoder.Encode(rf.currentTerm)
	stateEncoder.Encode(rf.vote)
	stateEncoder.Encode(rf.firstEntryIndex)
	stateEncoder.Encode(rf.firstEntryTerm)
	stateEncoder.Encode(rf.log)
	state := stateBuf.Bytes()

	rf.persister.SaveStateAndSnapshot(state, snapshot)
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
