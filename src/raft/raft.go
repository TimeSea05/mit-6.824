package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft peer.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same peer.
//

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same peer, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Vote struct {
	CandidateID int // candidate ID that received vote
	Term        int // in which term the candidate received vote
}

type LogEntry struct {
	Term    int         // term when entry received by leader
	Command interface{} // command for state machine
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft peer must maintain.

	state       int  // follower, leader or candidate
	currentTerm int  // last term peer has seen
	vote        Vote // term and candidate this peer has voted for
	leaderId    int  // id of leader in currentTerm

	tickerStartTime time.Time     // when timer(ticker) starts
	electionTimeout time.Duration // max time allowed for leader and follower not to communicate

	commitIndex int           // index of highest log entry known to be committed
	lastApplied int           // index of highest log entry applied to state machine
	applyCh     chan ApplyMsg // tester or service expects Raft to send ApplyMsg messages on
	log         []LogEntry    // log entries
	nextIndex   []int         // for each server, index of the next log entry to send to that server
	// matchIndex  []int         // for each server, index of highest log entry known to be replicated on server

	agreeThreads int // number of threads trying to reach agreement with followers
}

// return currentTerm and whether this peer
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.vote)
	for _, entry := range rf.log {
		encoder.Encode(entry)
	}

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)

	DebugLog(dPersist, rf.me, "SAVE: CT:%d; V:{ID:%d,T:%d};",
		rf.currentTerm, rf.vote.CandidateID, rf.vote.Term)
	logStr := "SAVE: Log["
	for idx, entry := range rf.log {
		logStr += fmt.Sprintf("I:%d,T:%d;", idx, entry.Term)
	}
	DebugLog(dPersist, rf.me, "%s]", logStr)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	buf := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buf)

	var currentTerm int
	var vote Vote

	raftLog := make([]LogEntry, 0)

	if err := decoder.Decode(&currentTerm); err != nil {
		log.Fatalf("Decode field `currentTerm` failed: %v", err)
	}
	if err := decoder.Decode(&vote); err != nil {
		log.Fatalf("Decode field `vote` failed: %v", err)
	}

	for {
		var entry LogEntry
		err := decoder.Decode(&entry)
		if err == nil {
			raftLog = append(raftLog, entry)
		} else if err == io.EOF {
			break
		} else {
			log.Fatalf("Decode field `log` failed: %v", err)
		}
	}

	rf.currentTerm = currentTerm
	rf.vote = vote
	rf.log = raftLog

	DebugLog(dPersist, rf.me, "LOAD: CT:%d; V:{ID:%d,T:%d};")
	logStr := "ReadPersist: Log["
	for idx, entry := range rf.log {
		logStr += fmt.Sprintf("I:%d,T:%d;", idx, entry.Term)
	}
	DebugLog(dPersist, rf.me, "%s]", logStr)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the service using Raft (e.g. a k/v peer) wants to start
// agreement on the next command to be appended to Raft's log. if this
// peer isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this peer believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		newEntry := LogEntry{Term: term, Command: command}
		rf.log = append(rf.log, newEntry)
		DebugLog(dAppend, rf.me, "Agree On [I:%d,T:%d]", index, term)
		rf.persist()

		go rf.startAgreement(index)
		rf.agreeThreads++
		DebugLog(dAgree, rf.me, "SET agreeThreads -> %d", rf.agreeThreads)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft peer. the ports
// of all the Raft servers (including this one) are in peers[]. this
// peer's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this peer to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,

		// Your initialization code here (2A, 2B, 2C).
		vote:            Vote{CandidateID: -1, Term: 0},
		leaderId:        -1,
		tickerStartTime: time.Now(),
		electionTimeout: time.Millisecond * time.Duration(ElectionTimeoutLeftEnd+rand.Intn(ElectionTimeoutInterval)),

		applyCh:   applyCh,
		log:       make([]LogEntry, 1),
		nextIndex: make([]int, len(peers)),
	}

	// initialize all nextIndex values to the index just
	// after the last one in its log
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
