package raft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"6.824/labgob"
)

// Debugging
var debugLevel int
var debugStart time.Time

// raft topics
const (
	dElection    = "ELECT"
	dStateChange = "STACH"
	dTermChange  = "TRMCH"
	dVote        = "VOTE "
	dHeartBeart  = "HBEAT"
	dLock        = "LOCK "
	dAppend      = "APPND"
	dSendEntry   = "SNDEN"
	dAgree       = "AGREE"
	dCommit      = "COMIT"
	dRaftState   = "RFSTA"
	dPersist     = "PERST"
	dSnapshot    = "SSHOT"
)

// kv raft topics
const (
	DCallGet          = "CAGET"
	DCallPutOrAppend  = "CAPUT"
	DReplyGet         = "REGET"
	DReplyPutOrAppend = "REPUT"
	DSnapshot         = "SSHOT"
)

// map topic -> debugLevel
var debugLevelOfTopic map[string]int

func init() {
	// initialize debug verbosity
	verbose := os.Getenv("VERBOSE")
	if verbose != "" {
		level, err := strconv.Atoi(verbose)
		if err != nil {
			log.Fatal("Invalid verbosity!")
		}
		debugLevel = level
	}

	// set debug start time
	debugStart = time.Now()

	// initialize map
	debugLevelOfTopic = make(map[string]int)

	raftTopics := []string{
		"ELECT", "STACH", "TRMCH", "VOTE ", "HBEAT",
		"LOCK ", "APPND", "SNDEN", "AGREE", "COMIT",
		"RFSTA", "PERST", "SSHOT",
	}
	for _, str := range raftTopics {
		debugLevelOfTopic[str] = 1
	}

	kvraftTopics := []string{
		"CAGET", "CAPUT", "REGET", "REPUT",
	}
	for _, str := range kvraftTopics {
		debugLevelOfTopic[str] = 2
	}
}

func DebugLog(topic string, peer int, format string, a ...interface{}) (n int, err error) {
	level := debugLevelOfTopic[topic]
	if level <= debugLevel {
		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%6d %s PEER %d: ", time, topic, peer)
		newFmt := prefix + format + "\n"

		fmt.Printf(newFmt, a...)
	}
	return
}

func MinInt(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func MaxInt(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

// Return string form of current raft state
// Make sure you already hold `rf.mu` before calling this function,
// or data race will be detected
func (rf *Raft) stateStr() string {
	switch rf.state {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	}

	log.Fatalf("Invalid raft state: %d", rf.state)
	return ""
}

// Return encoded raft state: [currentTerm, vote, lastIncludedIdx, lastIncludedTerm, log]
// When you call this function, make sure you already hold `rf.mu`
// or data race will be detected
func (rf *Raft) encodeState() []byte {
	buf := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buf)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.vote)
	encoder.Encode(rf.lastIncludedIdx)
	encoder.Encode(rf.lastIncludedTerm)
	encoder.Encode(rf.log)

	return buf.Bytes()
}
