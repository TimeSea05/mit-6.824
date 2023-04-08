package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// Debugging
var debugLevel int
var debugStart time.Time

// event topics
const (
	dElection    = "ELECT"
	dStateChange = "STACH"
	dTermChange  = "TRMCH"
	dVote        = "VOTE "
	dHeartBeart  = "HBEAT"
	dLock        = "LOCK "
	dTimer       = "TIMER"
	dWaitGroup   = "WGRUP"
	dRPC         = "RPCTH"
	dAppend      = "APPND"
	dSendEntry   = "SNDEN"
	dAgree       = "AGREE"
	dCommit      = "COMIT"
	dRaftState   = "RFSTA"
	dPersist     = "PERST"
	dSnapshot    = "SSHOT"
)

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
}

func DebugLog(topic string, peer int, format string, a ...interface{}) (n int, err error) {
	if debugLevel > 0 {
		if debugLevel < 2 && topic == dPersist {
			return
		}

		time := time.Since(debugStart).Milliseconds()
		prefix := fmt.Sprintf("%6d %s PEER %d: ", time, topic, peer)
		newFmt := prefix + format + "\n"

		fmt.Printf(newFmt, a...)
	}
	return
}

func minInt(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func maxInt(a int, b int) int {
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
