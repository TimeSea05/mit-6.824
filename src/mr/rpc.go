package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type WorkerRegisterArgs string
type WorkerRegisterReply struct {
	ID   int
	Name string

	// partition intermediate output into
	// `NReduce` buckets
	NReduce int
}

type AssignMapTaskReply struct {
	ID   int
	Name string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
