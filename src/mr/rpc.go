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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	InitiateTask = iota
	FinishMap
	FinishReduce
)

const (
	Unassigned = iota
	Assined
	Done
)

const mapMess = "map"

const redMess = "reduce"

type Communicate struct {
	Id      int
	Content string
}

// Add your RPC definitions here.

type MapJob struct {
	InputFile string
	MapJobNum int
	ReduceNum int
}

type ReduceJob struct {
	IntermediateFiles []string
	ReduceNum         int
}

type JobReply struct {
	Task   string
	Map    MapJob
	Reduce ReduceJob
	IsDone bool
}

type ReportMapJob struct {
	InputFile           string
	IntermediateFileMap map[int]string
}

type ReportReduceJob struct {
	ReduceNum int
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
