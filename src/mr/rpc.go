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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	DONE   int = 0
	MAP    int = 1
	REDUCE int = 2
	WAIT   int = 3
)

type Args struct {
	WorkerId    int
	TaskType    int
	MapIndex    int
	ReduceIndex int
}

type Reply struct {
	FileName    string
	TaskType    int
	TaskStatus  bool
	MapIndex    int
	ReduceIndex int
	NMap        int
	NReduce     int
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
