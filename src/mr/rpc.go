package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	MapTask    TaskType = 1
	ReduceTask TaskType = 2
	WaitTask   TaskType = 3
	DoneTask   TaskType = 4
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AcTaskArgs struct {
}
type AcTaskReply struct {
	TaskState   TaskType
	FileName    string
	ReduceFiles []string
	TaskId      int
	NReduce     int
}

type TaskDoneArgs struct {
	TaskType TaskType
	FileName []string
	TaskId   int
	//nReduce  int
}

type TaskDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
