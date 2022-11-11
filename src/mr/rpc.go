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

type AskForTaskArgs struct {
	WorkerId int
}

type AskForTaskReply struct {
	TaskType int
	Id       int
	// 0: No current free task, 1: Map requires FileName & NumReduce, 2: Reduce requires BucketId & FilesLocation
	FileName      string
	NumReduce     int
	FilesLocation []string
}

type MapCompletionArgs struct {
	FileName      string
	FilesLocation []string
}

type MapCompletionReply struct {
}

type ReduceCompletionArgs struct {
	Index int
}

type ReduceCompletionReply struct {
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
