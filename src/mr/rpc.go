package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
	MAP int64 = 0
	REDUCE int64 = 1
)

type GetTaskReply struct {
	TaskType int64
	MapInputName int64
	ID int64	// map ID or reduce ID

	MapCount int64
	ReduceCount int64
}

type NoticeArgs struct {
	TaskType int64
	ID int64
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
