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

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType int64		// map or reduce
	MapInputName string	// input filename
	ID int64	// map ID or reduce ID

	MapCount int64 // 用于 reduce task
	ReduceCount int64	// 用于 map task
	Done bool	// 所有操作已经完成
}

func CreateMapTaskReply(fileName string, id int64, rc int64) *GetTaskReply{
	return &GetTaskReply{
			TaskType: MAP,
			MapInputName: fileName,
			ID: id,
			ReduceCount: rc,
			Done: false,
	}
}

func CreateReduceTaskReply(id int64, mc int64) *GetTaskReply{
	return &GetTaskReply{
		TaskType: REDUCE,
		ID: id,
		MapCount: mc,
		Done: false,
	}
}

func CreateDoneTaskReply() * GetTaskReply{
	return &GetTaskReply{
		Done: true,
	}
}

type NoticeArgs struct {
	TaskType int64 	// map or reduce
	ID int64		// a number
}

type NoticeReply struct {
	
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
