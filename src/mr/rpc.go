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
	MAP int = 0
	REDUCE int = 1
)

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType int		// map or reduce
	MapInputName string	// input filename
	ID int	// map ID or reduce ID

	MapCount int // 用于 reduce task
	ReduceCount int	// 用于 map task
	Done bool	// 所有操作已经完成
}

func CreateMapTaskReply(fileName string, id int, rc int) *GetTaskReply{
	return &GetTaskReply{
			TaskType: MAP,
			MapInputName: fileName,
			ID: id,
			ReduceCount: rc,
			Done: false,
	}
}
func InitMapTaskReply(out *GetTaskReply, fileName string, id int, rc int){
	out.TaskType = MAP
	out.MapInputName = fileName
	out.ID = id
	out.ReduceCount = rc
	out.Done = false
}


func CreateReduceTaskReply(id int, mc int) *GetTaskReply{
	return &GetTaskReply{
		TaskType: REDUCE,
		ID: id,
		MapCount: mc,
		Done: false,
	}
}

func InitReduceTaskReply(out *GetTaskReply, id int, mc int){
	out.TaskType = REDUCE
	out.ID = id
	out.MapCount = mc
	out.Done = false
}


func CreateDoneTaskReply() * GetTaskReply{
	return &GetTaskReply{
		Done: true,
	}
}

func InitDoneTaskReply(out *GetTaskReply) {
	out.Done = true
}

type NoticeArgs struct {
	TaskType int 	// map or reduce
	ID int		// a number
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
