package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	DONE = iota
	DEALING
	PENDING
)

type Coordinator struct {
	sync.Mutex
	// Your definitions here.
	MapInputFileRecord map[string]int64 	// 记录 map task 的输入，以及完成情况
	MapTaskDoneCount int64			// 当前已经完成的 map task 数量
	ReduceTaskRecord map[int64]int64 // 记录 reduce task 的完成情况
	ReduceTaskDoneCount int64

	MapTaskCount 	int64
	ReduceTaskCount int64

}



// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (c *Coordinator) GetTask(reply *GetTaskReply) error{

	return nil
}

func (c *Coordinator) NoticeDone(args *NoticeArgs){

}

func (c *Coordinator) timerCB(id int64){

}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	c.Lock()
	defer c.Unlock()

	// Your code here.
	if c.MapTaskDoneCount == c.MapTaskCount &&
		c.ReduceTaskDoneCount == c.ReduceTaskDoneCount{
		ret = true
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
