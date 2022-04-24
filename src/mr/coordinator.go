package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//const (
//	DONE = iota
//	DEALING
//	PENDING
//)

type Coordinator struct {
	sync.Mutex
	// Your definitions here.
	MapPendingFileRecord map[string]bool 	// map，未处理的输入
	MapDealingFileRecord map[string]bool 	// map，正在处理的输入
	MapDoneFileRecord map[string] bool 		// map，已经处理的输入
	MapIDToFileName map[int64]string
	MapFileNameToID map[string]int64

	ReducePendingTaskRecord map[int64]bool // reduce，未处理
	ReduceDealingTaskRecord map[int64]bool // reduce，正在处理
	ReduceDoneTaskRecord map[int64]bool	// reduce，已经处理

	MapTaskCount 	int64		// const
	ReduceTaskCount int64		// const

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


func (c *Coordinator) GetTask(args ,reply *GetTaskReply) error{
	for true {
		c.Lock()
		if len(c.MapPendingFileRecord) != 0 {
			// 1. 如果还有 map 没有被调用
			var key string
			for k, _ := range c.MapPendingFileRecord {
				key = k
				break
			}

			reply = CreateMapTaskReply(key, c.MapFileNameToID[key], c.ReduceTaskCount)
			delete(c.MapPendingFileRecord, key)
			c.MapDealingFileRecord[key] = true
			// TODO: 设置超时回调函数
			time.AfterFunc(10*time.Second, c.timerCB( MAP,c.MapFileNameToID[key]))

			return nil
		}else if len(c.MapDealingFileRecord) != 0 {
			// 2. 如果还有 map 没有被调用
		}else if int64(len(c.MapDoneFileRecord)) == c.MapTaskCount &&
			len(c.ReducePendingTaskRecord) != 0{
			// 3. 有未出的reduce，并且当前条件可以处理reduce了
			var key int64
			for k, _ := range c.ReducePendingTaskRecord {
				key = k
				break
			}
			CreateReduceTaskReply(key,c.MapTaskCount)
			delete(c.ReducePendingTaskRecord, key)
			c.ReduceDealingTaskRecord[key] = true
			// TODO: 设置超时回调函数
			time.AfterFunc(10*time.Second, c.timerCB(REDUCE, key))

			return nil
		}else if len(c.ReduceDealingTaskRecord) != 0{
			// 4. 只有正在处理的reduce

		}else if int64(len(c.ReduceDoneTaskRecord)) == c.ReduceTaskCount &&
			int64(len(c.MapDoneFileRecord)) == c.MapTaskCount{

			// 5. map reduce done
			reply = CreateDoneTaskReply()
			return nil
		}else{
			panic("undefine")
		}
		c.Unlock()
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator) NoticeDone(args *NoticeArgs){
	c.Lock()
	defer c.Unlock()
	if args.TaskType == MAP{
		delete(c.MapDealingFileRecord, c.MapIDToFileName[args.ID])
		c.MapDoneFileRecord[c.MapIDToFileName[args.ID]] = true
	}else if args.TaskType == REDUCE{
		delete(c.ReduceDealingTaskRecord, args.ID)
		c.ReduceDoneTaskRecord[args.ID] = true
	}else{
		panic("undefine")
	}
}

func (c *Coordinator) timerCB(taskType, id int64) func(){
	return func() {
		c.Lock()
		defer c.Lock()
		if taskType == MAP{
			c.MapPendingFileRecord[c.MapIDToFileName[id]] = true
		}else if taskType == REDUCE{
			c.ReducePendingTaskRecord[id] = true
		}else{
			panic("undefine")
		}
	}
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
	if int64(len(c.MapDoneFileRecord)) == c.MapTaskCount &&
		int64(len(c.ReduceDoneTaskRecord)) == c.ReduceTaskCount{
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

	c.MapTaskCount = int64(len(files))
	c.ReduceTaskCount = int64(nReduce)

	for i,fn := range files{
		c.MapPendingFileRecord[fn] = true
		c.MapFileNameToID[fn] = int64(i)
		c.MapIDToFileName[int64(i)] = fn
	}
	for i := 0; i < nReduce; i++{
		c.ReducePendingTaskRecord[int64(i)] = true
	}

	c.server()
	return &c
}
