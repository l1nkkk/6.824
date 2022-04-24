package mr

import "C"
import (
	"fmt"
	"github.com/l1nkkk/6.824/src/common/logger"
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

// for debug
func (c *Coordinator)String(){
	fmt.Printf("MapTaskCount: %6d, ReduceTaskCount: %6d \n", c.MapTaskCount, c.ReduceTaskCount)
	fshow1 := func(name string, input map[string]bool){
		fmt.Printf("=========(Map) %s Record=========", name)
		for k,_ := range input{
			fmt.Printf("- %9d %s\n", c.MapFileNameToID[k], k)
		}
	}
	fshow2 := func(name string, input map[int64]bool){
		fmt.Printf("=========(Reduce) %s Record=========", name)
		for k,_ := range input{
			fmt.Printf("- %9d \n",  k)
		}
	}

	fshow1("MapPendingFileRecord", c.MapPendingFileRecord)
	fshow1("MapDealingFileRecord", c.MapDealingFileRecord)
	fshow1("MapDoneFileRecord", c.MapDoneFileRecord)

	fshow2("ReducePendingTaskRecord", c.ReducePendingTaskRecord)
	fshow2("ReduceDealingTaskRecord", c.ReduceDealingTaskRecord)
	fshow2("ReduceDoneTaskRecord", c.ReduceDoneTaskRecord)

}



const (
	InitState = iota
	DealingMapState
	WaitMapDoneState
	DealingReduceState
	WaitReduceDoneState
	DoneState

)
func (c *Coordinator) CheckState(flag int){
	c.Lock()
	defer c.Unlock()
	var (
		msg string
		maprecords int64
		reducerecords int64
	)

	mpendingNum := int64(len(c.MapPendingFileRecord))
	mdealingNum := int64(len(c.MapDealingFileRecord))
	mdoneNum := int64(len(c.MapDoneFileRecord))

	rpendingNum := int64(len(c.ReducePendingTaskRecord))
	rdealingNum := int64(len(c.ReduceDealingTaskRecord))
	rdoneNum := int64(len(c.ReduceDoneTaskRecord))


	switch flag {
	case InitState:
		if  mpendingNum == c.MapTaskCount && mdealingNum == 0 &&  mdoneNum == 0 &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0 {
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	case DealingMapState:
		if  mpendingNum != 0 && mdealingNum != 0  &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0{
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	case WaitMapDoneState:
		if mpendingNum == 0 && mdealingNum != 0 &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0 {
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	case DealingReduceState:
		if mpendingNum == 0 && mdealingNum == 0 &&  mdoneNum == c.MapTaskCount &&
			rpendingNum != 0 && rdealingNum != 0 {
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	case WaitReduceDoneState:
		if mpendingNum == 0 && mdealingNum == 0 &&  mdoneNum == c.MapTaskCount &&
			rpendingNum == 0 && rdealingNum != 0  && rdoneNum != 0 {
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	case DoneState:
		if  mpendingNum == 0 && mdealingNum == 0 &&  mdoneNum == c.MapTaskCount &&
			rpendingNum == 0 && rdealingNum == 0 && rdoneNum == c.ReduceTaskCount {
			goto CHECK2
		}else {
			msg = "InitState error"
			goto PANIC
		}
	}
CHECK2:
	// 记录的一致性检查，避免操作过程中，某个记录的丢失
	maprecords = int64(len(c.MapDealingFileRecord) + len(c.MapPendingFileRecord) +
		len(c.MapDoneFileRecord))
	reducerecords = int64(len(c.ReduceDealingTaskRecord) + len(c.ReducePendingTaskRecord) +
		len(c.ReduceDoneTaskRecord))
	if maprecords == c.MapTaskCount && reducerecords == c.ReduceTaskCount{
		return
	}else if maprecords > c.MapTaskCount || reducerecords > c.ReduceTaskCount{
		logger.Error.Printf("time out occur\n %v", c)

	} else{
		msg = "Consistence error"
		goto PANIC
	}
PANIC:
	logger.Panic.Panic(msg)
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

// GetTask 获取待执行的 task
func (c *Coordinator) GetTask(args *GetTaskArgs,reply *GetTaskReply) error{
	for true {
		func(){
			c.Lock()
			defer c.Unlock()
			if c.getTask(reply){
				return
			}
		}()
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator)getTask(reply *GetTaskReply) bool{
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
		c.CheckState(DealingMapState)

		// TODO: 设置超时回调函数
		time.AfterFunc(10*time.Second, c.timerCB( MAP,c.MapFileNameToID[key]))

		logger.Info.Printf("task: %v be requeire", reply)
		return true
	}else if len(c.MapDealingFileRecord) != 0 {
		// 2. 如果还有 map 没有被调用
		logger.Info.Print("wait until map tasks all done")
		c.CheckState(WaitMapDoneState)
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
		c.CheckState(DealingReduceState)

		// TODO: 设置超时回调函数
		time.AfterFunc(10*time.Second, c.timerCB(REDUCE, key))

		logger.Info.Printf("task: %v be requeire", reply)
		return true
	}else if len(c.ReduceDealingTaskRecord) != 0{
		// 4. 只有正在处理的reduce
		logger.Info.Print("wait until reduce tasks all done")

		c.CheckState(WaitReduceDoneState)

	}else if int64(len(c.ReduceDoneTaskRecord)) == c.ReduceTaskCount &&
		int64(len(c.MapDoneFileRecord)) == c.MapTaskCount{

		// 5. map reduce done
		reply = CreateDoneTaskReply()
		c.CheckState(DoneState)
		return true
	}else{
		logger.Panic.Panic("undefine")
	}
	return false
}


func (c *Coordinator) NoticeDone(args *NoticeArgs, reply *NoticeReply){
	c.Lock()
	defer c.Unlock()
	if args.TaskType == MAP{
		delete(c.MapDealingFileRecord, c.MapIDToFileName[args.ID])
		c.MapDoneFileRecord[c.MapIDToFileName[args.ID]] = true

		logger.Info.Printf("map task: %v done", args)
	}else if args.TaskType == REDUCE{
		delete(c.ReduceDealingTaskRecord, args.ID)
		c.ReduceDoneTaskRecord[args.ID] = true

		logger.Info.Printf("reduce task: %v done", args)
	}else{
		logger.Panic.Panic("undefine")
	}
}

func (c *Coordinator) timerCB(taskType, id int64) func(){
	return func() {
		c.Lock()
		defer c.Lock()
		if taskType == MAP{
			c.MapPendingFileRecord[c.MapIDToFileName[id]] = true

			logger.Warning.Printf("map task: %v time out", id)
		}else if taskType == REDUCE{
			c.ReducePendingTaskRecord[id] = true

			logger.Warning.Printf("reduce task: %v time out", id)
		}else{
			logger.Panic.Panic("undefine")
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
