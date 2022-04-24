package mr

import "C"
import (
	"fmt"
	"github.com/l1nkkk/6.824/src/common/logger"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

	sync.Mutex
	MapPendingFileRecord map[string]bool // map，未处理的输入
	MapDealingFileRecord map[string]bool // map，正在处理的输入
	MapDoneFileRecord    map[string]bool // map，已经处理的输入
	MapIDToFileName      map[int]string  // id --> inputfileName
	MapFileNameToID      map[string]int  // inputfileName --> id

	ReducePendingTaskRecord map[int]bool // reduce，未处理
	ReduceDealingTaskRecord map[int]bool // reduce，正在处理
	ReduceDoneTaskRecord    map[int]bool // reduce，已经处理

	MapTaskCount    int // const
	ReduceTaskCount int // const
	TimerMgr        map[int]*time.Timer
}

// for debug
func (c *Coordinator) String() string{
	var out string
	out += fmt.Sprintf("MapTaskCount: %6d, ReduceTaskCount: %6d \n", c.MapTaskCount, c.ReduceTaskCount)
	fshow1 := func(name string, input map[string]bool) {
		out += fmt.Sprintf("=========(Map) %s Record=========\n", name)
		for k, _ := range input {
			out += fmt.Sprintf("- %9d %s\n", c.MapFileNameToID[k], k)
		}
	}
	fshow2 := func(name string, input map[int]bool) {
		out += fmt.Sprintf("=========(Reduce) %s Record=========\n", name)
		for k, _ := range input {
			out += fmt.Sprintf("- %9d \n", k)
		}
	}

	fshow1("MapPendingFileRecord", c.MapPendingFileRecord)
	fshow1("MapDealingFileRecord", c.MapDealingFileRecord)
	fshow1("MapDoneFileRecord", c.MapDoneFileRecord)

	fshow2("ReducePendingTaskRecord", c.ReducePendingTaskRecord)
	fshow2("ReduceDealingTaskRecord", c.ReduceDealingTaskRecord)
	fshow2("ReduceDoneTaskRecord", c.ReduceDoneTaskRecord)
	return out
}

const (
	InitState = iota
	DealingMapState
	WaitMapDoneState
	DealingReduceState
	WaitReduceDoneState
	DoneState
)

// showSimplyState 用戶使用前需要先上锁，简单打印当前状态
func (c *Coordinator) showSimplyState(){
	mpendingNum := len(c.MapPendingFileRecord)
	mdealingNum := len(c.MapDealingFileRecord)
	mdoneNum := len(c.MapDoneFileRecord)

	rpendingNum := len(c.ReducePendingTaskRecord)
	rdealingNum := len(c.ReduceDealingTaskRecord)
	rdoneNum := len(c.ReduceDoneTaskRecord)
	logger.Info.Printf("mpendingNum: %d, mdealingNum: %d, mdoneNum: %d, " +
		"rpendingNum: %d, rdealingNum: %d, rdoneNum: %d",
		mpendingNum, mdealingNum, mdoneNum,
		rpendingNum, rdealingNum, rdoneNum)
}

// checkState 用戶使用前需要先上锁，检查当前状态是否合法
func (c *Coordinator) checkState(flag int) {
	var (
		msg           string
		maprecords    int
		reducerecords int
	)

	mpendingNum := len(c.MapPendingFileRecord)
	mdealingNum := len(c.MapDealingFileRecord)
	mdoneNum := len(c.MapDoneFileRecord)

	rpendingNum := len(c.ReducePendingTaskRecord)
	rdealingNum := len(c.ReduceDealingTaskRecord)
	rdoneNum := len(c.ReduceDoneTaskRecord)


	switch flag {
	case InitState:
		if mpendingNum == c.MapTaskCount && mdealingNum == 0 && mdoneNum == 0 &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0 {
			goto CHECK2
		} else {
			msg = "InitState error"
			goto PANIC
		}
	case DealingMapState:
		if mpendingNum != 0 &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0 {
			goto CHECK2
		} else {
			msg = "DealingMapState error"
			goto PANIC
		}
	case WaitMapDoneState:
		if mpendingNum == 0 && mdealingNum != 0 &&
			rpendingNum == c.ReduceTaskCount && rdealingNum == 0 && rdoneNum == 0 {
			goto CHECK2
		} else {
			msg = "WaitMapDoneState error"
			goto PANIC
		}
	case DealingReduceState:
		if mpendingNum == 0 && mdealingNum == 0 && mdoneNum == c.MapTaskCount &&
			rpendingNum != 0  {
			goto CHECK2
		} else {
			msg = "DealingReduceState error"
			goto PANIC
		}
	case WaitReduceDoneState:
		if mpendingNum == 0 && mdealingNum == 0 && mdoneNum == c.MapTaskCount &&
			rpendingNum == 0 && rdealingNum != 0  {
			goto CHECK2
		} else {
			msg = "WaitReduceDoneState error"
			goto PANIC
		}
	case DoneState:
		if mpendingNum == 0 && mdealingNum == 0 && mdoneNum == c.MapTaskCount &&
			rpendingNum == 0 && rdealingNum == 0 && rdoneNum == c.ReduceTaskCount {
			goto CHECK2
		} else {
			msg = "InitState error"
			goto PANIC
		}
	}
CHECK2:
	// 记录的一致性检查，避免操作过程中，某个记录的丢失
	maprecords = len(c.MapDealingFileRecord) + len(c.MapPendingFileRecord) +
		len(c.MapDoneFileRecord)
	reducerecords = len(c.ReduceDealingTaskRecord) + len(c.ReducePendingTaskRecord) +
		len(c.ReduceDoneTaskRecord)
	if maprecords == c.MapTaskCount && reducerecords == c.ReduceTaskCount {
		return
	} else if maprecords > c.MapTaskCount || reducerecords > c.ReduceTaskCount {
		logger.Error.Printf("time out occur\n %v", c)
		return
	} else {
		msg = "Consistence error"
		goto PANIC
	}
	return
PANIC:
	logger.Info.Printf("mpendingNum: %d, mdealingNum: %d, mdoneNum: %d, " +
		"rpendingNum: %d, rdealingNum: %d, rdoneNum: %d",
		mpendingNum, mdealingNum, mdoneNum,
		rpendingNum, rdealingNum, rdoneNum)
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

// GetTask 获取待执行的 task，map or reduce or Done
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	//logger.Info.Printf("GetTask() be called...\n")
	for true {
		ok := func() bool {
			//logger.Info.Println("want to lock")
			c.Lock()
			defer c.Unlock()
			//defer logger.Info.Println("Unlock success")
			if c.getTask(reply) {
				return true
			}

			return false
		}()
		if ok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Coordinator) getTask(reply *GetTaskReply) bool {
	if len(c.MapPendingFileRecord) != 0 {
		c.checkState(DealingMapState)

		// 1. 如果还有 map 没有被调用
		var key string
		for k, _ := range c.MapPendingFileRecord {
			key = k
			break
		}

		InitMapTaskReply(reply, key, c.MapFileNameToID[key], c.ReduceTaskCount)
		delete(c.MapPendingFileRecord, key)
		c.MapDealingFileRecord[key] = true

		// TODO: 设置超时回调函数
		t := time.AfterFunc(10*time.Second, c.timerCB(MAP, c.MapFileNameToID[key]))
		c.TimerMgr[c.MapFileNameToID[key]] = t

		logger.Info.Printf("task: %v be requeire", reply)
		return true

	} else if len(c.MapDealingFileRecord) != 0 {
		c.checkState(WaitMapDoneState)

		// 2. 如果还有 map 没有被调用
		logger.Info.Print("wait until map tasks all done")
	} else if len(c.MapDoneFileRecord) == c.MapTaskCount &&
		len(c.ReducePendingTaskRecord) != 0 {
		c.checkState(DealingReduceState)

		// 3. 有未出的reduce，并且当前条件可以处理reduce了
		var key int
		for k, _ := range c.ReducePendingTaskRecord {
			key = k
			break
		}
		InitReduceTaskReply(reply, key, c.MapTaskCount)
		delete(c.ReducePendingTaskRecord, key)
		c.ReduceDealingTaskRecord[key] = true

		// TODO: 设置超时回调函数
		t := time.AfterFunc(10*time.Second, c.timerCB(REDUCE, key))
		c.TimerMgr[key] = t

		logger.Info.Printf("task: %v be requeire", reply)
		return true

	} else if len(c.ReduceDealingTaskRecord) != 0 {
		c.checkState(WaitReduceDoneState)

		// 4. 只有正在处理的reduce
		logger.Info.Print("wait until reduce tasks all done")

	} else if len(c.ReduceDoneTaskRecord) == c.ReduceTaskCount &&
		len(c.MapDoneFileRecord) == c.MapTaskCount {
		c.checkState(DoneState)

		// 5. map reduce done
		InitDoneTaskReply(reply)
		return true

	} else {
		logger.Panic.Panic("undefine")
	}
	return false
}

func (c *Coordinator) NoticeDone(args *NoticeArgs, reply *NoticeReply) error {
	//logger.Info.Println("want to lock")
	c.Lock()
	defer c.Unlock()
	//defer logger.Info.Println("Unlock success")
	if args.TaskType == MAP {
		delete(c.MapDealingFileRecord, c.MapIDToFileName[args.ID])
		delete(c.MapPendingFileRecord, c.MapIDToFileName[args.ID])
		c.MapDoneFileRecord[c.MapIDToFileName[args.ID]] = true
		c.TimerMgr[args.ID].Stop()

		logger.Info.Printf("map task: %v done", args.ID)
	} else if args.TaskType == REDUCE {
		delete(c.ReduceDealingTaskRecord, args.ID)
		delete(c.ReducePendingTaskRecord, args.ID)
		c.ReduceDoneTaskRecord[args.ID] = true
		c.TimerMgr[args.ID].Stop()

		logger.Info.Printf("reduce task: %v done", args.ID)
	} else {
		logger.Panic.Panic("undefine")
	}
	return nil
}

func (c *Coordinator) timerCB(taskType, id int) func() {
	return func() {
		//logger.Info.Println("want to lock")
		c.Lock()
		defer c.Unlock()
		//defer logger.Info.Println("Unlock success")
		if taskType == MAP {
			c.MapPendingFileRecord[c.MapIDToFileName[id]] = true

			logger.Warning.Printf("map task: %v time out", id)
		} else if taskType == REDUCE {
			c.ReducePendingTaskRecord[id] = true

			logger.Warning.Printf("reduce task: %v time out", id)
		} else {
			logger.Panic.Panic("undefine")
		}
		c.showSimplyState()
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
		logger.Panic.Panic("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	//logger.Info.Println("want to lock")
	c.Lock()
	defer c.Unlock()
	//defer logger.Info.Println("Unlock success")

	// Your code here.
	if len(c.MapDoneFileRecord) == c.MapTaskCount &&
		len(c.ReduceDoneTaskRecord) == c.ReduceTaskCount {
		ret = true
	}
	logger.Info.Println("Done tick...")

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

	c.MapTaskCount = len(files)
	c.ReduceTaskCount = nReduce

	c.MapPendingFileRecord = make(map[string]bool)
	c.MapDealingFileRecord = make(map[string]bool)
	c.MapDoneFileRecord = make(map[string]bool)

	c.MapIDToFileName = make(map[int]string)
	c.MapFileNameToID = make(map[string]int)
	c.ReducePendingTaskRecord = make(map[int]bool)
	c.ReduceDealingTaskRecord = make(map[int]bool)
	c.ReduceDoneTaskRecord = make(map[int]bool)
	c.TimerMgr = make(map[int]*time.Timer)

	for i, fn := range files {
		c.MapPendingFileRecord[fn] = true
		c.MapFileNameToID[fn] = i
		c.MapIDToFileName[i] = fn
	}
	for i := 0; i < nReduce; i++ {
		c.ReducePendingTaskRecord[i] = true
	}

	logger.Info.Printf("Init State as follower: \n%v", &c)
	c.server()
	return &c
}
