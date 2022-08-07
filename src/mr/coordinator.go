package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

const (
	MaxTaskRunInterval = time.Second * 10
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

// A laziest, worker-stateless, channel-based implementation of Coordinator
type Coordinator struct {
	files   []string		// 文件名
	nReduce int				// reduce总数
	nMap    int				// map总数
	phase   SchedulePhase	// 当前状态
	tasks   []Task			// task集合

	heartbeatCh chan heartbeatMsg	// 心跳channel，worker触发rpc进行投递
	reportCh    chan reportMsg		// task 完成情况channel，worker执行完task调用rpc进行投递
	doneCh      chan struct{}		// 所有task完成，master对其进行投递
}

type heartbeatMsg struct {
	response *HeartbeatResponse
	ok       chan struct{}
}

type reportMsg struct {
	request *ReportRequest
	ok      chan struct{}
}

// the RPC argument and reply types are defined in rpc.go.
// Heartbeat rpc，异步被调用
func (c *Coordinator) Heartbeat(request *HeartbeatRequest, response *HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok	// l1nkkk，阻塞，实现得很优雅
	return nil
}

func (c *Coordinator) Report(request *ReportRequest, response *ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

// schedule coordinator 结构体的所有数据都在这个 goroutine 中去修改，从而避免了 data race 的问题
func (c *Coordinator) schedule() {

	// 1. 将 task 相关状态初始化为MAP mode
	c.initMapPhase()
	for {
		select {
		case msg := <-c.heartbeatCh:
			// 2-1. 如果当前master为CompletePhase状态，响应的JobType为CompleteJob
			if c.phase == CompletePhase {
				msg.response.JobType = CompleteJob
			} else if c.selectTask(msg.response) {
				// 2-2. 选择发送给Worker的Task，到了这里说明所有任务已经完成，切换状态机
				switch c.phase {
				case MapPhase:
					// a. 如果为 MapPhase，切换为 ReducePhase
					log.Printf("Coordinator: %v finished, start %v \n", MapPhase, ReducePhase)
					c.initReducePhase()
					c.selectTask(msg.response)
				case ReducePhase:
					// b. 如果为 ReducePhase 切换为 CompletePhase
					log.Printf("Coordinator: %v finished, Congratulations \n", ReducePhase)
					c.initCompletePhase()
					msg.response.JobType = CompleteJob
				case CompletePhase: // default instead of
					panic(fmt.Sprintf("Coordinator: enter unexpected branch"))
				}
			}
			log.Printf("Coordinator: assigned a task %v to worker \n", msg.response)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			// 2. 修改该task的状态为Finished
			if msg.request.Phase == c.phase {
				log.Printf("Coordinator: Worker has executed task %v \n", msg.request)
				c.tasks[msg.request.Id].status = Finished
			}
			msg.ok <- struct{}{}
		}
	}
}
// selectTask 选择交给worker执行的任务，Map or Reduce 通用，return true表示所有任务都完成了
func (c *Coordinator) selectTask(response *HeartbeatResponse) bool {
	allFinished, hasNewJob := true, false
	for id, task := range c.tasks {
		switch task.status {
		case Idle:
			// 1. 如果为 idel，则选择之，并修改状态为 Working
			allFinished, hasNewJob = false, true
			c.tasks[id].status, c.tasks[id].startTime = Working, time.Now()
			response.NReduce, response.Id = c.nReduce, id
			if c.phase == MapPhase {
				response.JobType, response.FilePath = MapJob, c.files[id]
			} else {
				response.JobType, response.NMap = ReduceJob, c.nMap
			}
		case Working:
			// 2. 如果为 working，如果超时，则选择之
			allFinished = false
			if time.Now().Sub(task.startTime) > MaxTaskRunInterval {
				hasNewJob = true
				c.tasks[id].startTime = time.Now()
				response.NReduce, response.Id = c.nReduce, id
				if c.phase == MapPhase {
					response.JobType, response.FilePath = MapJob, c.files[id]
				} else {
					response.JobType, response.NMap = ReduceJob, c.nMap
				}
			}
		case Finished:
			// 3. 如果为 finish，则 continue
		}
		if hasNewJob {
			break
		}
	}

	if !hasNewJob {
		response.JobType = WaitJob
	}
	return allFinished
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.tasks = make([]Task, len(c.files))
	for index, file := range c.files {
		c.tasks[index] = Task{
			fileName: file,
			id:       index,
			status:   Idle,
		}
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = ReducePhase
	c.tasks = make([]Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = Task{
			id:     i,
			status: Idle,
		}
	}
}

func (c *Coordinator) initCompletePhase() {
	c.phase = CompletePhase
	c.doneCh <- struct{}{}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	name := coordinatorSock()
	os.Remove(name)
	l, e := net.Listen("unix", name)
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
	<-c.doneCh
	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		nMap:        len(files),
		heartbeatCh: make(chan heartbeatMsg),
		reportCh:    make(chan reportMsg),
		doneCh:      make(chan struct{}, 1),
	}
	// 1. 启动rpc服务
	c.server()

	// 2. 启动master调度
	go c.schedule()
	return &c
}
