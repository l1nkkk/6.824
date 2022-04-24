package mr

import (
	"encoding/json"
	"fmt"
	"github.com/l1nkkk/6.824/src/common/logger"
	"io/ioutil"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"K"`
	Value string `json:"V"`
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	logger.Info.Printf("Running In Worker \n")
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for true {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &args, &reply)

		if ok {
			logger.Info.Printf("call Coordinator.GetTask success, reply:%v", reply)
			if reply.Done == true {
				// 1. 已经没有task需要执行
				return
			} else if reply.TaskType == MAP {
				dealMapTask(mapf, &reply)
				goto TASKDONE
			} else if reply.TaskType ==REDUCE{
				dealReduceTask(reducef, &reply)
				goto TASKDONE
			}else{
				logger.Panic.Panic("undefine TaskType")
			}
		} else {
			logger.Panic.Panic("call Coordinator.GetTask error")
		}
	TASKDONE:
		{
			argsNotice := NoticeArgs{}
			argsNotice.ID = reply.ID
			argsNotice.TaskType = reply.TaskType
			replyNotice := NoticeReply{}
			ok := call("Coordinator.NoticeDone", &argsNotice, &replyNotice)
			if ok{
				if reply.TaskType == MAP{
					logger.Info.Printf("MAP task: <%d,%s> done", reply.ID, reply.MapInputName)
				}else{
					logger.Info.Printf("REDUCE task: <%d> done", reply.ID)
				}
			}else{
				logger.Panic.Panic("call Coordinator.NoticeDone error")
			}
		}
	}

}

func dealMapTask(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	var (
		intermediate []KeyValue
		shardIntermediate [][]KeyValue
		inputfileName string
		inputfile *os.File
		content []byte
		err error
		ofiles []*os.File
		wdata []byte
	)

	/// 1. 读取输入文件，并将其内容传入 map function 中，获取输出
	inputfileName = reply.MapInputName
	if inputfile, err = os.Open(inputfileName); err != nil {
		logger.Panic.Panic("cannot open %v", inputfileName)
	}
	if content, err = ioutil.ReadAll(inputfile); err != nil {
		log.Fatalf("cannot read %v", inputfileName)
	}
	inputfile.Close()

	intermediate = mapf(inputfileName, string(content))

	// 2. 打开 nReduce 个文件

	defer func() {
		for _, f := range ofiles {
			f.Close()
		}
	}()
	for i := 0; i < int(reply.ReduceCount); i++ {
		oname := "mr-" + fmt.Sprintf("%d", reply.ID) + "-" + fmt.Sprintf("%d", i)
		if ofile, err := os.Create(oname); err != nil {
			panic("open file error")
		} else {
			ofiles = append(ofiles, ofile)
		}
	}

	// 3. shard  intermediate data into shardIntermediate
	shardIntermediate = make([][]KeyValue, reply.ReduceCount)
	for _, kv := range intermediate {
		shardIntermediate[ihash(kv.Key)%reply.ReduceCount] = append(
			shardIntermediate[ihash(kv.Key)%reply.ReduceCount],
			kv,
			)
	}

	// 4. Marshal and store the data
	for i, imd := range shardIntermediate{
		if wdata,err = json.Marshal(imd); err != nil{
			logger.Panic.Panic(err)
		}
		if _, err = ofiles[i].Write(wdata); err != nil{
			logger.Panic.Panic(err)
		}
	}

}

func dealReduceTask(reducef func(string, []string) string, reply *GetTaskReply) {
	var (
		intermediate []KeyValue
		tMediate []KeyValue
		tf           *os.File
		tdata        []byte
		err          error
	)

	// 1. 打开并读取所有该reduce应该处理的文件
	for i := 0; i < reply.MapCount; i++ {
		filename := "mr-" + fmt.Sprintf("%d", i) + "-" + fmt.Sprintf("%d", reply.ID)
		if tf, err = os.Open(filename); err != nil {
			logger.Panic.Println(err)
		}
		if tdata, err = ioutil.ReadAll(tf); err != nil{
			logger.Panic.Println(err)
		}

		// 1-1 json decode
		if err = json.Unmarshal(tdata, &tMediate); err != nil{
			logger.Panic.Println(err)
		}
		intermediate = append(intermediate, tMediate...)
	}

	// 2. 排序
	sort.Sort(ByKey(intermediate))

	// 3. reduce，并输出, steal code from mrsequential.go
	ofile, _ := os.Create("mr-out-"+fmt.Sprintf("%d", reply.ID))
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
