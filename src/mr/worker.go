package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use iHash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func iHash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string) {
	for {
		response := doHeartbeat()
		log.Printf("Worker: receive coordinator's heartbeat %v \n", response)
		switch response.JobType {
		case MapJob:
			doMapTask(mapF, response)
		case ReduceJob:
			doReduceTask(reduceF, response)
		case WaitJob:
			time.Sleep(1 * time.Second)
		case CompleteJob:
			return
		default:
			panic(fmt.Sprintf("unexpected jobType %v", response.JobType))
		}
	}
}

func doMapTask(mapF func(string, string) []KeyValue, response *HeartbeatResponse) {
	// 1. 读取文件
	fileName := response.FilePath
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	// 2. 执行map task
	kva := mapF(fileName, string(content))

	// 3. 采用hash分片生成中间文件数据，分片数为reduce是数量
	intermediates := make([][]KeyValue, response.NReduce)
	for _, kv := range kva {
		index := iHash(kv.Key) % response.NReduce
		intermediates[index] = append(intermediates[index], kv)
	}

	// 4. 中间文件并发落盘
	var wg sync.WaitGroup
	for index, intermediate := range intermediates {
		wg.Add(1)
		go func(index int, intermediate []KeyValue) {
			defer wg.Done()
			intermediateFilePath := generateMapResultFileName(response.Id, index)
			var buf bytes.Buffer
			enc := json.NewEncoder(&buf)
			for _, kv := range intermediate {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode json %v", kv.Key)
				}
			}
			atomicWriteFile(intermediateFilePath, &buf)
		}(index, intermediate)
	}

	// 5. 阻塞，等待执行完毕
	wg.Wait()
	// 6. 响应给master
	doReport(response.Id, MapPhase)
}

func doReduceTask(reduceF func(string, []string) string, response *HeartbeatResponse) {
	var kva []KeyValue
	// 1. 打开reduce task 的输入文件 -> 读取数据 -> 反序列化 -> append kva
	for i := 0; i < response.NMap; i++ {
		filePath := generateMapResultFileName(i, response.Id)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// 2. []KeyValue(kva) --merge--> map[string][]string(results)
	// map: key -> value list
	results := make(map[string][]string)
	// Maybe we need merge sort for larger data
	for _, kv := range kva {
		results[kv.Key] = append(results[kv.Key], kv.Value)
	}

	// 3. 对每个 key -> value list 执行reduce，并对结果进行落盘
	var buf bytes.Buffer
	for key, values := range results {
		output := reduceF(key, values)
		fmt.Fprintf(&buf, "%v %v\n", key, output)
	}
	atomicWriteFile(generateReduceResultFileName(response.Id), &buf)

	// 4. done
	doReport(response.Id, ReducePhase)
}

// doHeartbeat Worker给Master发送心跳包，并获取response，其中可能包含接下来需要进行的操作
func doHeartbeat() *HeartbeatResponse {
	response := HeartbeatResponse{}
	call("Coordinator.Heartbeat", &HeartbeatRequest{}, &response)
	return &response
}

// doReport 告知任务已经完成，phase  represent MAP or REDUCE or COMPLETE
func doReport(id int, phase SchedulePhase) {
	call("Coordinator.Report", &ReportRequest{id, phase}, &ReportResponse{})
}


// call 调用rpc，args 为函数参数，reply 为函数返回
func call(rpcName string, args interface{}, reply interface{}) bool {
	sockName := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
