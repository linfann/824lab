package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		reply := getTask()
		switch reply.TaskState {
		case MapTask:
			//log.Println("get a map work")
			doMap(reply.FileName, reply.NReduce, reply.TaskId, mapf)
		case ReduceTask:
			//log.Println("get a reduce work")
			doReduce(reply.ReduceFiles, reply.TaskId, reducef)
		case WaitTask:
			log.Println("I should wait")
			time.Sleep(2 * time.Second)
		case DoneTask:
			log.Println("bye bye")
			return
		default:
			panic(fmt.Sprintf("Invalid TaskTyte: %v", reply.TaskState))
		}
	}
}

func doMap(fileName string, nReduce int, taskId int, mapf func(string, string) []KeyValue) {
	//log.Printf("the task: %v", fileName)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		//log.Fatalf(fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatal("Fail to read file: "+fileName, err)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	buf := make([][]KeyValue, nReduce)
	//分槽存储临时数据
	for _, kv := range kva {
		slot := ihash(kv.Key) % nReduce
		buf[slot] = append(buf[slot], kv)
	}
	//创建每个reduce对应的文件
	fileNames := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		outName := fmt.Sprintf("mout-%d-%d", taskId, i)
		fileNames[i] = outName
		tempFile, err := ioutil.TempFile("", outName)
		if err != nil {
			log.Fatal("map temp file err: "+outName, err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range buf[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), outName)
	}
	//发送任务完成信息
	// declare an argument structure.
	args := TaskDoneArgs{
		TaskType: MapTask,
		FileName: fileNames,
		TaskId:   taskId,
	}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskDoneReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.WorkerTaskDone", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("map task sucess!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func doReduce(reduceFiles []string, taskId int, reducef func(string, []string) string) {
	data := make([]KeyValue, 0)
	for _, file := range reduceFiles {
		//fmt.Println("reduce need read from: ", file)
		data = append(data, readFromIntermediate(file)...)
	}
	oname := fmt.Sprintf("mr-out-%v", taskId)
	//log.Printf("ready to create tmpfile")
	tempfile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		log.Fatal("cannot create tempfile for %v\n", oname)
	}
	//log.Printf("create tmpfile ok, ready to sort")
	sort.Sort(ByKey(data))
	//log.Printf("sort ok")
	i := 0
	for i < len(data) {
		j := i + 1
		for j < len(data) && data[j].Key == data[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, data[k].Value)
		}
		output := reducef(data[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", data[i].Key, output)
		i = j
	}
	err = os.Rename(tempfile.Name(), oname)
	if err != nil {
		log.Fatal("rename tempfile failed for %v\n", oname)
	}

	//log.Println("ready to say done")
	//发送任务完成信息
	// declare an argument structure.
	args := TaskDoneArgs{
		TaskType: ReduceTask,
		TaskId:   taskId,
	}

	// fill in the argument(s).

	// declare a reply structure.
	reply := TaskDoneReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.WorkerTaskDone", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reduce task sucess!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

func readFromIntermediate(fileName string) []KeyValue {
	kva := make([]KeyValue, 0)
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("file open err", err)
		return nil
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
	return kva
}

func getTask() AcTaskReply {
	args := AcTaskArgs{}

	reply := AcTaskReply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply: %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	//log.Println("get reply: ", reply)
	return reply
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
