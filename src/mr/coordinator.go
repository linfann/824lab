package mr

import (
	"errors"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files     []string
	tmpFiles  [][]string
	nReduce   int
	tasks     []*Task
	state     TaskType
	mutex     sync.Mutex
	workerNum int
	doneNum   int
}

type Task struct {
	TaskState   TaskType
	fileName    string
	reduceFiles []string
	startTime   time.Time
	allocate    bool
	nReduce     int
	taskId      int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) AssignTask(args *AcTaskArgs, reply *AcTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.state == DoneTask {
		reply.TaskState = DoneTask
		return nil
	}
	for _, task := range c.tasks {
		if !task.allocate || task.TaskState != DoneTask && time.Now().After(task.startTime.Add(10*time.Second)) {
			task.startTime = time.Now()
			task.allocate = true
			//task.taskId = c.workerNum
			if c.state == MapTask {
				reply.TaskState = MapTask
				reply.TaskId = task.taskId
				reply.FileName = task.fileName
				reply.NReduce = c.nReduce

			} else {
				reply.TaskState = ReduceTask
				reply.TaskId = task.taskId
				reply.ReduceFiles = task.reduceFiles
				//reply.nReduce = c.nReduce
			}
			//c.workerNum++
			//log.Printf("Assign a task: %v\n", reply)
			return nil
		}
	}
	reply.TaskState = WaitTask
	return nil
}

func (c *Coordinator) WorkerTaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if args.TaskType == MapTask {
		if c.state != MapTask {
			log.Println("Coordinator is not in map period: ", args.TaskId)
			return nil
		}
		if c.tasks[args.TaskId].TaskState != DoneTask {
			c.tasks[args.TaskId].TaskState = DoneTask
			c.doneNum++
			for i := 0; i < c.nReduce; i++ {
				c.tmpFiles[i] = append(c.tmpFiles[i], args.FileName[i])
			}
			if c.doneNum == len(c.tasks) {
				//map阶段完成，进入reduce
				go c.turnToReduce()
			}
		}
		return nil
	} else if args.TaskType == ReduceTask {
		if c.state != ReduceTask {
			log.Fatal("Coordinator is not in reduce period" + " " + string(args.TaskId))
			return errors.New("coordinator is not in reduce period")
		}
		if c.tasks[args.TaskId].TaskState != DoneTask {
			c.tasks[args.TaskId].TaskState = DoneTask
			c.doneNum++
			if c.doneNum == len(c.tasks) {
				//map阶段完成，进入reduce
				c.state = DoneTask
			}
		}
		return nil
	}
	return nil
}

func (c *Coordinator) turnToReduce() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.state = ReduceTask
	reduceTasks := make([]*Task, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		reduceTasks[i] = &Task{
			TaskState:   ReduceTask,
			reduceFiles: c.tmpFiles[i],
			allocate:    false,
			taskId:      i,
		}
	}
	c.tasks = reduceTasks
	c.doneNum = 0
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	//ret := false

	// Your code here.

	return c.state == DoneTask
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	log.Println("start make Coordinator")
	// Your code here.
	c.state = MapTask
	c.files = files
	c.nReduce = nReduce
	c.tasks = make([]*Task, len(files))
	c.tmpFiles = make([][]string, nReduce)
	c.workerNum = 0
	for i, file := range files {
		c.tasks[i] = &Task{
			TaskState: MapTask,
			fileName:  file,
			nReduce:   c.nReduce,
			taskId:    i,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.tmpFiles[i] = make([]string, 0)
	}
	c.server()
	return &c
}
