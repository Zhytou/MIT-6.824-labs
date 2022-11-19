package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	id        int
	workerId  int
	fileName  string
	taskType  int
	status    bool
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mtx            sync.Mutex
	inputFileNames []string
	nMap           int
	nReduce        int
	phase          int
	tasks          []Task
}

// Your code here -- RPC handlers for the worker to call.

const MAX_WAIT_TIME = time.Duration(15 * time.Second)

func (c *Coordinator) DistributeTask(args *Args, reply *Reply) error {
	c.mtx.Lock()
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.TaskStatus = false
	switch c.phase {
	case MAP:
		{
			allMapTaskStatus := true
			for i := 0; i < len(c.tasks); i += 1 {
				if !c.tasks[i].status {
					allMapTaskStatus = false
					if time.Since(c.tasks[i].startTime) > MAX_WAIT_TIME {
						c.tasks[i].workerId = args.WorkerId
						c.tasks[i].startTime = time.Now()

						reply.FileName = c.tasks[i].fileName
						reply.TaskType = MAP
						reply.MapIndex = c.tasks[i].id
						log.Printf("Redistribute a invalid map task %d to worker %d", reply.MapIndex, args.WorkerId)
						c.mtx.Unlock()
						return nil
					}
				}
			}
			if len(c.tasks) == c.nMap {
				if allMapTaskStatus {
					c.phase = REDUCE
				}
				reply.TaskType = WAIT
			} else {
				newTask := Task{}
				newTask.id = len(c.tasks)
				newTask.workerId = args.WorkerId
				newTask.fileName = c.inputFileNames[newTask.id]
				newTask.startTime = time.Now()
				newTask.taskType = MAP
				newTask.status = false
				c.tasks = append(c.tasks, newTask)

				reply.MapIndex = newTask.id
				reply.FileName = c.inputFileNames[newTask.id]
				reply.TaskType = MAP
				log.Printf("Distribute a new map task %d to worker %d", reply.MapIndex, args.WorkerId)

				c.mtx.Unlock()
				return nil
			}
		}

	case REDUCE:
		{
			allReduceTaskStatus := true
			for i := c.nMap; i < len(c.tasks); i += 1 {
				if c.tasks[i].taskType == REDUCE && !c.tasks[i].status {
					allReduceTaskStatus = false
					if time.Since(c.tasks[i].startTime) > MAX_WAIT_TIME {
						c.tasks[i].workerId = args.WorkerId
						c.tasks[i].startTime = time.Now()

						reply.TaskType = REDUCE
						reply.ReduceIndex = c.tasks[i].id
						log.Printf("Redistribute a invalid reduce task %d to worker %d", reply.ReduceIndex, args.WorkerId)
						c.mtx.Unlock()
						return nil
					}
				}
			}
			if len(c.tasks) == c.nMap+c.nReduce {
				if allReduceTaskStatus {
					c.phase = DONE
				}
				reply.TaskType = WAIT
			} else {
				newTask := Task{}
				newTask.id = len(c.tasks) - c.nMap
				newTask.workerId = args.WorkerId
				newTask.startTime = time.Now()
				newTask.taskType = REDUCE
				newTask.status = false
				c.tasks = append(c.tasks, newTask)

				reply.ReduceIndex = newTask.id
				reply.TaskType = REDUCE
				log.Printf("Distribute a new reduce task %d to worker %d", reply.ReduceIndex, args.WorkerId)

				c.mtx.Unlock()
				return nil
			}
		}
	default:
		reply.TaskType = DONE
	}
	c.mtx.Unlock()
	return nil
}

func (c *Coordinator) CompleteTask(args *Args, reply *Reply) error {
	c.mtx.Lock()
	switch args.TaskType {
	case MAP:
		if !c.tasks[args.MapIndex].status && time.Since(c.tasks[args.MapIndex].startTime) <= MAX_WAIT_TIME && c.tasks[args.MapIndex].workerId == args.WorkerId {
			c.tasks[args.MapIndex].status = true
			log.Printf("Worker %d completes MapTask %d", args.WorkerId, args.MapIndex)
			reply.TaskStatus = true
		} else {
			log.Printf("Worker %d fails MapTask %d", args.WorkerId, args.MapIndex)
		}
	case REDUCE:
		if !c.tasks[c.nMap+args.ReduceIndex].status && time.Since(c.tasks[c.nMap+args.ReduceIndex].startTime) <= MAX_WAIT_TIME && c.tasks[c.nMap+args.ReduceIndex].workerId == args.WorkerId {
			c.tasks[c.nMap+args.ReduceIndex].status = true
			log.Printf("Worker %d completes ReduceTask %d", args.WorkerId, args.ReduceIndex)
			reply.TaskStatus = true
		} else {
			log.Printf("Worker %d fails ReduceTask %d", args.WorkerId, args.ReduceIndex)
		}
	default:
	}
	c.mtx.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
	ret := false

	// Your code here.
	c.mtx.Lock()
	ret = c.phase == DONE
	c.mtx.Unlock()

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
	c.inputFileNames = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = MAP

	c.server()
	return &c
}
