package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// TODO: 1、有时候wc和index会报错；2、出现这个就必定出错，待检查dialing:dial unix /var/tmp/824-mr-1000: connect: connection refused
	for {
		args := Args{}
		reply := Reply{}
		args.WorkerId = os.Getpid()

		if ok := call("Coordinator.DistributeTask", &args, &reply); !ok {
			log.Fatal("rpc Coordinator.DistributeTask fails")
		}
		switch reply.TaskType {
		case MAP:
			{
				log.Printf("Worker %d recieves a map task %d", os.Getpid(), reply.MapIndex)
				content, err := os.ReadFile(reply.FileName)
				if err != nil {
					log.Fatalf("cannot create %v", reply.FileName)
				}
				kva := mapf(reply.FileName, string(content))

				args.MapIndex = reply.MapIndex
				args.TaskType = MAP
				args.WorkerId = os.Getpid()
				reply.TaskStatus = false

				if call("Coordinator.CompleteTask", &args, &reply) && reply.TaskStatus {
					var intermediateFiles []*os.File
					var encoders []*json.Encoder
					for i := 0; i < reply.NReduce; i += 1 {
						tmpFileName := fmt.Sprintf("pid-%d-mr-%d-%d", os.Getpid(), reply.MapIndex, i)
						intermediateFile, err := os.Create(tmpFileName)
						if err != nil {
							log.Fatalf("cannot create %v", tmpFileName)
						}
						intermediateFiles = append(intermediateFiles, intermediateFile)

						encoder := json.NewEncoder(intermediateFile)
						encoders = append(encoders, encoder)
					}

					for _, kv := range kva {
						err := encoders[ihash(kv.Key)%reply.NReduce].Encode(&kv)
						if err != nil {
							log.Fatalf("encoder fails")
						}
					}

					for i := 0; i < reply.NReduce; i += 1 {
						intermediateFiles[i].Close()
					}

					for i := 0; i < reply.NReduce; i += 1 {
						tmpFileName := fmt.Sprintf("pid-%d-mr-%d-%d", os.Getpid(), reply.MapIndex, i)
						intermediateFileName := fmt.Sprintf("mr-%d-%d", reply.MapIndex, i)
						err := os.Rename(tmpFileName, intermediateFileName)
						if err != nil {
							log.Fatalf("cannot rename %v", tmpFileName)
						}
					}

				}
			}
		case REDUCE:
			{
				log.Printf("Worker %d recieves a reduce task %d", os.Getpid(), reply.ReduceIndex)
				var kva []KeyValue
				for i := 0; i < reply.NMap; i++ {
					intermediateFileName := fmt.Sprintf("mr-%d-%d", i, reply.ReduceIndex)
					intermediateFile, err := os.Open(intermediateFileName)
					if err != nil {
						log.Print(err)
						log.Fatalf("cannot read %v", intermediateFileName)
					}
					decoder := json.NewDecoder(intermediateFile)
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
					intermediateFile.Close()
				}

				sort.Sort(ByKey(kva))

				tmpFileName := fmt.Sprintf("pid-%d-mr-out-%d", os.Getpid(), reply.ReduceIndex)
				outputFile, err := os.Create(tmpFileName)
				if err != nil {
					log.Fatalf("cannot create %v", tmpFileName)
				}

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}

					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}
					value := reducef(kva[i].Key, values)
					fmt.Fprintf(outputFile, "%v %v\n", kva[i].Key, value)

					i = j
				}

				args.ReduceIndex = reply.ReduceIndex
				args.TaskType = REDUCE
				args.WorkerId = os.Getpid()
				reply.TaskStatus = false

				if call("Coordinator.CompleteTask", &args, &reply) && reply.TaskStatus {
					outputFile.Close()
					outputFileName := fmt.Sprintf("mr-out-%d", reply.ReduceIndex)
					err := os.Rename(tmpFileName, outputFileName)
					if err != nil {
						log.Fatalf("cannot rename %v", tmpFileName)
					}
				}

			}
		case WAIT:
			time.Sleep(5 * time.Second)
		default:
			return
		}

	}
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
