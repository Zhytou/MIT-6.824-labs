package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
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

	args := Args{}
	reply := Reply{}

	for ok := call("Coordinator.DistributeTask", &args, &reply); ok; ok = call("Coordinator.DistributeTask", &args, &reply) {
		switch reply.TaskType {
		case MAP:
			{
				content, _ := os.ReadFile(reply.FileName)
				kva := mapf(reply.FileName, string(content))

				var intermediateFiles []*os.File

				for i := 0; i < reply.NReduce; i += 1 {
					intermediateFileName := "mr-" + strconv.Itoa(reply.MapIndex) + "-" + strconv.Itoa(i)
					intermediateFile, _ := os.OpenFile(intermediateFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0660)
					intermediateFiles = append(intermediateFiles, intermediateFile)
				}

				for _, kv := range kva {
					intermediateFile := intermediateFiles[ihash(kv.Key)%reply.NReduce]
					encoder := json.NewEncoder(intermediateFile)
					encoder.Encode(&kv)

				}

				for i := 0; i < reply.NReduce; i += 1 {
					defer intermediateFiles[i].Close()
				}

				args.MapIndex = reply.MapIndex
				args.TaskType = MAP
				call("Coordinator.CompleteTask", &args, &reply)

			}
		case REDUCE:
			{
				var kva []KeyValue
				for i := 0; i < reply.NMap; i += 1 {
					intermediateFileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceIndex)
					intermediateFile, _ := os.Open(intermediateFileName)

					decoder := json.NewDecoder(intermediateFile)
					for {
						var kv KeyValue
						if err := decoder.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}

					intermediateFile.Close()
					// os.Remove(intermediateFileName)
				}

				outputFileName := "mr-out-" + strconv.Itoa(reply.ReduceIndex)
				outputFile, _ := os.Create(outputFileName)

				sort.Sort(ByKey(kva))

				i := 0
				for i < len(kva) {
					j := i + 1
					for j < len(kva) && kva[j].Key == kva[i].Key {
						j++
					}

					key := kva[i].Key
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, kva[k].Value)
					}

					value := reducef(key, values)

					fmt.Fprintf(outputFile, "%v %v\n", key, value)

					i = j
				}

				outputFile.Close()

				args.ReduceIndex = reply.ReduceIndex
				args.TaskType = REDUCE
				call("Coordinator.CompleteTask", &args, &reply)
			}
		case WAIT:
			time.Sleep(time.Second)
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
