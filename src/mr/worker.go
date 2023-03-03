package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type mapFuncType func(string, string) []KeyValue
type reduceFuncType func(string, []string) string

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doMap(mapf func(string, string) []KeyValue, workerInfo *WorkerRegisterReply) {
	var mapTaskInfo AssignMapTaskReply
	var mapTaskName string

	// ask coordinator for help until all tasks has been assigned
	for {
		call("Coordinator.AssignMapTask", workerInfo.ID, &mapTaskInfo)
		if mapTaskInfo.Name == "" || mapTaskInfo.ID == -1 {
			break
		}
		mapTaskName = mapTaskInfo.Name

		// used to store intermediate key-value pairs
		buckets := make([][]KeyValue, workerInfo.NReduce)

		file, err := os.Open(mapTaskName)
		if err != nil {
			log.Fatalf("open %v failed", mapTaskName)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("read %v failed", mapTaskName)
		}
		file.Close()

		kva := mapf(mapTaskName, string(content))
		for _, kv := range kva {
			bucketIdx := ihash(kv.Key) % workerInfo.NReduce
			buckets[bucketIdx] = append(buckets[bucketIdx], kv)
		}

		// write intermediate key-value pairs to disk
		// intermediate file format: json
		for i := 0; i < workerInfo.NReduce; i++ {
			file, err := os.CreateTemp(".", "*.json")
			if err != nil {
				log.Fatal("Failed to create temporary file")
			}

			sort.Sort(ByKey(buckets[i]))

			encoder := json.NewEncoder(file)
			for _, kv := range buckets[i] {
				encoder.Encode(&kv)
			}
			file.Close()
			os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d.json", mapTaskInfo.ID, i))
		}

		// inform the coordinator that map tasks in `mapTasks` has finished
		var informed bool
		for {
			call("Coordinator.UpdateMapTaskState", mapTaskInfo.ID, &informed)
			if informed {
				break
			}
		}
	}
}

// after all map tasks assigned by coordinator has been finished
// this worker should wait until all the other workers finish their map tasks
// then this worker can ask coordinator for reduce tasks
func waitToReduce() {
	var isAllMapTasksFinished bool
	for {
		call("Coordinator.CheckMapTasksFinished", 0, &isAllMapTasksFinished)
		if isAllMapTasksFinished {
			break
		}

		time.Sleep(time.Second)
	}
}

// find an intermediate file for reduce task with ID `reduceTaskID`
// filename like "mr-X-${reduceTaskID}.json"
func getIntmFileNames(reduceTaskID int) []string {
	files, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	suffix := strconv.Itoa(reduceTaskID) + ".json"
	var intmFiles []string
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) {
			intmFiles = append(intmFiles, file.Name())
		}
	}

	return intmFiles
}

func doReduce(reducef reduceFuncType, workerInfo *WorkerRegisterReply) {
	reduceTaskID := -1

	// ask coordinator for reduce task until all reduce tasks has been assigned
	for {
		call("Coordinator.AssignReduceTask", workerInfo.ID, &reduceTaskID)
		if reduceTaskID == -1 {
			break
		}

		// for each reduce task, read intermediate files whose name ends with `reduceTaskID`
		// and store key-value pairs to a slice `intmKVs`
		var intmKVs []KeyValue
		intmFilesNames := getIntmFileNames(reduceTaskID)

		for _, filename := range intmFilesNames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}

			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err = decoder.Decode(&kv); err != nil {
					break
				}
				intmKVs = append(intmKVs, kv)
			}

			file.Close()
			os.Remove(filename)
		}

		// create reduce output file
		reduceOutputFileName := "mr-out-" + strconv.Itoa(reduceTaskID)
		reduceOutputFile, err := os.Create(reduceOutputFileName)
		if err != nil {
			log.Fatal(err)
		}

		// sort values by key
		sort.Sort(ByKey(intmKVs))

		// collect values with same key together, call `reducef`
		// write result to output file
		i := 0
		for i < len(intmKVs) {
			key := intmKVs[i].Key

			j := i + 1
			for j < len(intmKVs) && intmKVs[j].Key == key {
				j++
			}

			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intmKVs[k].Value)
			}
			output := reducef(key, values)

			fmt.Fprintf(reduceOutputFile, "%v %v\n", key, output)
			i = j
		}
		reduceOutputFile.Close()

		// inform coordinator that this reduce task has finished
		var updated bool
		for {
			call("Coordinator.UpdateReduceTaskState", reduceTaskID, &updated)
			if updated {
				break
			}
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf mapFuncType, reducef reduceFuncType) {
	// first register this worker to coordinator
	var workerInfo WorkerRegisterReply
	call("Coordinator.RegisterWorker", "", &workerInfo)

	doMap(mapf, &workerInfo)
	waitToReduce()
	doReduce(reducef, &workerInfo)

	// continuously check if all reduce tasks has finished
	// if `isAllReduceTasksFinshed` is updated, or call failed(return false)
	// then break the loop to exit
	var isAllReduceTasksFinished bool
	for {
		callResult := call("Coordinator.CheckReduceTasksFinished", 0, &isAllReduceTasksFinished)
		if isAllReduceTasksFinished || !callResult {
			break
		}

		time.Sleep(time.Second)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
