package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

const (
	Pending = iota
	MapExecuting
	MapFinished
	MapFailed
	ReduceExecuting
	ReduceFinished
	ReduceFailed
)

const TimeOut = 10

type workerStat struct {
	name      string // worker name
	available bool   // whether or not this worker is functioning
}

type taskState struct {
	worker string // which worker runs this map task
	status int    // state of map task, like `MapExecuting`
}

type Coordinator struct {
	nMap, nReduce int

	// workers
	workerStats     []workerStat
	workerStatsLock sync.Mutex

	// map tasks
	mapTasks              []string
	mapTasksStats         []taskState
	mapTasksStatsLock     sync.Mutex
	isAllMapTasksFinished bool

	// reduce tasks
	reduceTasksStats         []taskState
	reduceTasksStatsLock     sync.Mutex
	isAllReduceTasksFinished bool
}

// Your code here -- RPC handlers for the worker to call.

// add record of workers to coordinator
func (c *Coordinator) RegisterWorker(name WorkerRegisterArgs, workerInfo *WorkerRegisterReply) error {
	if name == "" {
		workerInfo.Name = "worker-" + strconv.Itoa(workerInfo.ID)
	} else {
		workerInfo.Name = string(name)
	}
	workerInfo.NReduce = c.nReduce

	c.workerStatsLock.Lock()
	defer c.workerStatsLock.Unlock()

	workerInfo.ID = len(c.workerStats)
	newWorkerStat := workerStat{
		name:      workerInfo.Name,
		available: true,
	}
	c.workerStats = append(c.workerStats, newWorkerStat)

	return nil
}

// assign tasks for workers
// ID: worker ID
// filename: map task name(filename)
func (c *Coordinator) AssignMapTask(workerID int, reply *AssignMapTaskReply) (err error) {
	err = nil

	c.workerStatsLock.Lock()
	workerName := c.workerStats[workerID].name
	c.workerStatsLock.Unlock()

	c.mapTasksStatsLock.Lock()
	defer c.mapTasksStatsLock.Unlock()

	// go through map tasks state to find a map task that has not been assigned(state: `Pending`)
	// if found, then set `worker` attribute to the name of worker asking for a map task
	// and change `status` attribute from `Pending` to `MapExecuting`
	for ID, state := range c.mapTasksStats {
		if state.status == Pending {
			c.mapTasksStats[ID].status = MapExecuting
			c.mapTasksStats[ID].worker = workerName

			reply.ID = ID
			reply.Name = c.mapTasks[ID]
			return
		}
	}

	// if not found, set map task name to "", id to -1
	// which means there is no more available map tasks
	reply.ID = -1
	reply.Name = ""
	return
}

// update status of map tasks.
// if workers finished map tasks assigned to them
// they will use RPC to call this function to update the status of these map tasks
// tasks: name of finished map task(filename)
// updated: whether or not coordinator has been notified
func (c *Coordinator) UpdateMapTaskState(mapTaskID int, updated *bool) error {
	c.mapTasksStatsLock.Lock()
	defer c.mapTasksStatsLock.Unlock()

	c.mapTasksStats[mapTaskID].status = MapFinished
	c.mapTasksStats[mapTaskID].worker = ""

	*updated = true
	return nil
}

// check if all map tasks has been finished
func (c *Coordinator) CheckMapTasksFinished(arg int, reply *bool) (err error) {
	c.mapTasksStatsLock.Lock()
	defer c.mapTasksStatsLock.Unlock()

	if c.isAllMapTasksFinished {
		*reply = true
		return
	}

	for _, state := range c.mapTasksStats {
		if state.status != MapFinished {
			*reply = false
			return
		}
	}

	c.isAllMapTasksFinished = true
	*reply = true
	return
}

// assign tasks for reducers
func (c *Coordinator) AssignReduceTask(workerID int, reduceTaskID *int) error {
	c.workerStatsLock.Lock()
	workerName := c.workerStats[workerID].name
	c.workerStatsLock.Unlock()

	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	for index, state := range c.reduceTasksStats {
		if state.status == Pending {
			c.reduceTasksStats[index].worker = workerName
			c.reduceTasksStats[index].status = ReduceExecuting

			*reduceTaskID = index
			return nil
		}
	}

	*reduceTaskID = -1
	return nil
}

// update status of reduce tasks.
// if workers finished reduce tasks assigned to them
// they will use RPC to call this function to update the status of these reduce tasks
// reduceTaskID: ID of reduce task
// updated: whether or not coordinator has been notified
func (c *Coordinator) UpdateReduceTaskState(reduceTaskID int, updated *bool) error {
	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	c.reduceTasksStats[reduceTaskID].worker = ""
	c.reduceTasksStats[reduceTaskID].status = ReduceFinished

	*updated = true
	return nil
}

func (c *Coordinator) CheckReduceTasksFinished(arg int, reply *bool) error {
	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	if c.isAllReduceTasksFinished {
		*reply = true
		return nil
	}

	for _, state := range c.reduceTasksStats {
		if state.status != ReduceFinished {
			*reply = false
			return nil
		}
	}

	c.isAllReduceTasksFinished = true
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	return c.isAllReduceTasksFinished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:             len(files),
		nReduce:          nReduce,
		mapTasks:         files,
		mapTasksStats:    make([]taskState, len(files)),
		reduceTasksStats: make([]taskState, nReduce),
	}

	// initialize state of map tasks
	for i := 0; i < c.nMap; i++ {
		c.mapTasksStats[i].status = Pending
	}

	// initialize state of reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasksStats[i].status = Pending
	}

	c.server()
	return &c
}
