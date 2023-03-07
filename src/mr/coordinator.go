package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
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
	worker     string    // which worker runs this map task
	status     int       // state of map task, like `MapExecuting`
	assignTime time.Time // time coordinator assigned this task to a worker
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

func (c *Coordinator) findTimeOutTasks() {
	// first find timeout map tasks
	c.mapTasksStatsLock.Lock()
	if !c.isAllMapTasksFinished {
		for id, state := range c.mapTasksStats {
			if state.status == MapExecuting && int(time.Since(state.assignTime).Seconds()) >= TimeOut {
				c.mapTasksStats[id].status = MapFailed
				failedWorkerName := c.mapTasksStats[id].worker

				c.workerStatsLock.Lock()
				for workerID, workerStat := range c.workerStats {
					if workerStat.name == failedWorkerName {
						c.workerStats[workerID].available = false
					}
				}
				c.workerStatsLock.Unlock()
			}
		}
	}
	c.mapTasksStatsLock.Unlock()

	// second find timeout reduce tasks
	c.reduceTasksStatsLock.Lock()
	if !c.isAllReduceTasksFinished {
		for id, state := range c.reduceTasksStats {
			if state.status == ReduceExecuting && int(time.Since(state.assignTime).Seconds()) >= TimeOut {
				c.reduceTasksStats[id].status = ReduceFailed
				failedWorkerName := c.reduceTasksStats[id].worker

				c.workerStatsLock.Lock()
				for workerID, workerStat := range c.workerStats {
					if workerStat.name == failedWorkerName {
						c.workerStats[workerID].available = false
					}
				}
				c.workerStatsLock.Unlock()
			}
		}
	}
	c.reduceTasksStatsLock.Unlock()
}

// Your code here -- RPC handlers for the worker to call.

// add record of workers to coordinator
func (c *Coordinator) RegisterWorker(name WorkerRegisterArgs, workerInfo *WorkerRegisterReply) error {
	c.workerStatsLock.Lock()
	defer c.workerStatsLock.Unlock()

	workerInfo.ID = len(c.workerStats)
	if name == "" {
		workerInfo.Name = "worker-" + strconv.Itoa(workerInfo.ID)
	} else {
		workerInfo.Name = string(name)
	}
	workerInfo.NReduce = c.nReduce

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
	c.workerStatsLock.Lock()
	workerName := c.workerStats[workerID].name
	c.workerStatsLock.Unlock()

	c.mapTasksStatsLock.Lock()
	defer c.mapTasksStatsLock.Unlock()

	// could not assign map tasks to worker %s as all map tasks has been finished
	if c.isAllMapTasksFinished {
		reply.ID = -1
		reply.Name = ""
		return
	}

	// go through map tasks state to find a map task that has not been assigned(state: `Pending`)
	// if found, then set `worker` attribute to the name of worker asking for a map task
	// and change `status` attribute from `Pending` to `MapExecuting`
	for ID, state := range c.mapTasksStats {
		if state.status == Pending || state.status == MapFailed {
			c.mapTasksStats[ID].status = MapExecuting
			c.mapTasksStats[ID].worker = workerName
			c.mapTasksStats[ID].assignTime = time.Now()

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

	if c.mapTasksStats[mapTaskID].status != MapFailed {
		c.mapTasksStats[mapTaskID].status = MapFinished
		c.mapTasksStats[mapTaskID].worker = ""
		*updated = true
	} else {
		*updated = false
	}

	return nil
}

// check if all map tasks has been finished
func (c *Coordinator) CheckMapTasksFinished(arg int, reply *CheckTaskFinishedReply) (err error) {
	c.mapTasksStatsLock.Lock()
	defer c.mapTasksStatsLock.Unlock()

	if c.isAllMapTasksFinished {
		*reply = CheckTaskFinishedReply{
			AllFinished:   true,
			AnyTaskFailed: false,
		}
		return
	}

	for _, state := range c.mapTasksStats {
		if state.status == Pending || state.status == MapExecuting {
			*reply = CheckTaskFinishedReply{
				AllFinished:   false,
				AnyTaskFailed: false,
			}
			return
		}

		if state.status == MapFailed {
			*reply = CheckTaskFinishedReply{
				AllFinished:   false,
				AnyTaskFailed: true,
			}
			return
		}
	}

	*reply = CheckTaskFinishedReply{
		AllFinished:   true,
		AnyTaskFailed: false,
	}

	c.isAllMapTasksFinished = true
	return
}

// assign tasks for reducers
func (c *Coordinator) AssignReduceTask(workerID int, reduceTaskID *int) error {
	c.workerStatsLock.Lock()
	workerName := c.workerStats[workerID].name
	c.workerStatsLock.Unlock()

	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	// could not assign reduce tasks to worker as all reduce tasks has been finished
	if c.isAllReduceTasksFinished {
		*reduceTaskID = -1
		return nil
	}

	for index, state := range c.reduceTasksStats {
		if state.status == Pending || state.status == ReduceFailed {
			c.reduceTasksStats[index].worker = workerName
			c.reduceTasksStats[index].status = ReduceExecuting
			c.reduceTasksStats[index].assignTime = time.Now()

			*reduceTaskID = index
			return nil
		}
	}

	// could not assign reduce tasks to worker as all reduce tasks has been assigned
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

	if c.reduceTasksStats[reduceTaskID].status != ReduceFailed {
		c.reduceTasksStats[reduceTaskID].worker = ""
		c.reduceTasksStats[reduceTaskID].status = ReduceFinished
		*updated = true
	} else {
		*updated = false
	}

	return nil
}

func (c *Coordinator) CheckReduceTasksFinished(arg int, reply *CheckTaskFinishedReply) (err error) {
	c.reduceTasksStatsLock.Lock()
	defer c.reduceTasksStatsLock.Unlock()

	if c.isAllReduceTasksFinished {
		*reply = CheckTaskFinishedReply{
			AllFinished:   true,
			AnyTaskFailed: false,
		}
		return
	}

	for _, state := range c.reduceTasksStats {
		if state.status == Pending || state.status == ReduceExecuting {
			*reply = CheckTaskFinishedReply{
				AllFinished:   false,
				AnyTaskFailed: false,
			}
			return
		}

		if state.status == ReduceFailed {
			*reply = CheckTaskFinishedReply{
				AllFinished:   false,
				AnyTaskFailed: true,
			}
			return
		}
	}

	*reply = CheckTaskFinishedReply{
		AllFinished:   true,
		AnyTaskFailed: false,
	}

	c.isAllReduceTasksFinished = true
	return
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

	// setup a thread to continuously check if
	// any map or reduce tasks has failed(timeout)
	go func() {
		for {
			c.findTimeOutTasks()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
