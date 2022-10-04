package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	//"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	isDone            bool
	retireWorkers     bool
	mapStatus         map[string]int
	mapJobId          int
	mapJobIdMapper    map[string]int
	reduceStatus      map[int]int
	iFileStatus       [][]int
	cLock             *sync.RWMutex
	givenFiles        []string
	coonReduce        int
	intermediateFiles map[int][]string
	mapChan           chan string
	reduceChan        chan int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
/*func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}*/

func (c *Coordinator) populateVariables(files []string, nReduce int) {
	c.givenFiles = files
	c.coonReduce = nReduce
	c.mapJobId = 0
	c.mapJobIdMapper = make(map[string]int)
	c.mapStatus = make(map[string]int)
	c.reduceStatus = make(map[int]int)
	c.intermediateFiles = make(map[int][]string)
	c.cLock = new(sync.RWMutex)
	for _, file := range c.givenFiles {
		c.mapStatus[file] = Unassigned
	}
	for i := 0; i < c.coonReduce; i++ {
		c.reduceStatus[i] = Unassigned
	}
	c.mapChan = make(chan string, 5)
	c.reduceChan = make(chan int, 5)
}

func (c *Coordinator) AssignTask(convey Communicate, jobReply *JobReply) error {
	c.cLock.RLock()
	if c.retireWorkers == true {
		c.cLock.RUnlock()
		jobReply.IsDone = true
		return nil
	}
	c.cLock.RUnlock()
	select {
	case file := <-c.mapChan:
		jobReply.Task = mapMess
		c.cLock.Lock()
		_, o := c.mapJobIdMapper[file]
		if !o {
			c.mapJobId += 1
			c.mapJobIdMapper[file] = c.mapJobId
		}
		c.cLock.Unlock()
		c.cLock.RLock()
		mj := MapJob{
			InputFile: file,
			MapJobNum: c.mapJobIdMapper[file],
			ReduceNum: c.coonReduce,
		}
		c.cLock.RUnlock()
		jobReply.Map = mj
		c.cLock.Lock()
		c.mapStatus[file] = Assined
		c.cLock.Unlock()
		go c.TimeTicker(jobReply.Task, mj.InputFile, -1)
		return nil
	case reduceNum := <-c.reduceChan:
		jobReply.Task = redMess
		c.cLock.RLock()
		rj := ReduceJob{
			IntermediateFiles: c.intermediateFiles[reduceNum],
			ReduceNum:         reduceNum,
		}
		c.cLock.RUnlock()
		jobReply.Reduce = rj
		c.cLock.Lock()
		c.reduceStatus[reduceNum] = Assined
		c.cLock.Unlock()
		go c.TimeTicker(jobReply.Task, "", rj.ReduceNum)
		return nil
	}
	return nil
}

func (c *Coordinator) MapJobDone(reply ReportMapJob, convey *Communicate) error {
	//fmt.Println("Job for file " + reply.InputFile + " Comleted")
	c.cLock.Lock()
	c.mapStatus[reply.InputFile] = Done
	for i, v := range reply.IntermediateFileMap {
		c.intermediateFiles[i] = append(c.intermediateFiles[i], v)
	}
	c.cLock.Unlock()
	//fmt.Println("Input File: " + reply.InputFile)
	return nil
}

func (c *Coordinator) ReduceJobDone(reply ReportReduceJob, convey *Communicate) error {
	//fmt.Println("Reduce Job for ID: " + strconv.Itoa(reply.ReduceNum) + " is Completed")
	c.cLock.Lock()
	c.reduceStatus[reply.ReduceNum] = Done
	c.cLock.Unlock()
	//fmt.Println("Status Updated")
	return nil
}

func (c *Coordinator) TimeTicker(job string, jobFile string, redNum int) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for true {
		select {
		case <-ticker.C:
			if job == mapMess {
				//fmt.Println("Failed to get the map job for File: " + jobFile)
				c.cLock.Lock()
				c.mapStatus[jobFile] = Unassigned
				c.cLock.Unlock()
				c.mapChan <- jobFile
				return
			} else if job == redMess {
				//fmt.Println("Failed to get the reduce job for Red Num: " + strconv.Itoa(redNum))
				c.cLock.Lock()
				c.reduceStatus[redNum] = Unassigned
				c.cLock.Unlock()
				c.reduceChan <- redNum
				return
			} else {
				fmt.Println("No valid job provided")
				return
			}
		default:
			switch job {
			case mapMess:
				c.cLock.RLock()
				if c.mapStatus[jobFile] == Done {
					c.cLock.RUnlock()
					return
				}
				c.cLock.RUnlock()
				//time.Sleep(time.Second)
			case redMess:
				c.cLock.RLock()
				if c.reduceStatus[redNum] == Done {
					c.cLock.RUnlock()
					return
				}
				c.cLock.RUnlock()
				//time.Sleep(time.Second)
			}
		}
	}
}

func (c *Coordinator) CheckMapStatus() bool {
	for _, file := range c.givenFiles {
		c.cLock.RLock()
		if c.mapStatus[file] != Done {
			c.cLock.RUnlock()
			return false
		}
		c.cLock.RUnlock()
	}
	return true
}

func (c *Coordinator) CheckReduceStatus() bool {
	for i, _ := range c.intermediateFiles {
		c.cLock.RLock()
		if c.reduceStatus[i] != Done {
			c.cLock.RUnlock()
			return false
		}
		c.cLock.RUnlock()
	}
	return true
}

func (c *Coordinator) MakeJobs() {
	for i, file := range c.givenFiles {
		c.cLock.RLock()
		if c.mapStatus[c.givenFiles[i]] == Unassigned {
			c.cLock.RUnlock()
			c.mapChan <- file
		}
		//time.Sleep(2 * time.Second)
	}

	for true {
		status := c.CheckMapStatus()
		if status {
			break
		}
	}

	//fmt.Println("All Map jobs Completed")

	for i, _ := range c.intermediateFiles {
		c.cLock.RLock()
		if c.reduceStatus[i] == Unassigned {
			c.cLock.RUnlock()
			c.reduceChan <- i
		}
		//time.Sleep(2 * time.Second)
	}

	for true {
		status := c.CheckReduceStatus()
		if status {
			break
		}
	}

	//fmt.Println("All Reduce jobs Completed")

	c.cLock.Lock()
	c.retireWorkers = true
	c.cLock.Unlock()

	time.Sleep(10 * time.Second)

	c.cLock.Lock()
	c.isDone = true
	c.cLock.Unlock()
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
	c.cLock.Lock()
	ret := c.isDone
	c.cLock.Unlock()

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.populateVariables(files, nReduce)

	go c.MakeJobs()
	c.server()
	return &c
}
