package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"


type Coordinator struct {
	// Your definitions here.
	isDone bool
	cLock sync.Mutex
	givenFiles []string
	coonReduce int
	mapFiles []string
	reduceFiles []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) populateVariables(files []string, nReduce int) {
	intendedMaps := 5
	c.givenFiles = files
	c.coonReduce = nReduce
	var mapfiles []string
	for i := 0 ; i < intendedMaps ; i++ {
		if i == (intendedMaps-1) {
			mapfiles = c.givenFiles[(i*len(c.givenFiles)/intendedMaps):len(c.givenFiles)]
		} else {
			mapfiles = c.givenFiles[(i*len(c.givenFiles)/intendedMaps):((i+1)*len(c.givenFiles)/intendedMaps)]
		}
	}
	c.mapFiles = mapfiles
	fmt.Printf("ppV: %v\n", c.mapFiles)
}

func (c *Coordinator) AssignTask(convey Convey, task *Task) error {
	task.Id = 1
	task.Assignment = "map"
	task.FileNames = c.mapFiles
	fmt.Printf("ID: %v\nAssignment: %v\nFiles: %v\n", task.Id, task.Assignment, task.FileNames)
	fmt.Printf("Convey Message: %v", convey.Message)
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
	ret := c.isDone

	// Your code here.


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
	c.populateVariables(files, nReduce)

	c.server()
	return &c
}
