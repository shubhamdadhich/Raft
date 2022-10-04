package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	/*workerMapJob := MapJob{}
	workerMapJob.InputFile = ""
	workerMapJob.MapJobNum = 0
	workerMapJob.ReduceNum = 0
	workerReduceJob := ReduceJob{}
	job.Map = &workerMapJob
	job.Reduce = &workerReduceJob*/
	getAssignment(mapf, reducef)
}

func getAssignment(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for true {
		convey := Communicate{}
		job := JobReply{}
		convey.Id = InitiateTask
		assignCall := call("Coordinator.AssignTask", convey, &job)
		if assignCall {
			if job.IsDone == true {
				//fmt.Println("Task Done")
				return
			}
			if job.Task == "" {
				//fmt.Println("No Job Found")
				time.Sleep(time.Second)
				continue
			}
			switch job.Task {
			case mapMess:
				//fmt.Println("Map Task")
				//fmt.Println("Task ID: ", job.Map.MapJobNum)
				//fmt.Println("Task Input File: ", job.Map.InputFile)
				intermediateFileNames := performMap(job.Map, mapf)
				reportMap := ReportMapJob{InputFile: job.Map.InputFile, IntermediateFileMap: intermediateFileNames}
				reportMapConv := Communicate{Id: FinishMap}
				reportMapCall := call("Coordinator.MapJobDone", reportMap, &reportMapConv)
				if !reportMapCall {
					fmt.Println("Report failed!")
				}
				//fmt.Println("map Job for file: " + job.Map.InputFile + " Completed!")
			case redMess:
				//fmt.Println("Reduce Task")
				//fmt.Println("Reduce Num: ", job.Reduce.ReduceNum)
				//fmt.Println("Reduce Intermediate Files: ", job.Reduce.IntermediateFiles)
				redret := performReduce(job.Reduce, reducef)
				if !redret {
					fmt.Println("Reduce Task Failed")
					return
				}
				reportReduce := ReportReduceJob{ReduceNum: job.Reduce.ReduceNum}
				reportReduceConv := Communicate{Id: FinishReduce}
				reportReduceCall := call("Coordinator.ReduceJobDone", reportReduce, &reportReduceConv)
				if !reportReduceCall {
					fmt.Printf("Report failed!")
				}
			}

		} else {
			fmt.Println("Call failed!")
		}
	}
}

func performMap(mapJob MapJob, mapf func(string, string) []KeyValue) map[int]string {
	filename := mapJob.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediateFileName := make(map[int]string)
	kvaParts := make([][]KeyValue, mapJob.ReduceNum)
	for _, kv := range kva {
		hashKeyMod := ihash(kv.Key) % mapJob.ReduceNum
		kvaParts[hashKeyMod] = append(kvaParts[hashKeyMod], kv)
	}

	for i := 0; i < mapJob.ReduceNum; i++ {
		intermediateFileName[i] = ("map-" + strconv.Itoa(mapJob.MapJobNum) + "-" + strconv.Itoa(i))
		iFileName := intermediateFileName[i]
		ifile, _ := os.Create(iFileName)
		defer ifile.Close()
		enc := json.NewEncoder(ifile)
		for _, kv := range kvaParts[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Failed to encode KV: ", kv)
				return nil
			}
		}
	}
	return intermediateFileName
}

func performReduce(reduceJob ReduceJob, reducef func(string, []string) string) bool {
	oname := "mr-out-" + strconv.Itoa(reduceJob.ReduceNum)
	intermediate := []KeyValue{}
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for _, iFileName := range reduceJob.IntermediateFiles {
		iFile, err := os.Open(iFileName)
		if err != nil {
			log.Fatalf("cannot open %v", iFile)
			return false
		}
		dec := json.NewDecoder(iFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		iFile.Close()
	}

	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*func CallExample() {

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
}*/

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
