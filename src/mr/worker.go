package mr

import "io/ioutil"
import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "time"
import "regexp"
import "sort"
import "encoding/json"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MRTask struct {
	// Tasktype could be: map, reduce, wait or exit
	Tasktype string
	MapFileName string
	MapTaskNum int
	ReduceTaskNum int
	nReduce int
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// as the first step in implementing mr, we implement map function first.
	// fetch a task. work according to task type

	// if rpc all fails, exit
	var assignedtask MRTask
	mock := ExampleArgs{}

	for {
		ok := call("Coordinator.AssignTask", mock, &assignedtask)
		if ok != true {
		  os.Exit(1)
		}
		if assignedtask.Tasktype == "map" || assignedtask.Tasktype == "reduce" {
		//debug("task type : " + assignedtask.Tasktype)

			if assignedtask.Tasktype == "map" {
				var fname string
				fname = "mapwork, filename is "
				fname += assignedtask.MapFileName
				fname += "nreduce is"
				fname += strconv.Itoa(assignedtask.nReduce)
				debug(fname)
				mapwork(mapf, assignedtask)
			} else {
				reducework(reducef, assignedtask)

			}

		}
		if assignedtask.Tasktype == "exit" {
				debug("task type : " + assignedtask.Tasktype)
			os.Exit(1)
		} else {
			if assignedtask.Tasktype != "wait" {
		//		debug("wierd task type : " + assignedtask.Tasktype)
			}
			time.Sleep(1)

		}

	}

}

func mapwork(mapf func(string, string) []KeyValue, Task MRTask) {
	nReduce := 10 
	//retrieve map result
	file, err := os.Open(Task.MapFileName)
	if err != nil {
		log.Fatalf("open file %v failed \n", Task.MapFileName);
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("read file %v failed \n", Task.MapFileName);
	}
	file.Close()
	kva := mapf(Task.MapFileName, string(content))
	// write into temp files, hashing int onRduce files.
	tempFilesNames := make([]string, nReduce)
	OpenedTempFiles := make([]*os.File, nReduce)
	encs := make([]*json.Encoder, nReduce)
	//FileNames := make([]string, nReduce)

// file name init, file creation
	for i := 0; i < nReduce; i++ {
		tempFilesNames[i] = "f"+ strconv.Itoa(Task.MapTaskNum) + strconv.Itoa(i)
		t,  _ := os.Create(tempFilesNames[i])
		OpenedTempFiles[i] = t
		encs[i] = json.NewEncoder(OpenedTempFiles[i])
	}

// kv pairs encoding
kvlen := len(kva)
	for j:=0; j < kvlen; j++ {
		n := ihash(kva[j].Key) % nReduce
		encs[n].Encode(&kva[j])
	}
//
	for i := 0; i < nReduce; i++ {
		mapname := "mr-"+strconv.Itoa(Task.MapTaskNum)+"-"+strconv.Itoa(i)
		os.Rename(tempFilesNames[i], mapname)
	}

	info := "before finishing..., task type is"
	info += Task.Tasktype
	info += strconv.Itoa(Task.MapTaskNum)
	debug(info)
	var reply MRTask
	ok := call("Coordinator.TaskFinished", &Task, &reply)
	if ok {
	//	fmt.Printf("finish task succeed!!\n");
	}else {
		fmt.Printf("finish task failed\n");
	}

}

func reducework(reducef func(string, []string) string, Task MRTask) {
	F2reduce := make(map[string]bool)
	fileNames, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("open cur dir failed\n")
	}
	var f2reg = "(mr-.-"
	f2reg += strconv.Itoa(Task.ReduceTaskNum)
	f2reg += ")"

	re := regexp.MustCompile(f2reg)
	for _, f := range fileNames {
		if re.MatchString(f.Name()) {
		  F2reduce[f.Name()] = true
		  fmt.Printf("name matched for reduce %v, \n", f.Name());
		}
	}
// write in temp file
	var tempname string
	tempname = "temp-out-"+ strconv.Itoa(Task.ReduceTaskNum)
	tf, _ := os.Create(tempname)
	for f, _ := range F2reduce {
		// open, decode, reduce, write
		fo, _ := os.Open(f)
		dec := json.NewDecoder(fo)
		var fcontent []KeyValue
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			fcontent = append(fcontent, kv)

		}
		sort.Sort(ByKey(fcontent))
		i := 0
		lenf := len(fcontent)
		for i < lenf {
			j := i+1
			var content4key []string
			for j < lenf && fcontent[j].Key == fcontent[i].Key {
				content4key = append(content4key,fcontent[j].Value )
				j++
			}
			output := reducef(fcontent[i].Key, content4key)
			fmt.Fprintf(tf, "%v, %v\n", fcontent[i].Key, output);
			i = j

		}
	}
// taskfinished

	var reply MRTask
	ok := call("Coordinator.TaskFinished", &Task, &reply)
	// change file names


	if ok {
		fmt.Printf("finish task succeed!!\n");
	}else {
		fmt.Printf("finish task failed\n");
	}

	var finalname string
	finalname = "mr-out-" + strconv.Itoa(Task.ReduceTaskNum)


	os.Rename(tempname, finalname)
// rename tempfile
}



//func (FileNum int, Task string) AskForTaskFromCoordinator() {


//}




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
