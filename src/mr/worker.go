package mr

import "io/ioutil"
import "os"
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}


type MrTask struct {
	Filename string
	Tasktype string
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
	reducef func(string, []string) string, num int) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.

	// as the first step in implementing mr, we implement map function first.

	var maptask MrTask 
	mock := ExampleArgs{}
	ok := call("Coordinator.AssignMapTask", mock, &maptask)
	if ok {
		fmt.Printf("map task fetched successfully, file name is %v\n", maptask.Filename)
	} else {
		fmt.Printf("map task fetching failed!\n")
	}


	tfile, err := os.Open(maptask.Filename)
	if err != nil{
		
		fmt.Printf("open the file to be mapped failed!\n")
	}
	content, err := ioutil.ReadAll(tfile)
	if err != nil {
		fmt.Printf("read content of the file to be mapped failed!\n")
	}

	mapresult := mapf(maptask.Filename, string(content)) 


	filenum := strconv.Itoa(num)
	if err != nil {
		debug("strconv failure")	
	}
	secname := fmt.Sprintf("%s%d", "my-out-", filenum)
	secfile, err := os.Create(secname) 
	if err != nil {
		debug("file creation failuer\n")
		log.Fatal(err)
	}
	for i :=0; i < len(mapresult); i++ {
		fmt.Fprintf(secfile, "%v %v\n", mapresult[i].Key, mapresult[i].Value)
	}
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
