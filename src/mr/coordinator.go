package mr


import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"



type Coordinator struct {
	// Your definitions here.
	nextTaskNum int
	tasks [100]MrTask
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



func  debug(info string) {
	fmt.Printf(info+"\n");
}
func (c *Coordinator) AssignMapTask(args *ExampleArgs, reply *MrTask) error {
	if c.nextTaskNum >= len(c.tasks) {
	  fmt.Printf("no more tasks...\n" );
	}
	reply.Filename = c.tasks[c.nextTaskNum].Filename
	debug("filename is"+reply.Filename+"nextTaskNum is"+strconv.Itoa(c.nextTaskNum))
	c.nextTaskNum++

	
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
	ret := false

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
	ttasks := [100]MrTask{}
	c.tasks = ttasks 
	c.nextTaskNum = 0
	for i := 0; i < len(files); i++ {
		debug("file len is "+strconv.Itoa(len(files)))
		debug("i is"+strconv.Itoa(i)+"filename is"+files[i]+"\n")
		c.tasks[i].Filename = files[i]
		debug("i is"+strconv.Itoa(i)+"filename is"+c.tasks[i].Filename+"\n")
	}
	// Your code here.


	c.server()
	return &c
}
