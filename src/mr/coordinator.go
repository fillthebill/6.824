package mr


import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strconv"
import "sync"
import "time"


type InProgressTaskInfo struct {
// endtime

	TaskAssigned MRTask
	StartTime time.Time
}

type Coordinator struct {
	TotalMapNumber int
	TotalReduceNumber int
	// Your definitions here.
	AllMapFinished bool
	AllReduceFinished bool

	UnassignedMapTasks map[MRTask]bool
	UnassignedReduceTasks map[MRTask]bool 
// 
	// NumOfMap/ReduceTasksUnfinished (either unassigned or inprogress)
	NumOfMapTasksUnfinished int
	NumOfReduceTasksUnfinished int

	InProgressMapTasks map[InProgressTaskInfo]bool
	InProgressReduceTasks map[InProgressTaskInfo]bool

	MuCoor sync.Mutex

}

func NewCoordinator(filesToMap []string, nReduce int) (*Coordinator) {
	TotalMapNumber := len(filesToMap)
	TotalReduceNumber := nReduce
	UnassignedMapTasks := make(map[MRTask]bool)
	UnassignedReduceTasks := make(map[MRTask]bool)

	for i := 0; i < TotalMapNumber; i++ {
		var maptask MRTask
		maptask.Tasktype = "map"
		maptask.MapTaskNum = i
		maptask.MapFileName = filesToMap[i]
		maptask.nReduce = 10 
		UnassignedMapTasks[maptask]= true
	}

	for j := 0; j < TotalReduceNumber; j++ {
		var reducetask MRTask
		reducetask.Tasktype = "reduce"
		reducetask.ReduceTaskNum = j
		reducetask.nReduce = nReduce 
		UnassignedReduceTasks[reducetask]= true
	}

	inpmt := make(map[InProgressTaskInfo]bool)
	inrmt := make(map[InProgressTaskInfo]bool)
	var mu sync.Mutex
	return &Coordinator{TotalMapNumber: TotalMapNumber,
	TotalReduceNumber: TotalReduceNumber,
	AllMapFinished: false,
	AllReduceFinished: false,
	UnassignedMapTasks: UnassignedMapTasks,
	UnassignedReduceTasks: UnassignedReduceTasks,
	NumOfMapTasksUnfinished: TotalMapNumber,
	NumOfReduceTasksUnfinished:TotalReduceNumber,
	InProgressMapTasks: inpmt,
	InProgressReduceTasks: inrmt,
	MuCoor: mu,}
}
// Your code here -- RPC handlers for the worker to call.
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func debug_coordinator(c *Coordinator) {

	fmt.Printf("NumUnfinishedMap is %d, UnfinishedReduce is %d \n",
	c.NumOfMapTasksUnfinished,
	c.NumOfReduceTasksUnfinished)

	fmt.Printf("AllMapFinished is %t, AllReduceFinished is %t \n",
	c.AllMapFinished,
	c.AllReduceFinished)
}

func  debug(info string) {
	fmt.Printf(info+"\n")
}


// should be called after lock has been taken
func RemoveUnfinishedToUnassigned(c *Coordinator, UnAssignedMapOrReduceTasks map[MRTask]bool, InProgressMRTasks map[InProgressTaskInfo]bool ) {

	for k, _ := range InProgressMRTasks {
		t := time.Now()
		if t.Sub(k.StartTime) > 10 {
			UnAssignedMapOrReduceTasks[k.TaskAssigned] = true
			delete(InProgressMRTasks, k)
		}
	}
}

func RenewInProgressTasks(c *Coordinator) {
	// take the mutex lock before calling this function
	// if maptasks have all finished, check inprogress reduce tasks
	if c.AllMapFinished == true {
	  RemoveUnfinishedToUnassigned(c, c.UnassignedReduceTasks, c.InProgressReduceTasks)
	}else {
	  RemoveUnfinishedToUnassigned(c, c.UnassignedMapTasks, c.InProgressMapTasks)
	}
	// else check inprogress map tasks only.
}


func AssignMapTaskOrWait(c *Coordinator) (r MRTask){
	var m MRTask
	if len(c.UnassignedMapTasks) == 0 {
		m.Tasktype = "wait"
	}else {
		for k, _ :=range c.UnassignedMapTasks {
			m = k
//			mnreduce := "nreduce of returned map is"
//			mnreduce += strconv.Itoa(m.nReduce)
//			debug(mnreduce)
			delete(c.UnassignedMapTasks, k)
			var taskinfo InProgressTaskInfo
			taskinfo.TaskAssigned = k
			taskinfo.StartTime = time.Now()
			c.InProgressMapTasks[taskinfo] = true
			return m 
		}
	}
	return m
}

func AssignReduceTaskOrWait(c *Coordinator) (r MRTask){
	var m MRTask
	if len(c.UnassignedReduceTasks) == 0 {
		m.Tasktype = "wait"
	}else {
		for k, _ :=range c.UnassignedReduceTasks {
			m = k
			delete(c.UnassignedReduceTasks, k)

			var taskinfo InProgressTaskInfo
			taskinfo.TaskAssigned = k
			taskinfo.StartTime = time.Now()
			c.InProgressReduceTasks[taskinfo] = true
			return m 
		}
	}
	return m 
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *MRTask) error {
	// represent what the requesting side should do using the 
	// Typetask field in struct MRTask 

	// ways of moving map/reduce task infomation
	// 1. when new map/reduce task is assigned, move unassigned map/reduce tasks to inprogress map/reduce tasks. Impl in AssignTask 
	// 2. when a task has run out of time, move inprogress map/reduce task to unassigned task.   Impl in RenewInProgressTasks()
	// 3. when a task finished, remove it from inprogress tasks, and change the counter for finished tasks. Impl in TaskFinished()
	c.MuCoor.Lock()
	RenewInProgressTasks(c)

	var r MRTask
	if c.AllMapFinished != true && c.AllReduceFinished != true {
		// move unassigned map tasks to inprogress map task
		// if all Map tasks has been assgined, return wait type task.
		r = AssignMapTaskOrWait(c)

	}else if c.AllMapFinished == true  && c.AllReduceFinished != true{
		// move unassigned reduce tasks to inprogress reduce tasks 
		r = AssignReduceTaskOrWait(c)
	}else if c.AllMapFinished == true && c.AllReduceFinished == true {
		r.Tasktype = "exit"
	}else {

		debug("assign task erro, reduce finished but not map")
		// report an error
	}
	fmt.Printf("task assgined %v \n", r)
	fmt.Printf("task in progress..%v \n", c.InProgressMapTasks)
	time.Sleep(1)
	*reply = r
	reply.MapFileName = r.MapFileName
	reply.MapTaskNum = r.MapTaskNum
	reply.nReduce= r.nReduce
	c.MuCoor.Unlock()
	return nil
}

func (c *Coordinator) TaskFinished(arg *MRTask, reply *MRTask) error {
// since coordinator is always alive
// when map/reduce is accomplished by worker,message is sent to coordinator to indicate such an event
// what if the worker is killed before message is received
// what's the temporal order of killing worker and changing filename atomically
// first changing the file name, then send the message
// even if process killed before sending message, 
	c.MuCoor.Lock()
	debug("taks done is ...")
	debug(arg.Tasktype)
	RemoveInProgressTask(c, arg)
	debug_coordinator(c)
	c.MuCoor.Unlock()
	return nil
}

func RemoveInProgressTask(c *Coordinator, TaskDone *MRTask) {

	if TaskDone.Tasktype == "map" {
		info := "size of inprogress map is .."
		info += strconv.Itoa(len(c.InProgressMapTasks))
		debug(info)

		for k, _ := range c.InProgressMapTasks {
			fmt.Printf("in progress content %v \n", k)
			fmt.Printf("for comparison, TaskkDOne content %v \n", TaskDone)
			if k.TaskAssigned.MapTaskNum == TaskDone.MapTaskNum {
				if k.TaskAssigned.MapFileName != TaskDone.MapFileName {
					// error
					debug("maptask info not match, Name for TaskDone is ")
					debug(TaskDone.MapFileName)
					debug("maptask info not match, Name for TaskAssigned is ")
					debug(k.TaskAssigned.MapFileName)
					log.Fatalf("not match!!!\n");
				}
				// delete
				delete(c.InProgressMapTasks, k)
				c.NumOfMapTasksUnfinished--
				info := "after change, num Map Unished is"
				info += strconv.Itoa(c.NumOfMapTasksUnfinished)
				debug(info)
				if c.NumOfMapTasksUnfinished == 0 {
					c.AllMapFinished = true
				}
			}
		}

	}else if TaskDone.Tasktype == "reduce" {
		for k, _ := range c.InProgressReduceTasks {
			if k.TaskAssigned.ReduceTaskNum == TaskDone.ReduceTaskNum {
				// delete
				delete(c.InProgressReduceTasks, k)
				c.NumOfReduceTasksUnfinished--
				if c.NumOfReduceTasksUnfinished == 0 {
					c.AllReduceFinished = true
				}
			}
		}

	}

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
	if c.AllMapFinished == true && c.AllReduceFinished == true {
	ret = true	
	}

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// init coordinator
	c := NewCoordinator(files, nReduce) // input for initiaton, numOfMap, numOfReduce Tasks.
	c.server()
	return c
}
