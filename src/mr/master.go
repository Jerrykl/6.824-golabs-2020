package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"sync"
	"fmt"
)

const (
	MAP = iota
	REDUCE
	DONE
)

type taskList struct {
	tasks map[int]int
	mutex sync.Mutex
}

func (tl *taskList) printTaskList() {
	println("______________")
	for k, v := range tl.tasks {
		println(k, v)
	}
	println("______________")
}

func (tl *taskList) getTask() int {
	task_num, count := -1, 0x7fffffff
	for k, v := range tl.tasks {
		if v < count {
			count = v
			task_num = k
		}
	}
	// if count > 0 {
	// 	return -1
	// }
	if task_num != -1 {
		tl.tasks[task_num] += 1
	}
	// tl.printTaskList()
	return task_num
}

func (tl *taskList) deleteTask(n int) {
	_, ok := tl.tasks[n]
	if ok {
		delete(tl.tasks, n)
	} else {
		fmt.Printf("Task %v completed repeatedly: \n", n)
	}
}

func (tl *taskList) initTasks(l int) {
	for i := 0; i < l; i++ {
		tl.tasks[i] = 0
	}
}

type Master struct {
	tl taskList
	numToFile map[int]string
	nReduce int

	stage int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) FetchTask(args *RPCArgs, reply *RPCReply) error {
	m.tl.mutex.Lock()
	defer m.tl.mutex.Unlock()
	switch m.stage {
	case MAP:
		reply.N = m.tl.getTask()
		reply.S = m.numToFile[reply.N]
	case REDUCE:
		reply.N = m.tl.getTask()
		reply.S = ""
	case DONE:
		reply.N = -1
		return nil
	}
	reply.NReduce = m.nReduce
	reply.Stage = m.stage

	// if reply.N != -1 {
	// 	fmt.Printf("Master | reply.n %v reply.s %v\n", reply.N, reply.S)
	// }
	return nil
}

func (m *Master) FinishTask(args *RPCArgs, reply *RPCReply) error {
	if args.Stage != m.stage {
		return nil
	}
	m.tl.mutex.Lock()
	defer m.tl.mutex.Unlock()
	m.tl.deleteTask(args.N)

	if len(m.tl.tasks) == 0 {
		if m.stage == MAP {
			// refill the task list for REDUCE stage
			m.tl.initTasks(m.nReduce)
		}
		if m.stage != DONE {
			m.stage += 1
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpcs := rpc.NewServer()
	rpcs.Register(m)
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.stage == DONE {
		ret = true
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.tl = taskList{tasks: make(map[int]int)}
	m.numToFile = make(map[int]string)
	m.nReduce = nReduce
	m.stage = MAP

	for i, file := range files {
		m.numToFile[i] = file
	}

	// for k, v := range m.numToFile {
	// 	println(k, v)
	// }

	m.tl.initTasks(len(files))

	m.server()
	return &m
}
