package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// N: task number
// S: file name (if has)
// Stage: MAP or REDUCE OR DONE
type RPCArgs struct {
	N int
	S string
	Stage int
}

// N: task number
// S: not used
// Stage: MAP or REDUCE OR DONE
// NReduce: number of reduce partitions
type RPCReply struct {
	N int
	S string
	Stage int
	NReduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
