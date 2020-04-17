package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"strings"
	"sort"
	"strconv"
)

const (
	tmpDir = "./tmp/"
	modePerm = 0755
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


type WK struct {
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
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

func open(filename string) *os.File {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v, %v", filename, err)
	}
	return file
}

func readall(file *os.File) []byte {
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", err)
	}
	return content
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WK{mapf, reducef, 0}

	os.Mkdir(tmpDir, modePerm)

	for {
		args := RPCArgs{}
		reply := RPCReply{}
		workerFetchTask(&args, &reply)
		w.nReduce = reply.NReduce
		workerProcessTask(&args, &reply, &w)
		args.N = reply.N
		args.Stage = reply.Stage
		workerFinishTask(&args, &reply)
		// fmt.Printf("Worker %v | Finish %v\n", os.Getpid(), args.N)
	}
}

//
// the RPC argument and reply types are defined in rpc.go.
//
// make an RPC call to the master
//
func workerFetchTask(args *RPCArgs, reply *RPCReply) {
	for {
		call("Master.FetchTask", &args, &reply)
		if reply.N != -1 {
			break
		}
		time.Sleep(time.Second)
	}
	// fmt.Printf("Worker %v | N %v S %v Stage %v\n", os.Getpid(), reply.N, reply.S, reply.Stage)
}

func workerFinishTask(args *RPCArgs, reply *RPCReply) {
	call("Master.FinishTask", &args, &reply)
}

func workerProcessTask(args *RPCArgs, reply *RPCReply, w *WK) {
	if reply.S != "" {
		workerMap(reply.N, reply.S, w)
	} else {
		workerReduce(reply.N, w)
	}
}

func MarshalAndWrite(data interface{}, f *os.File) {
	b, err := json.Marshal(data)
	if err != nil {
		log.Fatalf("marshal failed: %v", err)
	}
	f.Write(b)
}

func ReadAndUnMarshal(data interface{}, f *os.File) {
	b := readall(f)

	err := json.Unmarshal(b, data)
	if err != nil {
		log.Fatalf("unmarshal failed: %v", err)
	}
}

func workerMap(m int, filename string, w *WK) {

	intermediate := make(map[int][]KeyValue)
	
	// read the kvs from the file 
	file := open(filename)
	content := readall(file)
	file.Close()
	kva := w.mapf(filename, string(content))

	// partition the k-v pairs into different buckets by ihash(kv.Key) % NReduce
	for _, kv := range kva {
		kvs := intermediate[ihash(kv.Key) % w.nReduce]
		intermediate[ihash(kv.Key) % w.nReduce] = append(kvs, kv)
	}

	// marshal and write k-v pairs to tmp-M-N and rename to MR-M-N
	for n, kvs := range intermediate {
		tmpPrefix := "tmp-" + strconv.Itoa(m) + "-" + strconv.Itoa(n) + "-"
		f, err := ioutil.TempFile(tmpDir, tmpPrefix)
		if err != nil {
			log.Fatalf("cannot create tmp file of prefix %v", tmpPrefix)
		}

		MarshalAndWrite(kvs, f)
		f.Close()

		newName := tmpDir + "mr-" + strconv.Itoa(m) + "-" + strconv.Itoa(n)
		err = os.Rename(f.Name(), newName)
		// fmt.Println(f.Name(), "    ", newName)
		if err != nil {
			// fmt.Println(err)
			os.Remove(f.Name())
		}
	}

}

func workerReduce(n int, w *WK) {
	// read file
	files, err := ioutil.ReadDir(tmpDir)
	if err != nil {
		log.Fatalf("cannot read %v", tmpDir)
	}

	kvAll := []KeyValue{}

	// read intermedaite files
	for _, file := range files {
		filename := file.Name()
		if !(strings.HasPrefix(filename, "mr") && strings.HasSuffix(filename, strconv.Itoa(n))) {
			continue
		}

		kvPart := []KeyValue{}

		f := open(tmpDir + filename)
		ReadAndUnMarshal(&kvPart, f)
		kvAll = append(kvAll, kvPart...)
		f.Close()
	}

	sort.Sort(ByKey(kvAll))

	oname := "mr-out-" + strconv.Itoa(n)

	tmpPrefix := "tmp-" + strconv.Itoa(n) + "-"
	f, err := ioutil.TempFile(tmpDir, tmpPrefix)
	if err != nil {
		log.Fatalf("cannot create tmp file %v", tmpPrefix)
	}

	// write results to files
	for i := 0; i < len(kvAll); {
		j := i + 1
		for j < len(kvAll) && kvAll[j].Key == kvAll[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kvAll[k].Value)
		}
		output := w.reducef(kvAll[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(f, "%v %v\n", kvAll[i].Key, output)

		i = j
	}
	f.Close()
	err = os.Rename(f.Name(), oname)
	// fmt.Println(f.Name(), "    ", oname)
	if err != nil {
		// fmt.Println(err)
		os.Remove(f.Name())
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.Dial("unix", sockname)
	if err != nil {
		os.RemoveAll(tmpDir)
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
