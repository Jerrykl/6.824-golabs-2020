package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
	// "fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	ResCheckNum  int = 100
	ResCheckInterval = 10 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string
	Key      string
    Value    string
    ClientID int64
    Seq      int64
}

type Result struct {
	Seq   int64
	HasKey bool
	Value string
}

type Reply struct {
	Err Err
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// Your definitions here.
	stopCh chan struct{}

	db       map[string]string
	resTable map[int64]Result   // talbe for clients' results (duplicate table)
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) applyOp(op Op) {

	if res, ok := kv.resTable[op.ClientID]; ok {
		if res.Seq >= op.Seq {
			return
		}
	}

	res := Result {
		Seq: op.Seq,
	}

	switch op.OpType {
	case "Get":
		res.Value, res.HasKey = kv.db[op.Key]
	case "Put":
		kv.db[op.Key] = op.Value
	case "Append":
		kv.db[op.Key] += op.Value
		// DPrintf("Server[%v] Append Key %v Value %v", kv.me, op.Key, kv.db[op.Key])
	}

	kv.resTable[op.ClientID] = res
}

func (kv *KVServer) applyCommand() {

	for {
		select {
		case <- kv.stopCh:
			return
	    case applyMsg := <- kv.applyCh:

			if !applyMsg.CommandValid {
				DPrintf("[%v] get snapshot", kv.me)
				kv.applySnapshot()
				break
			}

			kv.lock()

			if op, ok := applyMsg.Command.(Op); ok {
				kv.applyOp(op)
			}

			kv.saveSnapshot(applyMsg.CommandIndex)

			kv.unlock()
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op {
		OpType:   "Get",
		Key:      args.Key,
		Value:    "",
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	rep := kv.startOp(op)
	reply.Err, reply.Value = rep.Err, rep.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op {
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	rep := kv.startOp(op)
	reply.Err = rep.Err
}

func (kv *KVServer) checkResTable(id int64, seq int64, key string) (bool, bool, string) {
	kv.lock()
	defer kv.unlock()
	if res, ok := kv.resTable[id]; ok {
		if res.Seq == seq {
			return true, res.HasKey, res.Value
		}
	}
	return false, false, ""
}

func (kv *KVServer) startOp(op Op) (rep Reply) {

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("ErrWrongLeader [%v] %v %v", kv.me, op.OpType, op.Key)
		rep.Err = ErrWrongLeader
		return
	}

	DPrintf("startOp [%v] %v %v", kv.me, op.OpType, op.Key)

	for count := 0; count < ResCheckNum; count++ {
		if ok, hasKey, value := kv.checkResTable(op.ClientID, op.Seq, op.Key); ok {
			if op.OpType == "Get" && !hasKey {
				rep.Err = ErrNoKey
			} else {
				rep.Err = OK
			}
			DPrintf("finOp [%v] %v %v | %v", kv.me, op.OpType, op.Key, value)
			rep.Value = value
			return
		}
		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			rep.Err = ErrWrongLeader
			DPrintf("ErrWrongLeader [%v] %v %v", kv.me, op.OpType, op.Key)
			return
		}
		time.Sleep(ResCheckInterval)
	}
	// timeout
	DPrintf("Timeout [%v] %v %v", kv.me, op.OpType, op.Key)
	rep.Err = ErrWrongLeader
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("Server[%v] killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// =====================================================
//                  SNAPSHOT MODULE

func (kv *KVServer) saveSnapshot(snapshotIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}

	DPrintf("[%v] saveSnapshot snapshotIndex %v RaftStateSize %v maxraftstate %v", kv.me, snapshotIndex, kv.persister.RaftStateSize(), kv.maxraftstate)
	data := kv.genSnapshotData()
	kv.rf.PersistSnapshot(snapshotIndex, data)
	DPrintf("[%v] saveSnapshot SUCCESS size %v", kv.me, len(data))
}

func (kv *KVServer) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.db) != nil ||
		e.Encode(kv.resTable) != nil {
		panic("gen snapshot data encode err")
	}

	data := w.Bytes()
	return data
}

func (kv *KVServer) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var resTable map[int64]Result

	if d.Decode(&db) != nil ||
		d.Decode(&resTable) != nil {
		log.Fatalf("[%v] readSnapShot FAILED", kv.me)
	} else {
		kv.db = db
		kv.resTable = resTable
	}
}

func (kv *KVServer) applySnapshot() {
	kv.lock()
	defer kv.unlock()
	DPrintf("[%v] APPLY", kv.me)
	kv.readSnapShot(kv.persister.ReadSnapshot())
	DPrintf("[%v] APPLY SUCCESS", kv.me)
}

// =====================================================

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	DPrintf("server[%v] start maxraftstate %v", kv.me, maxraftstate)

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.resTable = make(map[int64]Result)
	kv.db = make(map[string]string)
	kv.readSnapShot(kv.persister.ReadSnapshot())

	go kv.applyCommand()

	return kv
}
