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
	TryNum      int = 100
	TryInterval int = 10
)

const (
	GET    int = 0
	PUT    int = 1
	APPEND int = 2
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   int
	Key      string
    Value    string
    ClientID int64
    Seq      int64
}

type reqRes struct {
	Seq   int64
	HasKey bool
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	cond    *sync.Cond // Cond for snapshot

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// Your definitions here.
	db       map[string]string
	reqTable map[int64]reqRes   // talbe for clients' requests (duplicate table)
	snapshotIndex int
}

func (kv *KVServer) update() {
	for applyMsg := range kv.applyCh {

		// DPrintf("server[%v] try get lock", kv.me)
		kv.mu.Lock()
		// DPrintf("server[%v] get lock", kv.me)

		if !applyMsg.CommandValid {
			DPrintf("server[%v] get snapshot", kv.me)
			kv.readSnapshot(kv.persister.ReadSnapshot())
			// DPrintf("server[%v] release lock", kv.me)
			kv.mu.Unlock()
			continue
		}

		op := applyMsg.Command.(Op)


		if req, ok := kv.reqTable[op.ClientID]; ok {
			if req.Seq >= op.Seq {
				// DPrintf("server[%v] release lock", kv.me)
				kv.mu.Unlock()
				continue
			}
		}

		req := reqRes {
			Seq: op.Seq,
		}

		switch op.OpType {
		case GET:
			req.Value, req.HasKey = kv.db[op.Key]
		case PUT:
			kv.db[op.Key] = op.Value
		case APPEND:
			kv.db[op.Key] += op.Value
			// DPrintf("Server[%v] Append Key %v Value %v", kv.me, op.Key, kv.db[op.Key])
		}
		kv.reqTable[op.ClientID] = req

		// DPrintf("=%v= Server[%v] apply ClientID %v Seq %v Key %v Value %v", applyMsg.CommandIndex, kv.me, op.ClientID, op.Seq, op.Key, kv.db[op.Key])

		kv.snapshotIndex = applyMsg.CommandIndex
		// DPrintf("server[%v] release lock", kv.me)
		kv.mu.Unlock()
		if kv.checkSnapshotCond() {
			kv.cond.Signal()
		}
		// kv.trySnapshot(applyMsg.CommandIndex)
	}
}

func (kv *KVServer) checkGetRes(args *GetArgs, reply *GetReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("****Server[%v] Get Client[%v][%v]", kv.me, args.ID, args.Seq)

	if req, ok := kv.reqTable[args.ID]; ok {
		if req.Seq == args.Seq {
			if req.HasKey {
				reply.Err = OK
				reply.Value = req.Value
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
			}
			// DPrintf("----Server[%v] Get Client[%v][%v]", kv.me, args.ID, req.seq)
			return true
		}
	}
	return false
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.checkGetRes(args, reply) {
		return
	}

	op := Op {
		OpType:   GET,
		Key:      args.Key,
		Value:    "",
		ClientID: args.ID,
		Seq:      args.Seq,
	}

	start := time.Now()
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Get Start %v \n Fin %v", start, time.Now())

	// wait for commit
	for count := 0; count < TryNum; count++ {
		if kv.checkGetRes(args, reply) {
			return
		}

		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			reply.Err = ErrWrongLeader
			return
		}

		time.Sleep(time.Duration(TryInterval) * time.Millisecond)
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) checkPutAppendRes(args *PutAppendArgs, reply *PutAppendReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("****Server[%v] %v Client[%v][%v]", kv.me, args.Op, args.ID, args.Seq)

	if req, ok := kv.reqTable[args.ID]; ok {
		if req.Seq == args.Seq {
			reply.Err = OK
			// DPrintf("----Server[%v] %v Client[%v][%v]", kv.me, args.Op, args.ID, req.seq)
			return true
		}
	}
	return false
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.checkPutAppendRes(args, reply) {
		return
	}

	op := Op {
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ID,
		Seq:      args.Seq,
	}

	switch args.Op {
	case "Put":
		op.OpType = PUT
	case "Append":
		op.OpType = APPEND
	default:
		DPrintf("Wrong op %v", op)
		return
	}

	start := time.Now()
	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("PutAppend Start %v \n Fin %v", start, time.Now())

	// wait for commit
	for count := 0; count < TryNum; count++ {
		if kv.checkPutAppendRes(args, reply) {
			return
		}

		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(time.Duration(TryInterval) * time.Millisecond)
	}
	reply.Err = ErrWrongLeader
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
	// lock and unlock just to make sure goroutine get kill signal
	kv.mu.Lock()
	kv.mu.Unlock()
	close(kv.applyCh)
	kv.cond.Signal()
	DPrintf("Server[%v] killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) checkSnapshotCond() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate
}

func (kv *KVServer) trySnapshot() {
	snapshotIndex := 0
	for {
		kv.mu.Lock()
		for {
			if kv.killed() {
				// DPrintf("Server[%v] goroutine exit", kv.me)
				kv.mu.Unlock()
				return
			}
			// snapshotIndex != kv.snapshotIndex in case of duplicate snapshot
			if (kv.checkSnapshotCond() && snapshotIndex != kv.snapshotIndex) {
				break
			} else {
				// DPrintf("Server[%v] goroutine Waiting", kv.me)
				kv.cond.Wait()
			}
		}
		// DPrintf("Server[%v] goroutine Awake", kv.me)

		snapshotIndex = kv.snapshotIndex

		DPrintf("Server[%v] trigger snapshot, index: %v, size: %v", kv.me, snapshotIndex, kv.persister.RaftStateSize())

		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.reqTable)
		data := w.Bytes()

		kv.mu.Unlock()

		kv.rf.PersistSnapshot(snapshotIndex, data)

		DPrintf("Server[%v] finish snapshot, index: %v, size: %v", kv.me, snapshotIndex, kv.persister.RaftStateSize())

	}
}

func (kv *KVServer) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db map[string]string
	var reqTable map[int64]reqRes


	if d.Decode(&db) != nil || d.Decode(&reqTable) != nil {
		log.Fatal("fail to restore persisted snapshot")
	} else {
		kv.db = db
		kv.reqTable = reqTable
	}
}

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
	DPrintf("maxraftstate %v", maxraftstate)

	// You may need initialization code here.

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.cond = sync.NewCond(&kv.mu)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.reqTable = make(map[int64]reqRes)
	kv.db = make(map[string]string)
	kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.update()
	go kv.trySnapshot()

	return kv
}
