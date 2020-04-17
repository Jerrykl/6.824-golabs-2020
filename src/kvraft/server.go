package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	RetryNum      int = 10
	RetryInterval int = 100
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
	seq   int64
	hasKey bool
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db       map[string]string
	reqTable map[int64]reqRes   // talbe for clients' requests (duplicate table)
}

func (kv *KVServer) update() {
	for applyMsg := range kv.applyCh {

		if !applyMsg.CommandValid {
			continue
		}
		op := applyMsg.Command.(Op)

		kv.mu.Lock()
		// DPrintf("[update] get lock")

		if req, ok := kv.reqTable[op.ClientID]; ok {
			if req.seq >= op.Seq {
				kv.mu.Unlock()
				continue
			}
		}

		req := reqRes {
			seq: op.Seq,
		}

		switch op.OpType {
		case GET:
			req.value, req.hasKey = kv.db[op.Key]
		case PUT:
			kv.db[op.Key] = op.Value
		case APPEND:
			kv.db[op.Key] += op.Value
			DPrintf("Server[%v] Append Key %v Value %v", kv.me, op.Key, kv.db[op.Key])
		}
		kv.reqTable[op.ClientID] = req

		// DPrintf("[update] release lock")
		kv.mu.Unlock()
	}
}

func (kv *KVServer) checkGetRes(args *GetArgs, reply *GetReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("****Server[%v] Get Client[%v][%v]", kv.me, args.ID, args.Seq)

	if req, ok := kv.reqTable[args.ID]; ok {
		if req.seq == args.Seq {
			if req.hasKey {
				reply.Err = OK
				reply.Value = req.value
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

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for commit
	for count := 0; count < RetryNum; count++ {
		if kv.checkGetRes(args, reply) {
			return
		}

		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			reply.Err = ErrWrongLeader
			return
		}

		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
	}
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) checkPutAppendRes(args *PutAppendArgs, reply *PutAppendReply) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// DPrintf("****Server[%v] %v Client[%v][%v]", kv.me, args.Op, args.ID, args.Seq)

	if req, ok := kv.reqTable[args.ID]; ok {
		if req.seq == args.Seq {
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

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// wait for commit
	for count := 0; count < RetryNum; count++ {
		if kv.checkPutAppendRes(args, reply) {
			return
		}

		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			reply.Err = ErrWrongLeader
			return
		}
		time.Sleep(time.Duration(RetryInterval) * time.Millisecond)
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
	close(kv.applyCh)
	DPrintf("Server[%v] killed", kv.me)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.reqTable = make(map[int64]reqRes)
	kv.db = make(map[string]string)
	go kv.update()

	return kv
}
