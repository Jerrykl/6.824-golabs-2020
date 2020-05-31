package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"
import "time"

const (
	ClientWaitInterval = 100
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64         // client id
	seq int64        // sequence number client request
	lastServer int   // cache the last leader
	count int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.lastServer = 0
	ck.seq = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	DPrintf("Client[%v][%v] Get %v", ck.id, ck.seq, key)
	t := time.Now()
	defer DPrintf("\tFinish Client[%v][%v][%v] Get %v Value", ck.id, ck.seq, ck.lastServer, key)
	args := GetArgs {
		Key: key,
		ClientID:  ck.id,
		Seq: ck.seq,
	}
	reply := GetReply {}

	ck.seq += 1

	for {
		ck.count += 1
		DPrintf("Client[%v] count %v", ck.id, ck.count)
		ok := ck.servers[ck.lastServer].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				DPrintf("Client[%v][%v] Get Total time: %v", ck.id, ck.seq, time.Since(t))
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
			}
		}
		time.Sleep(time.Duration(ClientWaitInterval) * time.Millisecond)
		ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	DPrintf("Client[%v][%v] %v Key: %v Value: %v", ck.id, ck.seq, op, key, value)
	t := time.Now()
	defer DPrintf("\tFinish Client[%v][%v][%v] %v Key: %v Value: %v", ck.id, ck.seq, ck.lastServer, op, key, value)
	args := PutAppendArgs {
		Key:    key,
		Value:  value,
		Op:     op,
		ClientID:     ck.id,
		Seq:    ck.seq,
	}
	reply := PutAppendReply {}

	ck.seq += 1

	for {
		ck.count += 1
		// DPrintf("Client[%v] count %v", ck.id, ck.count)
		ok := ck.servers[ck.lastServer].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				DPrintf("Client[%v][%v] Put Total time: %v count %v", ck.id, ck.seq, time.Since(t), ck.count)
				return
			case ErrWrongLeader:
			}
		}
		time.Sleep(time.Duration(ClientWaitInterval) * time.Millisecond)
		ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
