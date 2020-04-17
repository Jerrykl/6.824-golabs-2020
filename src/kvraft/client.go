package kvraft

import "../labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64         // client id
	seq int64        // sequence number client request
	lastServer int   // cache the last leader
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
	defer DPrintf("\tFinish Client[%v][%v] Get %v Value", ck.id, ck.seq, key)
	args := GetArgs {
		Key: key,
		ID:  ck.id,
		Seq: ck.seq,
	}
	reply := GetReply {}

	ck.seq += 1

	for {
		ok := ck.servers[ck.lastServer].Call("KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
			}
		}
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
	defer DPrintf("\tFinish Client[%v][%v] %v Key: %v Value: %v", ck.id, ck.seq, op, key, value)
	args := PutAppendArgs {
		Key:    key,
		Value:  value,
		Op:     op,
		ID:     ck.id,
		Seq:    ck.seq,
	}
	reply := PutAppendReply {}

	ck.seq += 1

	for {
		ok := ck.servers[ck.lastServer].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
			}
		}
		ck.lastServer = (ck.lastServer + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
