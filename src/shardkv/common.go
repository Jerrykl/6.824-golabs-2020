package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientID int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientID int64
	Seq      int64
}

type GetReply struct {
	Err   Err
	Value string
}

// migrate using PULL mode
// if a shard move from G1 to G2
// G2 would ask G1 for the shard by RPC call
type FetchShardArgs struct {
	Num   int // config number
	Shard int
}

type FetchShardReply struct {
	Success   bool
	ShardData ShardData
}

type ClearShardArgs struct {
	Num  int  // config number
	Shard int
}

type ClearShardReply struct {
	Success bool
}
