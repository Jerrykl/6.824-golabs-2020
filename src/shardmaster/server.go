package shardmaster


import "../raft"
import "../labrpc"
import "sync"
import "../labgob"
import "log"
import "time"
import "fmt"

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

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	resTable map[int64]Result // talbe for clients latest request (duplicate table)
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Result struct {
	seq int64
	num int   // config num (only used for getting the latest one)
}

type Op struct {
	// Your data here.
	OpType   string
	Args     interface{}

	ClientID int64
	Seq      int64
}

func (sm *ShardMaster) lock() {
	sm.mu.Lock()
}

func (sm *ShardMaster) unlock() {
	sm.mu.Unlock()
}

func (sm *ShardMaster) checkResTable(id int64, seq int64) (bool, int) {
	sm.lock()
	defer sm.unlock()
	if res, ok := sm.resTable[id]; ok {
		if res.seq == seq {
			return true, res.num
		}
	}
	return false, -1
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op {
		OpType: "Join",
		Args: *args,
		ClientID: args.ClientID,
		Seq: args.Seq,
	}
	rep := sm.startOp(op)
	reply.WrongLeader, reply.Err = rep.WrongLeader, rep.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op {
		OpType: "Leave",
		Args: *args,
		ClientID: args.ClientID,
		Seq: args.Seq,

	}
	rep := sm.startOp(op)
	reply.WrongLeader, reply.Err = rep.WrongLeader, rep.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op {
		OpType: "Move",
		Args: *args,
		ClientID: args.ClientID,
		Seq: args.Seq,
	}
	rep := sm.startOp(op)
	reply.WrongLeader, reply.Err = rep.WrongLeader, rep.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op {
		OpType: "Query",
		Args: *args,
		ClientID: args.ClientID,
		Seq: args.Seq,
	}
	rep := sm.startOp(op)
	reply.WrongLeader, reply.Err, reply.Config = rep.WrongLeader, rep.Err, rep.Config
}

func (sm *ShardMaster) startOp(op Op) (rep Reply) {
	DPrintf("[%v] %v", sm.me, op.OpType)
	_, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		rep.Err = ErrWrongLeader
		rep.WrongLeader = true
		return
	}

	for count := 0; count < TryNum; count++ {
		if ok, num := sm.checkResTable(op.ClientID, op.Seq); ok {
			rep.Err = OK
			rep.WrongLeader = false
			if op.OpType == "Query" {
				rep.Config = sm.configs[num]
			}
			return rep
		}
		if currTerm, _ := sm.rf.GetState(); currTerm != term {
			break
		}
		time.Sleep(time.Duration(TryInterval) * time.Millisecond)
	}

	rep.Err = Timeout
	rep.WrongLeader = true
	return rep
}

func (sm *ShardMaster) getRealIdx(idx int) int {
	if idx < 0 || idx >= len(sm.configs) {
		return len(sm.configs)-1
	} else {
		return idx
	}
}

func printShards(shards [NShards]int) {
	for i := 0; i < NShards; i++ {
		fmt.Print(shards[i], " ")
	}
	fmt.Println()
}

func (sm *ShardMaster) rebalance(lastShards [NShards]int, newGroups map[int][]string) [NShards]int {
	var newShards [NShards]int

	if len(newGroups) == 0 {
		return newShards
	}

	// copy shards
	for i := 0; i < NShards; i++ {
		newShards[i] = lastShards[i]
	}

	avg := NShards / len(newGroups)
	if avg == 0 {
		avg = 1
	}

	var gids []int
	var reShards []int // shard to reschedule
	shardNums := make(map[int]int) // gid -> number of shards

	for gid, _ := range newGroups {
		shardNums[gid] = 0
		gids = append(gids, gid)
	}

	// init reshards
	for i := range lastShards {
		if (lastShards[i] == 0) {
			reShards = append(reShards, i)
		} else {
			if _, ok := shardNums[lastShards[i]]; ok {
				shardNums[lastShards[i]] += 1
				if (shardNums[lastShards[i]] > avg) {
					reShards = append(reShards, i)
				}	
			} else {
				reShards = append(reShards, i)
			}
		}
	}

	// schedule shards that each group has at least avg num shards
	for gid, n := range shardNums {
		for ; n < avg; n++ {
			if len(reShards) > 0 {
				newShards[reShards[0]] = gid
				reShards = reShards[1:]
			}
		}
	}

	// schedule the rest shards
	for i := range reShards {
		newShards[reShards[i]] = gids[i]
	}

	// printShards(newShards)
	return newShards
}

func (sm *ShardMaster) doJoin(args JoinArgs) {
	lastCfg := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)

	// copy groups
	for k, v := range lastCfg.Groups {
		newGroups[k] = v
	}
	for k, v := range args.Servers {
		newGroups[k] = v
	}

	newShards := sm.rebalance(lastCfg.Shards, newGroups)

	sm.configs = append(sm.configs, Config {
		Num: len(sm.configs),
		Shards:	newShards,
		Groups: newGroups,
	})
}

func (sm *ShardMaster) doLeave(args LeaveArgs) {
	lastCfg := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)
	// copy groups
	for k, v := range lastCfg.Groups {
		newGroups[k] = v
	}
	for _, k := range args.GIDs {
		delete(newGroups, k)
	}

	newShards := sm.rebalance(lastCfg.Shards, newGroups)

	sm.configs = append(sm.configs, Config {
		Num: len(sm.configs),
		Shards:	newShards,
		Groups: newGroups,
	})
}

func (sm *ShardMaster) doMove(args MoveArgs) {
	lastCfg := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)
	// copy groups
	for k, v := range lastCfg.Groups {
		newGroups[k] = v
	}
	var newShards [NShards]int
	for i := 0; i < NShards; i++ {
		newShards[i] = lastCfg.Shards[i]
	}
	newShards[args.Shard] = args.GID

	sm.configs = append(sm.configs, Config {
		Num: len(sm.configs),
		Shards:	newShards,
		Groups: newGroups,
	})
}

func (sm *ShardMaster) applyOp() {
	for applyMsg := range sm.applyCh {

		op := applyMsg.Command.(Op)

		sm.lock()

		if res, ok := sm.resTable[op.ClientID]; ok {
			if res.seq >= op.Seq {
				sm.unlock()
				continue
			}
		}

		res := Result {
			seq: op.Seq,
		}

		switch op.OpType {
		case "Join":
			args, ok := op.Args.(JoinArgs)
			if !ok {
				log.Fatal("Not JoinArgs")
			}
			sm.doJoin(args)

		case "Leave":
			args, ok := op.Args.(LeaveArgs)
			if !ok {
				log.Fatal("Not LeaveArgs")
			}
			sm.doLeave(args)

		case "Move":
			args, ok := op.Args.(MoveArgs)
			if !ok {
				log.Fatal("Not MoveArgs")
			}
			sm.doMove(args)

		case "Query":
			args, ok := op.Args.(QueryArgs)
			if !ok {
				log.Fatal("Not QueryArgs")
			}
			res.num = sm.getRealIdx(args.Num)
		}
		sm.resTable[op.ClientID] = res

		sm.unlock()
	}
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	DPrintf("[%v] shardmaster killed", sm.me)
	// Your code here, if desired.
	close(sm.applyCh)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// must register the following types which are used in Op
func registerAll() {
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})	
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	registerAll()
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.resTable = make(map[int64]Result)
	go sm.applyOp()

	DPrintf("[%v] shardmaster start", sm.me)
	return sm
}
