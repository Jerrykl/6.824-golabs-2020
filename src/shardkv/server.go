package shardkv


import "../shardmaster"
import "../labrpc"
import "../raft"
import "sync"
import "../labgob"
import "time"
import "log"
import "fmt"
import "bytes"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// ClearShardWaitNum * ClearShardWaitInterval <= PullShardsInterval
const (
	PollConfigInterval = 100 * time.Millisecond
	PullShardsInterval = 200 * time.Millisecond

	ClearShardWaitNum  int = 10
	ClearShardWaitInterval = 20 * time.Millisecond

	ResCheckNum  int = 100
	ResCheckInterval = 10 * time.Millisecond
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string // "Get" "Put" "Append"
	Key      string
	Value    string

	ClientID int64
	Seq      int64
}

type Result struct {
	Seq    int64
	HasKey bool
	Value  string
}

type Reply struct {
	Err   Err
	Value string
}

// State
// "NONE":    not own this shard
// "FETCHED": shard has been fetched
// "CLR":     old shard has been cleared
type ShardState struct {
	Num   int
	State string
}

type ShardData struct {
	Num        int
	Data       map[string]string
	ResTable   map[int64]Result
}

type ShardInfo struct {
	Shard      int
	ShardState ShardState
	ShardData  ShardData
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	sm           *shardmaster.Clerk
	persister    *raft.Persister

	// Your definitions here.
	stopCh chan struct{}
	pullConfigTimer *time.Timer
	pullShardsTimer *time.Timer

	db [shardmaster.NShards]map[string]string
	resTable [shardmaster.NShards]map[int64]Result // talbe for clients latest request (duplicate table)

	shardsState [shardmaster.NShards]ShardState

	historyShards map[int]ShardData // shard -> ShardData

	config shardmaster.Config

	configsCache map[int]shardmaster.Config
}

func (kv *ShardKV) lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
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

func (kv *ShardKV) initShardsState() {
	kv.lock()
	defer kv.unlock()
	for i := 0; i < shardmaster.NShards; i++ {
		kv.shardsState[i] = ShardState {
			Num:   0,
			State: "NONE",
		}
	}
}

func (kv *ShardKV) initDB() {
	kv.lock()
	defer kv.unlock()
	for i := 0; i < shardmaster.NShards; i++ {
		kv.db[i] = make(map[string]string)
	}
}

func (kv *ShardKV) initResTable() {
	kv.lock()
	defer kv.unlock()
	for i := 0; i < shardmaster.NShards; i++ {
		kv.resTable[i] = make(map[int64]Result)
	}
}

func (kv *ShardKV) initConfig() {
	kv.lock()
	defer kv.unlock()
	kv.config.Num = 0
	for i := 0; i < shardmaster.NShards; i++ {
		kv.config.Shards[i] = 0
	}
}

func (kv *ShardKV) checkShardsState(key string) bool {
	kv.lock()
	defer kv.unlock()
	shard := key2shard(key)
	if kv.shardsState[shard].State == "NONE" || kv.shardsState[shard].Num != kv.config.Num {
		DPrintf("[%v][%v] check shard %v state %v num %v | config num %v", kv.gid, kv.me, shard, kv.shardsState[shard].State, kv.shardsState[shard].Num, kv.config.Num)
		return false
	}
	return true
}

func (kv *ShardKV) checkResTable(id int64, seq int64, key string) (bool, bool, string) {
	kv.lock()
	defer kv.unlock()
	shard := key2shard(key)
	if res, ok := kv.resTable[shard][id]; ok {
		if res.Seq == seq {
			return true, res.HasKey, res.Value
		}
	}
	return false, false, ""
}

func (kv *ShardKV) startOp(op Op) (rep Reply) {
	// _, isLeader := kv.rf.GetState()
	if !kv.checkShardsState(op.Key) {
		// DPrintf("ErrWrongLeader [%v][%v][%v] %v %v(%v)", kv.gid, kv.me, isLeader, op.OpType, op.Key, key2shard(op.Key))
		rep.Err = ErrWrongLeader
		return
	}

	_, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		DPrintf("ErrWrongLeader [%v][%v] %v %v", kv.gid, kv.me, op.OpType, op.Key)
		rep.Err = ErrWrongLeader
		return
	}

	DPrintf("startOp [%v][%v] %v %v", kv.gid, kv.me, op.OpType, op.Key)

	for count := 0; count < ResCheckNum; count++ {
		if ok, hasKey, value := kv.checkResTable(op.ClientID, op.Seq, op.Key); ok {
			if op.OpType == "Get" && !hasKey {
				rep.Err = ErrNoKey
			} else {
				rep.Err = OK
			}
			DPrintf("finOp [%v][%v] %v %v | %v", kv.gid, kv.me, op.OpType, op.Key, value)
			rep.Value = value
			return
		}
		if currTerm, _ := kv.rf.GetState(); currTerm != term {
			rep.Err = ErrWrongLeader
			DPrintf("ErrWrongLeader [%v][%v] %v %v", kv.gid, kv.me, op.OpType, op.Key)
			return
		}
		if !kv.checkShardsState(op.Key) {
			rep.Err = ErrWrongGroup
			DPrintf("ErrWrongGroup [%v][%v] %v %v", kv.gid, kv.me, op.OpType, op.Key)
			return
		}
		time.Sleep(ResCheckInterval)
	}
	// timeout
	DPrintf("Timeout [%v][%v] %v %v", kv.gid, kv.me, op.OpType, op.Key)
	rep.Err = ErrWrongGroup
	return
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("Server[%v] killed", kv.me)
	close(kv.stopCh)
}

func printShards(num int, shards [shardmaster.NShards]int) {
	fmt.Printf("%v | %v\n", num, shards)
}

func (kv *ShardKV) applyConfig(config shardmaster.Config) {

	if kv.config.Num >= config.Num {
		return
	}

	if kv.config.Num+1 != config.Num {
		log.Fatalf("Wrong config num %v %v", kv.config.Num, config.Num)
	}

	kv.config = config

	// printShards(config.Num, config.Shards)
	DPrintf("[%v][%v] new CONFIG %v | %v", kv.gid, kv.me, config.Num, kv.config.Num)
}

func (kv *ShardKV) applyClearShard(clearshard ClearShardArgs) {

	DPrintf("[%v][%v] applyClearShard shard %v num %v", kv.gid, kv.me, clearshard.Shard, clearshard.Num)
	if kv.historyShardsExist(clearshard.Num, clearshard.Shard) {
		delete(kv.historyShards, clearshard.Shard)
		DPrintf("[%v][%v] shard cleared %v", kv.gid, kv.me, clearshard.Shard)
	}
}

func (kv *ShardKV) applyShardInfo(shardinfo ShardInfo) {

	ss := kv.shardsState[shardinfo.Shard]

	DPrintf("[%v][%v] apply shardinfo shard %v curr %v(%v) next %v(%v)", kv.gid, kv.me, shardinfo.Shard, ss.State, ss.Num, shardinfo.ShardState.State, shardinfo.ShardState.Num)

	switch shardinfo.ShardState.State {
	case "NONE":
		if ss.State == "NONE" && ss.Num + 1 == shardinfo.ShardState.Num {
			break
		}

		if ss.Num + 1 != shardinfo.ShardState.Num || ss.State != "CLR" {
			DPrintf("NONE [%v][%v] shard %v  num (%v %v)", kv.gid, kv.me, shardinfo.Shard, ss.Num, shardinfo.ShardState.Num)
			return
		}
		// save shard data
		// no need to copy map because would not be used
		if kv.historyShardsExist(ss.Num, shardinfo.Shard) {
			log.Fatalf("[%v][%v] replace historyShards shard %v", kv.gid, kv.me, shardinfo.Shard)
		}
		DPrintf("[%v][%v] save historyShards shard %v num %v map %v %v", kv.gid, kv.me, shardinfo.Shard, ss.Num, kv.db[shardinfo.Shard], kv.resTable[shardinfo.Shard])
		kv.historyShards[shardinfo.Shard] = ShardData {
			Num:      ss.Num,
			Data:     kv.db[shardinfo.Shard],
			ResTable: kv.resTable[shardinfo.Shard],
		}
		kv.db[shardinfo.Shard] = make(map[string]string)
		kv.resTable[shardinfo.Shard] = make(map[int64]Result)

	case "FETCHED":
		if ss.Num + 1 != shardinfo.ShardState.Num || ss.State != "NONE" {
			DPrintf("FETCHED [%v][%v] shard %v  num (%v %v)", kv.gid, kv.me, shardinfo.Shard, ss.Num, shardinfo.ShardState.Num)
			return
		}
		if kv.historyShardsExist(ss.Num, shardinfo.Shard) {
			log.Fatalf("[%v][%v] still have historyShards shard %v", kv.gid, kv.me, shardinfo.Shard)
		}
		// apply shard data
		kv.db[shardinfo.Shard] = shardinfo.ShardData.Data
		kv.resTable[shardinfo.Shard] = shardinfo.ShardData.ResTable

	case "CLR":
		if ss.State == "CLR" && ss.Num + 1 == shardinfo.ShardState.Num {
			break
		}

		if !(ss.Num + 1 == shardinfo.ShardState.Num && ss.State == "NONE") && 
		   !(ss.Num == shardinfo.ShardState.Num && ss.State == "FETCHED") {
			DPrintf("CLR [%v][%v] shard %v curr %v next %v", kv.gid, kv.me, shardinfo.Shard, ss.State, shardinfo.ShardState.State)
			return
		}
	}

	// change state
	kv.shardsState[shardinfo.Shard] = shardinfo.ShardState

	DPrintf("[%v][%v] after apply shardinfo shard %v state %v", kv.gid, kv.me, shardinfo.Shard, kv.shardsState[shardinfo.Shard].State)
}

func (kv *ShardKV) applyOp(op Op) {

	DPrintf("[%v][%v] apply command %v", kv.gid, kv.me, op.OpType)

	shard := key2shard(op.Key)

	if res, ok := kv.resTable[shard][op.ClientID]; ok {
		if res.Seq >= op.Seq {
			return
		}
	}

	// checkShardsState
	ss := kv.shardsState[shard]
	if ss.State == "NONE" || ss.Num != kv.config.Num {
		return
	}

	res := Result {
		Seq: op.Seq,
	}

	switch op.OpType {
	case "Get":
		res.Value, res.HasKey = kv.db[shard][op.Key]
		DPrintf("[%v][%v] %v %v(%v) | %v", kv.gid, kv.me, op.OpType, op.Key, shard, res.Value)
	case "Put":
		kv.db[shard][op.Key] = op.Value
		DPrintf("[%v][%v] %v %v(%v) | %v", kv.gid, kv.me, op.OpType, op.Key, shard, op.Value)
	case "Append":
		kv.db[shard][op.Key] += op.Value
		DPrintf("[%v][%v] %v %v(%v) | %v | %v", kv.gid, kv.me, op.OpType, op.Key, shard, op.Value, kv.db[shard][op.Key])
	}

	kv.resTable[shard][op.ClientID] = res
}

func (kv *ShardKV) applyCommand() {

	for {
		select {
		case <- kv.stopCh:
			return
	    case applyMsg := <- kv.applyCh:

			if !applyMsg.CommandValid {
				DPrintf("[%v][%v] get snapshot %v", kv.gid, kv.me, applyMsg.CommandIndex)
				kv.applySnapshot()
				break
			}

			kv.lock()

			if config, ok := applyMsg.Command.(shardmaster.Config); ok {
				kv.applyConfig(config)
			} else if shardinfo, ok := applyMsg.Command.(ShardInfo); ok {
				kv.applyShardInfo(shardinfo)
			} else if clearshard, ok := applyMsg.Command.(ClearShardArgs); ok {
				kv.applyClearShard(clearshard)
			} else if op, ok := applyMsg.Command.(Op); ok {
				kv.applyOp(op)
			}

			kv.saveSnapshot(applyMsg.CommandIndex)

			kv.unlock()
		}	
	}
}

func (kv *ShardKV) getConfigNum() int {
	kv.lock()
	defer kv.unlock()
	return kv.config.Num
}

// 
// this function could not be merged into function pullShards()
// when merged into one fucntion could not pass the unreliable tests
// it seems that the servers isolated could not go forward
// I don't know why =-=
func (kv *ShardKV) pollConfig() {
	for {
		select {
		case <- kv.stopCh:
			return
		case <- kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PollConfigInterval)
				break
			}
			n := kv.getConfigNum()+1
			DPrintf("[%v][%v] pullConfigTimer triggered enter query %v", kv.gid, kv.me, n)
			config := kv.getConfig(n)

			if config.Num == kv.getConfigNum()+1 {
				// DPrintf("[%v][%v] start config %v enter", kv.gid, kv.me, config.Num)
				kv.rf.Start(config)
				// DPrintf("[%v][%v] start config %v exit", kv.gid, kv.me, config.Num)
			} else {
				DPrintf("[%v][%v] has newest config %v %v", kv.gid, kv.me, kv.getConfigNum(), config.Num)
			}
			DPrintf("[%v][%v] pullConfigTimer triggered exit", kv.gid, kv.me)
			kv.pullConfigTimer.Reset(PollConfigInterval)
		}
	}
}

func (kv *ShardKV) getConfig(num int) shardmaster.Config {
	kv.lock()
	defer kv.unlock()

	if num < 0 {
		num = 0
	}

	if config, ok := kv.configsCache[num]; ok {
		return config
	}
	config := kv.sm.Query(num)
	kv.configsCache[config.Num] = config
	return config
}

func (kv *ShardKV) getShardState(shard int) ShardState {
	kv.lock()
	defer kv.unlock()
	return kv.shardsState[shard]
}

func (kv *ShardKV) historyShardsExist(num int, shard int) bool {
	if _, ok := kv.historyShards[shard]; ok {
		if kv.historyShards[shard].Num == num {
			return true
		}
	}
	return false
}

func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) {
	kv.lock()
	defer kv.unlock()

	if kv.historyShardsExist(args.Num, args.Shard) {
		reply.Success = true
		reply.ShardData = kv.historyShards[args.Shard]
		DPrintf("[%v][%v] Num %v Shard %v fetched len %v %v", kv.gid, kv.me, args.Num, args.Shard, reply.ShardData.Data, reply.ShardData.ResTable)
		return
	}
	reply.Success = false
	// DPrintf("[%v] Num %v Shard %v fetch not found", kv.me, args.Num, args.Shard)
	DPrintf("[%v][%v] historyShards shard %v num %v not found", kv.gid, kv.me, args.Shard, args.Num)
}

func (kv *ShardKV) ClearShard(args *ClearShardArgs, reply *ClearShardReply) {
	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		reply.Success = false
		return
	}

	for i := 0; i < ClearShardWaitNum; i++ {
		kv.lock()
		exist := kv.historyShardsExist(args.Num, args.Shard)
		kv.unlock()
		if !exist {
			DPrintf("[%v][%v] num %v shard %v been cleared", kv.gid, kv.me, args.Num, args.Shard)
			reply.Success = true
			return
		}
		time.Sleep(ClearShardWaitInterval)
	}

	reply.Success = false
	DPrintf("[%v] Num %v Shard %v clear FAILED", kv.me, args.Num, args.Shard)
}

func (kv *ShardKV) fetchShard(shard int, num int) (ShardData, bool) {
	config := kv.getConfig(num)
	gid := config.Shards[shard]

	args := FetchShardArgs {
		Num:   num,
		Shard: shard,
	}

	if servers, ok := config.Groups[gid]; ok {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply FetchShardReply

			ok := srv.Call("ShardKV.FetchShard", &args, &reply)
			if ok && reply.Success {
				DPrintf("[%v][%v] Num %v Shard %v fetch success", kv.gid, kv.me, args.Num, args.Shard)
				return reply.ShardData, true
			}
		}
	} else {
		log.Fatal("Err: No group info")
	}
	return ShardData{}, false
}

func (kv *ShardKV) clearShard(shard int, num int) bool {
	config := kv.getConfig(num)
	gid := config.Shards[shard]

	args := ClearShardArgs {
		Num:   num,
		Shard: shard,
	}

	if servers, ok := config.Groups[gid]; ok {
		// try each server for the shard.
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ClearShardReply

			ok := srv.Call("ShardKV.ClearShard", &args, &reply)
			if ok && reply.Success {
				DPrintf("[%v][%v] clear [%v] shard %v num %v", kv.gid, kv.me, gid, shard, num)
				return true
			}
		}
	} else {
		DPrintf("Err: No group info")
	}
	DPrintf("CLR FAILED")
	return false
}

func (kv *ShardKV) pullShards() {
	for {
		select {
		case <- kv.stopCh:
			return
		case <- kv.pullShardsTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullShardsTimer.Reset(PullShardsInterval)
				break
			}
			for i := 0; i < shardmaster.NShards; i++ {
				go func (shard int)  {

					ss := kv.getShardState(shard)

					if ss.Num == kv.getConfigNum() && ss.State != "FETCHED" {
						DPrintf("[%v][%v] shard %v state %v num %v %v", kv.gid, kv.me, shard, ss.State, ss.Num, kv.getConfigNum())
						return
					}

					DPrintf("!!![%v][%v] shard %v state %v num %v %v", kv.gid, kv.me, shard, ss.State, ss.Num, kv.getConfigNum())
					currConfig := kv.getConfig(ss.Num)
					nextConfig := kv.getConfig(ss.Num+1)

					switch ss.State {
					case "NONE":
						if nextConfig.Shards[shard] == kv.gid {
							// will own the shard
							if currConfig.Shards[shard] == 0 {
								kv.rf.Start(ShardInfo {
									Shard: shard,
									ShardState: ShardState {
										Num:   nextConfig.Num,
										State: "CLR",
									},
								})
							} else {
								DPrintf("~~~[%v][%v] shard %v state %v num %v %v START", kv.gid, kv.me, shard, ss.State, ss.Num, kv.getConfigNum())
								if sharddata, ok := kv.fetchShard(shard, currConfig.Num); ok {
									kv.rf.Start(ShardInfo {
										Shard: shard,
										ShardState: ShardState {
											Num:   nextConfig.Num,
											State: "FETCHED",
										},
										ShardData: sharddata,
									})									
									DPrintf("~~~[%v][%v] shard %v state %v num %v %v SUCCESS map %v %v", kv.gid, kv.me, shard, ss.State, ss.Num, kv.getConfigNum(), sharddata.Data, sharddata.ResTable)
								}
								DPrintf("~~~[%v][%v] shard %v state %v num %v %v FAILED", kv.gid, kv.me, shard, ss.State, ss.Num, kv.getConfigNum())
							}
						} else {
							// still not own the shard
							kv.rf.Start(ShardInfo {
								Shard: shard,
								ShardState: ShardState {
									Num: nextConfig.Num,
									State: "NONE",
								},
							})
						}
					case "FETCHED":
						if kv.clearShard(shard, currConfig.Num-1) {
							kv.rf.Start(ShardInfo {
								Shard: shard,
								ShardState: ShardState {
									Num:   currConfig.Num, // finish and go into next config
									State: "CLR",
								},
							})
						}
					case "CLR":
						if nextConfig.Shards[shard] != kv.gid {
							kv.rf.Start(ShardInfo {
								Shard: shard,
								ShardState: ShardState {
									Num:   nextConfig.Num,
									State: "NONE",
								},
							})
						} else {
							// still own the shard
							kv.rf.Start(ShardInfo {
								Shard: shard,
								ShardState: ShardState {
									Num: nextConfig.Num,
									State: "CLR",
								},
							})
						}
					}
				}(i)
			}

			kv.pullShardsTimer.Reset(PullShardsInterval)
		}
	}
}

// =====================================================
//                  SNAPSHOT MODULE

func (kv *ShardKV) saveSnapshot(snapshotIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		// DPrintf("[%v][%v] RaftStateSize %v maxraftstate %v", kv.gid, kv.me, kv.persister.RaftStateSize(), kv.maxraftstate)
		return
	}

	DPrintf("[%v][%v] saveSnapshot snapshotIndex %v RaftStateSize %v maxraftstate %v", kv.gid, kv.me, snapshotIndex, kv.persister.RaftStateSize(), kv.maxraftstate)
	data := kv.genSnapshotData()
	kv.rf.PersistSnapshot(snapshotIndex, data)
	DPrintf("[%v][%v] saveSnapshot SUCCESS size %v", kv.gid, kv.me, len(data))
}

func (kv *ShardKV) genSnapshotData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if e.Encode(kv.db) != nil ||
		e.Encode(kv.resTable) != nil ||
		e.Encode(kv.shardsState) != nil ||
		e.Encode(kv.historyShards) != nil ||
		e.Encode(kv.config) != nil {
		panic("gen snapshot data encode err")
	}

	// testw := new(bytes.Buffer)
	// teste := labgob.NewEncoder(testw)
	// teste.Encode(kv.historyShards)
	// DPrintf("***SIZE*** %v", len(testw.Bytes()))

	data := w.Bytes()
	return data
}

func (kv *ShardKV) readSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var db [shardmaster.NShards]map[string]string
	var resTable [shardmaster.NShards]map[int64]Result
	var shardsState [shardmaster.NShards]ShardState
	var config shardmaster.Config
	var historyShards map[int]ShardData

	if d.Decode(&db) != nil ||
		d.Decode(&resTable) != nil ||
		d.Decode(&shardsState) != nil ||
		d.Decode(&historyShards) != nil ||
		d.Decode(&config) != nil {
		log.Fatalf("[%v][%v] readSnapShot FAILED", kv.gid, kv.me)
	} else {
		kv.db = db
		kv.resTable = resTable
		kv.shardsState = shardsState
		kv.config = config
		kv.historyShards = historyShards
	}
}

func (kv *ShardKV) applySnapshot() {
	kv.lock()
	defer kv.unlock()
	DPrintf("[%v][%v] APPLY", kv.gid, kv.me)
	kv.readSnapShot(kv.persister.ReadSnapshot())
	DPrintf("[%v][%v] APPLY SUCCESS", kv.gid, kv.me)
}

// =====================================================

func registerAll() {
	labgob.Register(shardmaster.Config{})
	labgob.Register(ShardData{})
	labgob.Register(ShardState{})
	labgob.Register(ShardInfo{})
	labgob.Register(ClearShardArgs{})
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	registerAll()
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.stopCh = make(chan struct{})
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.configsCache = make(map[int]shardmaster.Config)
	kv.historyShards = make(map[int]ShardData)
	kv.initDB()
	kv.initShardsState()
	kv.initResTable()
	kv.initConfig()

	kv.readSnapShot(kv.persister.ReadSnapshot())

	kv.pullConfigTimer = time.NewTimer(PollConfigInterval)
	kv.pullShardsTimer = time.NewTimer(PullShardsInterval)

	go kv.applyCommand()
	go kv.pollConfig()
	go kv.pullShards()

	DPrintf("[%v][%v] server start maxraftstate %v", kv.gid, kv.me, kv.maxraftstate)
	return kv
}
