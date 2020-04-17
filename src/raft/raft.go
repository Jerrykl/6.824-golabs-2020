package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"../labrpc"

	"bytes"
	"../labgob"

	"math/rand"
	"time"
	"log"
)

type State string

const (
	Follower State = "follower"
	Candidate State = "candidate"
	Leader State = "leader"
)

const (
	ElectionInterval int = 1000
	HeartbeatInterval int = 100
)

func Min(x, y int) int {
    if x < y {
        return x
    }
    return y
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	cond      *sync.Cond          // Cond for waitting new committed logs and apply them

	state State                   // current state
	lastReceive time.Time         // last receive time
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent
	currentTerm int     // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int     // candidateId that received vote in current term (or null if none)
	log         []LogEntry

	// volatile on all servers
	commitIndex int     // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int     // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile on leaders
	nextIndex   []int  // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int  // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == Leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf("[%d][%d] store persisted state commitIndex %d len(log) %d", rf.me, rf.currentTerm, rf.commitIndex, len(rf.log))
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil || 
		d.Decode(&logs) != nil {
		log.Fatal("fail to restore persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		DPrintf("[%d][%d] restore persisted state len(log) %d", rf.me, rf.currentTerm, len(rf.log))
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int           // candidate’s term
	CandidateId int    // candidate requesting vote
	LastLogIndex int   // index of candidate’s last log entry
	LastLogTerm int    // term of candidate’s last log entry

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int         // currentTerm, for candidate to update itself 
	VoteGranted bool // true means candidate received vote
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term           int           // leader’s term
	LeaderID       int           // so follower can redirect clients
	PrevLogIndex   int           // index of log entry immediately preceding new ones
	PrevLogTerm    int           // term of prevLogIndex entry
	Entries        []LogEntry    // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit   int           // leader’s commitIndex
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term    int   // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm int   // term for the conflict entry
	FirstIndexOfConflictTerm int  // the first index of the conflict term
}

// 
// return the last log entry
// 
func (rf *Raft) getLastLogEntry() LogEntry {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	} else {
		return LogEntry {
			Command: nil,
			Index:   -1,
			Term:    -1,
		}
	}
}

// 
// state transfer to Follower
// 
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = Follower
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastReceive = time.Now()
}

// 
// state transfer to Candidate
// 
func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.lastReceive = time.Now()
}

// 
// state transfer to Leader
// 
func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.lastReceive = time.Now()
	for p, _ := range rf.peers {
		rf.nextIndex[p] = len(rf.log)
		rf.matchIndex[p] = -1
	}
}

// 
// compare two log entries
// 
func (l LogEntry) isMoreUpToDate(r LogEntry) bool {
	return (l.Term > r.Term) || (l.Term == r.Term && l.Index >= r.Index)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// rf.lastReceive = time.Now()
	DPrintf("[%d][%d] receives request vote from %d", rf.me, rf.currentTerm, args.CandidateId)

	lastEntry := rf.getLastLogEntry()

	logUpToDate := LogEntry {
		Term:    args.LastLogTerm,
		Index:   args.LastLogIndex,
		Command: nil,
	}

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.convertToFollower(args.Term)
		}
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logUpToDate.isMoreUpToDate(lastEntry) {
			if (rf.votedFor == args.CandidateId) {
				log.Printf("[%d][%d] strange case: vote for %d\n", rf.me, rf.currentTerm, args.CandidateId)
			}
			DPrintf("[%d][%d] case: vote for %d l {%d, %d} r {%d, %d}", rf.me, rf.currentTerm, args.CandidateId, logUpToDate.Term, logUpToDate.Index, lastEntry.Term, lastEntry.Index)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
	rf.persist()
}

// 
// AppendEntries RPC handler
// 
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// DPrintf("[%d][%d] receives HEARBEAT from %d", rf.me, rf.currentTerm, args.LeaderID)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.FirstIndexOfConflictTerm = -1
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastReceive = time.Now()

	// local log is shorter
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.FirstIndexOfConflictTerm = len(rf.log)
		return
	}

	// get the first index of conflicted term if prevLogTerm conflicted
	if args.PrevLogIndex >= 0 {
		localPrevLogTerm := rf.log[args.PrevLogIndex].Term

		if localPrevLogTerm != args.PrevLogTerm {
			reply.FirstIndexOfConflictTerm = args.PrevLogIndex
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != localPrevLogTerm {
					break
				}
				reply.FirstIndexOfConflictTerm = i
			}
			reply.Success = false
			reply.ConflictTerm = localPrevLogTerm
			rf.log = rf.log[:args.PrevLogIndex]
			return
		}
	}

	// will append none if len(args.Entries) == 0
	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Signal()
	}

	rf.persist()

	reply.Success = true
	reply.ConflictTerm = -1
	reply.FirstIndexOfConflictTerm = -1
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartBeat(peer int) {
	rf.mu.Lock()
	if rf.nextIndex[peer] > len(rf.log) {
		rf.nextIndex[peer] = len(rf.log)
	}

	prevLogIndex := rf.nextIndex[peer]-1
	prevLogTerm := -1
	if prevLogIndex >= 0 {
		prevLogTerm = rf.log[prevLogIndex].Term
	}

	args := AppendEntriesArgs {
		Term:           rf.currentTerm,
		LeaderID:       rf.me,
		PrevLogIndex:   prevLogIndex,
		PrevLogTerm:    prevLogTerm,
		Entries:        rf.log[rf.nextIndex[peer]:], // return [] if rf.nextIndex[peer] >= len(rf.log)
		LeaderCommit:   rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		return
	}

	// DPrintf("[%d][%d] send HEARBEAT success to %d", rf.me, rf.currentTerm, peer)

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}

	// term has changed
	if rf.currentTerm != args.Term {
		return
	}

	// update rf.nextIndex when entries conflicted
	if !reply.Success {
		rf.nextIndex[peer] = reply.FirstIndexOfConflictTerm

		if reply.ConflictTerm != -1 {
			localLastConflictIndex := -1

			for i := prevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					localLastConflictIndex = i
					break
				} else if rf.log[i].Term < reply.ConflictTerm {
					break
				}
			}

			if localLastConflictIndex != -1 {
				rf.nextIndex[peer] = localLastConflictIndex + 1
			}

		}
		return
	}

	// update matchIndex and nextIndex
	rf.matchIndex[peer] = prevLogIndex + len(args.Entries)
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1

	// update commitIndex
	for n := len(rf.log) - 1; n > rf.commitIndex; n-- {
		// only commit logs of current term
		if rf.log[n].Term != rf.currentTerm {
			break
		}
		numRelicas := 1
		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= n {
				numRelicas += 1
			}
			if numRelicas > (len(rf.peers) / 2) {
				rf.commitIndex = n
				rf.cond.Signal()
				break
			}
		}
	}
}

// 
// periodically sends heartbeats to peer server
// 
func (rf *Raft) leaderAppend(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// DPrintf("[%d][%d] sends HEARBEAT to %d", rf.me, rf.currentTerm, peer)
		go rf.sendHeartBeat(peer)
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

// 
// try an election
// 
func (rf *Raft) tryElection() {
	rf.mu.Lock()
	DPrintf("[%d][%d] start of the election", rf.me, rf.currentTerm)
	rf.convertToCandidate()
	lastLogEntry := rf.getLastLogEntry()
	args := RequestVoteArgs{
		Term:          rf.currentTerm,
		CandidateId:   rf.me,
		LastLogIndex:  lastLogEntry.Index,
		LastLogTerm:   lastLogEntry.Term,
	}
	numVotes := 1
	rf.mu.Unlock()
	for p, _ := range rf.peers {
		if p == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			DPrintf("[%d] send request vote to %d", rf.me, server)
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.convertToFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				numVotes += 1
				// rf.state == Candidate to prevent from old votes and ensures that only one goroutine issues leader calls
				if (numVotes > len(rf.peers) / 2) && (rf.state == Candidate) {
					DPrintf("[%d][%d] becomes leader", rf.me, rf.currentTerm)
					rf.convertToLeader()
					for s, _ := range rf.peers {
						if s != rf.me {
							go rf.leaderAppend(s)
						}
					}
				}
			}
		}(p)
	}
}

// 
// set timer and try election
// 
func (rf *Raft) serverElect() {
	for {
		// kick off leader election periodically by sending out RequetVote
		// when it hasn't heard from another peer for a while
		electionTimeout := ElectionInterval + rand.Intn(200)
		startTime := time.Now()
		time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.lastReceive.Before(startTime) {
			if rf.state != Leader {
				DPrintf("[%d][%d] kicks off election", rf.me, rf.currentTerm)
				go rf.tryElection()
			}
		}
		rf.mu.Unlock()
	}
}

// 
// wait for new commited logs and apply them to state machine via applyCh
// 
func (rf *Raft) serverApply(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if (rf.commitIndex == rf.lastApplied) {
			rf.cond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			// value, _ := rf.log[rf.lastApplied].Command.(int)
			// DPrintf("[%d][%d] apply index %d value %d", rf.me, rf.currentTerm, rf.lastApplied, value)
			applyCh <- ApplyMsg {
				CommandValid:   true,
				Command:        rf.log[rf.lastApplied].Command,
				CommandIndex:   rf.lastApplied+1, // plus 1 to get index to start at 1
			}
		}
		rf.mu.Unlock()
	}
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.state == Leader

	// Your code here (2B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Index:      index,
			Term:       term,
			Command:    command,
		})
		rf.persist()
		// value, _ := command.(int)
		// DPrintf("[%d][%d] Starts Client Request %d Command %v", rf.me, rf.currentTerm, index, value)
	}

	// plus 1 to get index to start at 1
	return index+1, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.cond.Signal()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.cond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1

	// Your initialization code here (2A, 2B, 2C).
	rf.dead = 0
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.lastReceive = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.serverElect()
	go rf.serverApply(applyCh)
	DPrintf("%d initialized", rf.me)

	return rf
}
