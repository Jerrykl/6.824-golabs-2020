package raft

import (
	// "log"
	"time"
	"sync/atomic"
)

type InstallSnapshotArgs struct {
	Term int               // leader’s term
	LeaderID int           // so follower can redirect clients
	LastIncludedIndex int  // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int   // term of lastIncludedIndex
	Offset int             // byte offset where chunk is positioned in the snapshot file
	Data[] byte            // raw bytes of the snapshot chunk, starting at offset
	Done bool              // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	atomic.AddInt64(&rf.sendInstallSnapshotCount , 1)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.lastReceive = time.Now()

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		return
	}

	// accept snapshot
	surplus := args.LastIncludedIndex - rf.lastSnapshotIndex
	if surplus >= len(rf.log) {
		rf.log = make([]LogEntry, 1)
		rf.log[0].Index = args.LastIncludedIndex
		rf.log[0].Term = args.LastIncludedTerm
	} else {
		rf.log = rf.log[surplus:]
	}

	DPrintf("[%v] install snapshot index %v ", rf.me, args.LastIncludedIndex)

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.genPersistData(), args.Data)
}

func (rf *Raft) PersistSnapshot(snapshotIndex int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if snapshotIndex <= rf.lastSnapshotIndex || snapshotIndex > rf.commitIndex {
	// 	log.Fatalf("snapshotIndex: %v, lastSnapshotIndex: %v, commitIndex: %v", snapshotIndex, rf.lastSnapshotIndex, rf.commitIndex)
	// }

	DPrintf("[%v] do snapshot index %v ", rf.me, snapshotIndex)

	realSnapshotIndex := rf.getRealIndex(snapshotIndex)
	rf.lastSnapshotIndex = snapshotIndex
	rf.lastSnapshotTerm = rf.log[realSnapshotIndex].Term
	rf.log = rf.log[realSnapshotIndex:] // trick (rather than [realSnapshotIndex+1:], to keep the last one entry for easier indexing)
	stateData := rf.genPersistData()
	rf.persister.SaveStateAndSnapshot(stateData, snapshotData)
}

func (rf *Raft) getRealIndex(idx int) int {
	return idx - rf.lastSnapshotIndex
}