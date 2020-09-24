package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	// "sync"
	"fmt"
	// "runtime/debug"
	"sync/atomic"
	"time"

	"../labrpc"
	"github.com/sasha-s/go-deadlock"
	// log "github.com/sirupsen/logrus"
)

const (
	STATE_LEADER     = iota
	STATE_CANDIDATER = iota
	STATE_FOLLOWER   = iota
)

const (
	VOTED_FOR_NULL     = -1
	INIT_APPMSG_TERM   = -2
	INIT_APPMSG_INDEX  = -2
	INVALID_INDEX_HINT = -2
	NO_LEADER          = -1
	INVALID_TERM       = -2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// mu sync.Mutex // Lock to protect shared access to this peer's state
	mu        deadlock.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	LeaderId int
	Term     int
	State    int // Leader, Candidator, Follower

	VotedFor int

	log []Entry

	commitIndex      int
	lastAppliedIndex int
	lastAppliedTerm  int

	// Leader stats only
	nextIndex  []int
	matchIndex []int
	dead       int32
	lease      int64

	heartbeatCount int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	MinElectTime        int
	MaxElectTime        int
	HeartBeatInterval   time.Duration
	electionTime        time.Duration
	nextElectionTs      int64
	maxSnapShotIndex    int
	maxSnapShotTerm     int
	latestSnapShotIndex int

	// channel
	electTimeOutChan  chan bool
	stopHeartBeatChan chan bool
	uuid              int64

	applyCh       chan ApplyMsg
	notifyApplyCh chan bool
}

// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	if rf.State == STATE_LEADER && rf.lease > MicroSecondNow() {
		isleader = true
	}
	term = rf.Term
	rf.mu.Unlock()
	return term, isleader
}

// believes it is the leader.
func (rf *Raft) GetState2() (int, int, int, bool) {

	var term int
	var isleader bool
	var index int
	// Your code here (2A).
	rf.mu.Lock()
	if rf.State == STATE_LEADER && rf.lease > MicroSecondNow() {
		isleader = true
		index = rf.getLogIndex()
	}
	term = rf.Term
	rf.mu.Unlock()
	return index, term, rf.State, isleader
}

func (rf *Raft) SaveSnapShot(data []byte) {
	rf.mu.Lock()
	rf.persister.SaveSnapShot(data)
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) ReadSnapShot() []byte {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.ReadSnapshot()
}

func (rf *Raft) SateSize() int {
	// 就不要用 raft 锁了
	// log.Infof("SIZE: %d\n", rf.persister.RaftStateSize())
	return rf.persister.RaftStateSize()
}

func (rf *Raft) MaySaveSnapShot(snapShot []byte, index, term int) int {
	rf.mu.Lock()
	if len(rf.log) > 0 && rf.maxSnapShotIndex >= index {
		rf.mu.Unlock()
		return -1
	}
	// rf.lastAppliedIndex = index
	rf.setAppliedIndex(index, 3)
	rf.commitIndex = index
	rf.maxSnapShotIndex = index
	rf.maxSnapShotTerm = term

	pos := search(rf.log, index)
	if pos == -1 {
		if len(rf.log) > 0 && index > rf.log[len(rf.log)-1].Index {
			rf.log = make([]Entry, 0)
		}
	} else {
		if rf.log[pos].Index != index {
			panic(fmt.Sprintf("Error unequal index: %d, %d\n", index, rf.log[pos].Index))
		} else {
			rf.log = copyEntries(rf.log[pos+1:])
		}
	}
	Dlog("me: %d, Save SnapShot, index: %d, term: %d\n", rf.me, index, term)
	state := rf.serialSate()
	rf.persister.SaveStateAndSnapshot(state, snapShot)
	rf.mu.Unlock()
	return 0
}

func (rf *Raft) supportSnap() bool {
	if rf.persister.SnapshotSize() == 0 {
		if len(rf.log) > 0 && rf.log[0].Index == 0 {
			return false
		}
	}
	return true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) serialSate() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.Term)
	e.Encode(rf.VotedFor)
	e.Encode(rf.log)
	if rf.supportSnap() {
		e.Encode(rf.maxSnapShotIndex)
		e.Encode(rf.maxSnapShotTerm)
	} else {
		e.Encode(rf.lastAppliedIndex)
		e.Encode(rf.lastAppliedTerm)
	}
	return w.Bytes()
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.serialSate()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.Term)
	d.Decode(&rf.VotedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.maxSnapShotIndex)
	d.Decode(&rf.maxSnapShotTerm)
	if rf.supportSnap() {
		if len(rf.log) > 0 {
			rf.setAppliedIndex(rf.log[0].Index-1, 2)
			rf.commitIndex = rf.log[0].Index - 1
		} else {
			rf.setAppliedIndex(rf.maxSnapShotIndex, 2)
			rf.commitIndex = rf.maxSnapShotIndex
		}
	} else {
		rf.lastAppliedIndex = 0
		rf.commitIndex = 0
	}
	Dlog("me: %d recover from stat, index: %d, index: %d, log: %v\n", rf.me, rf.lastAppliedIndex, rf.commitIndex, rf.log)
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	doRquestVote(rf, args, reply)
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	doAppendEntry(rf, args, reply)
}

func (rf *Raft) InstallSnapshot(args *AppendEntryArgs, reply *AppendEntryReply) {
	doInstallSnapshot(rf, args, reply)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapShot(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	rf.mu.Lock()
	if rf.State == STATE_LEADER {
		// 这个 leader 已经和 大部分的 follower 失联了
		if rf.lease > MicroSecondNow() {
			index = rf.getLogIndex() + 1
			rf.log = append(rf.log, Entry{Command: command, Term: rf.Term, Index: index})
			term = rf.Term
			isLeader = true
			rf.persist()
			Dlog("node: %s, Start: %v, term: %d, index: %d @%d\n", Red(rf.me), command, term, index, MicroSecondNow())
		}
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// raft.log 至少保存一条记录!
func (rf *Raft) DropBeforeIndex(index int, justCheck bool, snapshot []byte) int {
	// 因为提交给 stat machine 中的 index 是加过 一的
	if 0 > index {
		panic("negative drop index\n")
	}
	if rf.maxSnapShotIndex >= index {
		return NO_NEED_TO_DROP_RAFT_STATE
	}
	rf.mu.Lock()
	// 绝对不会出现的
	if len(rf.log) == 0 {
		rf.mu.Unlock()
		return NO_NEED_TO_DROP_RAFT_STATE
	}
	if rf.log[len(rf.log)-1].Index == index {
		Dlog("me: %d will not drop raftstate\n", rf.me)
		rf.mu.Unlock()
		return DROP_RAFT_STATE_FAILED
	}
	pos := search(rf.log, index)
	if pos == NOT_FOUND_LEFT {
		rf.mu.Unlock()
		panic("can not find the index to drop, 1\n")
	}

	Dlog("me: %d will do logCompaction index: %d @ %d\n", rf.me, index, MicroSecondNow())
	rf.maxSnapShotIndex = rf.log[pos].Index
	rf.maxSnapShotTerm = rf.log[pos].Term
	/*
		rf.setAppliedIndex(index, 1)
		// rf.lastAppliedIndex = index
		rf.commitIndex = index
	*/
	rf.log = copyEntries(rf.log[pos+1:])
	Dlog("aft log: %v\n", rf.log)
	state := rf.serialSate()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
	rf.mu.Unlock()
	return 0
}

func (rf *Raft) getLogIndex() int {
	if len(rf.log) == 0 {
		return rf.maxSnapShotIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) sGeLogIndex() int {
	var ret int
	rf.mu.Lock()
	ret = rf.getLogIndex()
	rf.mu.Unlock()
	return ret
}

func (rf *Raft) copyLogFrom(index int) []Entry {
	pos := search(rf.log, index)
	if pos >= 0 {
		return copyEntries(rf.log[pos:])
	}
	panic(fmt.Sprintf("me: %d, NOT FOUND: raft log entry by index: %d, log:%v", rf.me, index, rf.log))

}

func (rf *Raft) copyLogTo(index int) []Entry {
	ret := make([]Entry, 0)
	pos := rightSearch(rf.log, index)
	if pos > -1 {
		ret = copyEntries(rf.log[:pos+1])
	}
	return ret
}

func (rf *Raft) getLogTerm(index int) int {
	if rf.maxSnapShotIndex == index {
		return rf.maxSnapShotTerm
	}
	pos := search(rf.log, index)
	if pos >= 0 {
		return rf.log[pos].Term
	}
	return INVALID_TERM
}

func (rf *Raft) getLogEntry(index int) (Entry, error) {
	if 0 >= index {
		return Entry{}, &ErrNotFound{Msg: "index can not being negative"}
	}
	pos := search(rf.log, index)
	if pos >= 0 {
		return rf.log[pos], nil
	}

	return Entry{}, &ErrNotFound{Msg: fmt.Sprintf("me: %d, NOT FOUND: raft log entry by index: %d, log:%v", rf.me, index, rf.log)}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	atomic.AddInt32(&rf.dead, 1)
}

//
func (rf *Raft) Killed() bool {
	return atomic.LoadInt32(&rf.dead) > 0
}

func (rf *Raft) sendElectionTimeout() {
	rf.safeUpdateElectionTimeOut()
	for {
		if rf.Killed() {
			break
		}
		currentElectionTimeOut := rf.getElectionTimeOut()
		rf.safeUpdateElectionTimeOut()
		select {
		case <-time.After(currentElectionTimeOut):
			if rf.Killed() {
				return
			}
			rf.mu.Lock()
			if rf.nextElectionTs > MicroSecondNow() {
				rf.mu.Unlock()
			} else {
				rf.mu.Unlock()
				go func() {
					rf.mu.Lock()
					state := rf.State
					rf.mu.Unlock()
					if state != STATE_LEADER {
						Dlog("will elect for leader: %d, state: %d, @%d\n", rf.me, state, MicroSecondNow())
						go rf.beCandidate()
					}
				}()
			}
		}
	}

	Dlog("will end send election timeout thread: %d\n", rf.me)
}

func (rf *Raft) getElectionTimeOut() time.Duration {
	rf.mu.Lock()
	ts := rf.electionTime
	rf.mu.Unlock()
	return ts
}

func (rf *Raft) updateElectionTimeOut() {
	nrandTs := randTs(rf.MinElectTime, rf.MaxElectTime)
	rf.electionTime = time.Duration(nrandTs) * time.Millisecond
	rf.nextElectionTs = MicroSecondNow() + nrandTs
}

func (rf *Raft) safeUpdateElectionTimeOut() {
	rf.mu.Lock()
	rf.updateElectionTimeOut()
	rf.mu.Unlock()
}

func (rf *Raft) beLeader() {
	rf.mu.Lock()
	rf.LeaderId = rf.me
	rf.State = STATE_LEADER
	if len(rf.nextIndex) != len(rf.peers) {
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
	}
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	// log.Infof("leaderId: %d, term: %d, nextIndex: %v, matchIndex: %v, commited: %d @%d\n",
	//	rf.me, rf.Term, rf.nextIndex, rf.matchIndex, rf.lastAppliedIndex, MicroSecondNow())
	rf.lease = MicroSecondNow() + int64(rf.MaxElectTime)
	rf.mu.Unlock()

	Dlog("%s: %d, term: %d, @%d\n", Red("beleaderId"), rf.me, rf.Term, MicroSecondNow())
	rf.initAppendMsg()
	rf.startHeartBeatThread()
}

func (rf *Raft) beCandidate() {
	doBeCandidate(rf)
}

func (rf *Raft) beFollower1(args *AppendEntryArgs) {
	Dlog("node: %d convert to follower @%d\n", rf.me, MicroSecondNow())
	rf.State = STATE_FOLLOWER
	rf.LeaderId = args.LeaderId
	rf.Term = args.Term
	rf.VotedFor = VOTED_FOR_NULL
	rf.persist()
}

func (rf *Raft) beFollower2(args *RequestVoteArgs) {
	Dlog("node: %d convert to follower @%d\n", rf.me, MicroSecondNow())
	stopHeartBeat := false
	if rf.State == STATE_LEADER {
		stopHeartBeat = true
	}
	rf.State = STATE_FOLLOWER
	rf.LeaderId = NO_LEADER
	rf.Term = args.Term
	rf.VotedFor = VOTED_FOR_NULL
	rf.persist()
	if stopHeartBeat {
		rf.stopHeartBeatChan <- true
	}
}

func (rf *Raft) stopHeartBeatThead() {
	rf.stopHeartBeatChan <- true
}

func (rf *Raft) initAppendMsg() {
	doInitAppendMsg(rf)
}

func (rf *Raft) startHeartBeatThread() {
	doStartHeartBeatThread(rf)
}

func (rf *Raft) applyCommittedRoutine() {
	doApplyCommittedRoutine(rf)
}

func (rf *Raft) setAppliedIndex(v, id int) {
	// if rf.lastAppliedIndex > v {
	Dlog("me: %d, id: %d, appliedIndex from %d to %d@%d\n", rf.me, id, rf.lastAppliedIndex, v, MicroSecondNow())
	// }
	rf.lastAppliedIndex = v
}

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
	Dlog("Make server: %d\n", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.State = STATE_FOLLOWER
	rf.LeaderId = -1
	rf.VotedFor = VOTED_FOR_NULL
	rf.commitIndex = 0
	rf.lastAppliedIndex = 0
	rf.Term = 0
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{}
	rf.applyCh = applyCh
	rf.notifyApplyCh = make(chan bool)
	rf.uuid = MicroSecondNow()

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.MinElectTime = 150
	rf.MaxElectTime = 300
	rf.HeartBeatInterval = 100 * time.Millisecond
	rf.electTimeOutChan = make(chan bool, 1)
	rf.stopHeartBeatChan = make(chan bool, 1)
	rf.maxSnapShotIndex = 0
	rf.maxSnapShotTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.sendElectionTimeout()
	go rf.applyCommittedRoutine()
	return rf
}
