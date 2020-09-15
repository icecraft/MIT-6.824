package raft

import (
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

func doAppendEntry(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) {

	Dlog("node: %v, leader:%v, term:%d, prevLogIndex:%d, prevLogTerm:%d, log:%v @%d\n",
		rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, MicroSecondNow())
	defer Dlog("node: %d, leader: %v, index: %d, reply: %v @%d\n", rf.me, args.LeaderId, args.PrevLogIndex, reply, MicroSecondNow())
	rf.mu.Lock()
	reply.Term = rf.Term
	reply.Success = false

	if reply.Term > args.Term {
		rf.mu.Unlock()
		return
	}
	rf.updateElectionTimeOut()

	reply.IndexHint = INVALID_INDEX_HINT
	// init Append Msg
	if rf.State == STATE_CANDIDATER {
		rf.beFollower1(args)
		rf.mu.Unlock()
		return
	} else if rf.State == STATE_LEADER {
		if args.Term > reply.Term {
			Dlog("node: %d Got args.Term: %d, my term: %d\n", rf.me, args.Term, reply.Term)
			rf.stopHeartBeatThead()
			rf.beFollower1(args)
			rf.mu.Unlock()
			return
		} else {
			log.Fatalf("two leader with the same term: %d, node1: %d, node2: %d\n", reply.Term, rf.me, args.LeaderId)
		}
	} else {
		// todo： 如果我是 leader呢？
		// 会不会出现 follower 的 term 等于 leader term ？
		if rf.Term != args.Term || rf.LeaderId != args.LeaderId {
			Dlog("node: %d recv msg: %v\n", rf.me, args)
			rf.LeaderId = args.LeaderId
			rf.Term = args.Term
			rf.persist()
			reply.Success = false
			rf.mu.Unlock()
			return
		}
	}

	// 2B
	/*
		case1: 直接返回 false
		case2: 返回 true， 且只会 append log
		case3: 返回 true， 且会 applied log 同 state machine!
	*/
	if args.PrevLogIndex == INIT_APPMSG_INDEX && args.PrevLogTerm == INIT_APPMSG_TERM {
		rf.mu.Unlock()
		return
	}

	// 通知 server 可以发送 snapshot 过来了
	if args.WillInstallSnapShot == true {
		reply.Success = true
		rf.mu.Unlock()
		return
	}

	var appendLog bool
	//case1
	if args.PrevLogIndex == rf.getLogIndex() {
		if args.PrevLogIndex >= 0 {
			if args.PrevLogTerm == rf.getLogTerm(args.PrevLogIndex) {
				appendLog = true
			}
		} else {
			appendLog = true
		}
	} else if rf.getLogIndex() > args.PrevLogIndex {
		if args.PrevLogIndex >= 0 {
			rf.log = rf.copyLogTo(args.PrevLogIndex)
			rf.persist()
			if rf.getLogTerm(args.PrevLogIndex) == args.PrevLogTerm {
				appendLog = true
			}
		} else {
			Dlog("node: %d, prevLogIndex: %d\n", rf.me, args.PrevLogIndex)
			panic("Error preLogIndex\n")
		}
	}

	if appendLog {
		if len(args.Entries) > 0 {
			Dlog("node: %d will append log, leaderId: %d, leaderTerm: %d, first_log: %v, last_log: %v, log length: %d, @%d\n",
				rf.me, args.LeaderId, args.Term, args.Entries[0], args.Entries[len(args.Entries)-1], len(args.Entries), MicroSecondNow())
		}
		rf.log = append(rf.log, args.Entries...)
		rf.persist()
		Dlog("node: %d, log length: %d, log: %v\n", rf.me, len(rf.log), rf.log)
		reply.Success = true
		reply.IndexHint = rf.getLogIndex()
	}

	if reply.Success {
		commitIndex := minInt(args.CommitIndex, rf.getLogIndex())

		if commitIndex > rf.lastAppliedIndex {
			Dlog("current cindex: %d, new cindex: %d\n", rf.commitIndex, commitIndex)
			rf.commitIndex = commitIndex
			rf.mu.Unlock()
			rf.notifyApplyCh <- true
			return
		}
	} else {
		Dlog("me: %d will not append log from node:%d\n", rf.me, args.LeaderId)
		reply.IndexHint = rf.getLogIndex()
		if reply.IndexHint > 0 && reply.IndexHint > rf.maxSnapShotIndex {
			cterm := rf.getLogTerm(reply.IndexHint)
			for reply.IndexHint > 0 && reply.IndexHint > rf.maxSnapShotIndex {
				if rf.getLogTerm(reply.IndexHint) != args.PrevLogTerm && cterm == rf.getLogTerm(reply.IndexHint) {
					reply.IndexHint--
					continue
				}
				break
			}
			if cterm == args.PrevLogTerm {
				if reply.IndexHint > 0 && reply.IndexHint > rf.maxSnapShotIndex {
					reply.IndexHint--
				}
			}

		}
		if len(rf.log) > 0 {
			Dlog("node: %d refuse to append, args:{term: %d, llogIdx: %d, llogTerm:%d}, llogIdx:%d, llogTerm: %d, Hint: %d\n",
				rf.me, args.Term, args.PrevLogIndex, args.PrevLogTerm, len(rf.log)-1, rf.log[len(rf.log)-1].Term, reply.IndexHint)
		}
	}
	rf.mu.Unlock()
	return
}

func doInitAppendMsg(rf *Raft) {
	go func() {
		rf.mu.Lock()
		if rf.State != STATE_LEADER {
			rf.mu.Unlock()
			return
		}
		args := AppendEntryArgs{
			Term:         rf.Term,
			LeaderId:     rf.me,
			PrevLogIndex: INIT_APPMSG_INDEX,
			PrevLogTerm:  INIT_APPMSG_TERM,
			Entries:      make([]Entry, 0),
			CommitIndex:  INIT_APPMSG_INDEX}
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go func(peerIndex int) {
				args1 := args
				reply := AppendEntryReply{}
				rf.sendAppendEntries(peerIndex, &args1, &reply)
			}(i)
		}
	}()
}

// 在心跳这里实现数据同步功能吧， 其实 raft 不应该这样的。但是应该怎么样的，我又说不清楚
func doStartHeartBeatThread(rf *Raft) {
	go func() {
		ticker := time.NewTicker(rf.HeartBeatInterval)
		var convertToFollower int32
		var recieveNum int32
		recieveNum = int32(len(rf.peers))
		for {
			if rf.Killed() {
				return
			}
			select {
			case <-rf.stopHeartBeatChan:
				Dlog("will stop heartbeat thread: %d\n", rf.me)
				return
			case <-ticker.C:
				if rf.Killed() {
					return
				}
				if atomic.LoadInt32(&recieveNum) >= int32(len(rf.peers)/2) {
					rf.mu.Lock()
					rf.lease = MicroSecondNow() + int64(rf.MaxElectTime)
					rf.mu.Unlock()
				}
				atomic.StoreInt32(&recieveNum, 0)

				if atomic.LoadInt32(&convertToFollower) > 0 {
					break
				}
				for i := range rf.peers {
					if i == rf.me {
						continue
					}
					if atomic.LoadInt32(&convertToFollower) > 0 {
						return
					}
					go func(peerIndex int) {
						sendMsgToPeer(rf, peerIndex, &convertToFollower, &recieveNum)
					}(i)
				}
				time.Sleep(10 * time.Millisecond)
				updateLeaderAppliedIndex(rf)
			}
		}
	}()
}

func sendMsgToPeer(rf *Raft, peerIndex int, convertToFollower, recieveNum *int32) {
	rf.mu.Lock()
	if rf.State != STATE_LEADER || atomic.LoadInt32(convertToFollower) > 0 {
		rf.mu.Unlock()
		atomic.AddInt32(convertToFollower, 1)
		return
	}

	args := AppendEntryArgs{
		Term:         rf.Term,
		LeaderId:     rf.me,
		Entries:      make([]Entry, 0),
		PrevLogTerm:  0,
		PrevLogIndex: 0,
		CommitIndex:  rf.commitIndex}

	args.PrevLogIndex = rf.nextIndex[peerIndex] - 1
	// 注意 snapshot 后 rf.log 中至少保留一条数据，不然不给做快照
	if len(rf.log) > 0 && rf.log[0].Index > args.PrevLogIndex+1 {
		Dlog("node: %d Will try to sync snapshot to node: %d\n", rf.me, peerIndex)
		args.WillInstallSnapShot = true
	} else {
		if args.PrevLogIndex > 0 {
			if args.PrevLogIndex > rf.getLogIndex() {
				log.Infof("ERROR: node: %d, prevLogIndex:%v, max index of log %v, matchIndex:%d, nextIndex: %d, term: %d@%d\n",
					peerIndex, args.PrevLogIndex, rf.getLogIndex(), rf.matchIndex[peerIndex], rf.nextIndex[peerIndex], rf.Term, MicroSecondNow())
			}
			// Dlog("prevlogIndex: %d\n", args.PrevLogIndex)
			args.PrevLogTerm = rf.getLogTerm(args.PrevLogIndex)
		}
		if rf.getLogIndex() > args.PrevLogIndex {
			args.Entries = rf.copyLogFrom(args.PrevLogIndex + 1)
		}
		/*
			Dlog("leaderId: %d, node: %d, nextIndex: %d, matchIndex: %d, leaderLog max index: %d\n",
				rf.me, peerIndex, rf.nextIndex[peerIndex], rf.matchIndex[peerIndex], rf.getLogIndex())
		*/
	}
	rf.mu.Unlock()

	reply := AppendEntryReply{}
	if ok := rf.sendAppendEntries(peerIndex, &args, &reply); ok {
		rf.mu.Lock()
		atomic.AddInt32(recieveNum, 1)
		if reply.Term > rf.Term {
			atomic.AddInt32(convertToFollower, 1)
			rf.updateElectionTimeOut()
			rf.State = STATE_FOLLOWER
			rf.Term = reply.Term
			rf.LeaderId = -1
			rf.VotedFor = VOTED_FOR_NULL
			rf.persist()
			Dlog("%d converted from leader to follower @%d when send msg\n", rf.me, MicroSecondNow())
		} else if reply.Term == rf.Term {
			if args.WillInstallSnapShot == true {
				if reply.Success == true {
					rf.mu.Unlock()
					go sendSnapShotToPeer(rf, peerIndex)
					return
				}
			} else if reply.Success == false {
				rf.nextIndex[peerIndex] = reply.IndexHint + 1
				if rf.matchIndex[peerIndex] >= rf.nextIndex[peerIndex] {
					rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
				}
			} else {
				if reply.IndexHint < rf.matchIndex[peerIndex] {
					log.Infof("IndexHint: %d, matchIndex: %d\n", reply.IndexHint, rf.matchIndex[peerIndex])
				}
				rf.matchIndex[peerIndex] = reply.IndexHint
				rf.nextIndex[peerIndex] = reply.IndexHint + 1
			}
		}

		Dlog("leader: %d, recieve appendMsg reply from %d, %v, matchIndex: %d, nextIndex: %d\n",
			rf.me, peerIndex, reply, rf.matchIndex[peerIndex], rf.nextIndex[peerIndex])

		rf.mu.Unlock()
	}
}

func updateLeaderAppliedIndex(rf *Raft) {
	matchIndex := make([]int, len((rf.peers)))
	var upper, lower, mid, majority, appliedId int
	rf.mu.Lock()
	for i, v := range rf.matchIndex {
		matchIndex[i] = v
	}
	myCommitIndex := rf.commitIndex
	lower = rf.commitIndex
	upper = rf.getLogIndex()
	rf.mu.Unlock()

	appliedId = lower

	for upper >= lower {
		majority = 0
		if 5 > upper-lower {
			for i, v := range matchIndex {
				if i == rf.me {
					continue
				}
				if v >= lower {
					majority++
				}
			}
			if majority >= len(matchIndex)/2 {
				appliedId = lower
				lower++
				continue
			}
			break
		}
		mid = (upper + lower) / 2
		for i, v := range matchIndex {
			if i == rf.me {
				continue
			}
			if v >= mid {
				majority++
			}
		}
		if majority >= len(matchIndex)/2 {
			appliedId = mid
			lower = mid
		} else {
			upper = mid
		}
	}

	if appliedId > myCommitIndex {
		Dlog("node: %d, cIndex: %d, newIndex:%d, matchIndex: %v\n", rf.me, myCommitIndex, appliedId, matchIndex)
		doUpdate := false
		rf.mu.Lock()
		if rf.getLogTerm(appliedId) == rf.Term { // TODO:
			rf.commitIndex = appliedId
			doUpdate = true
		}
		rf.mu.Unlock()
		if doUpdate {
			rf.notifyApplyCh <- true
		}
	}
}

// 参考 https://github.com/yinfredyue/MIT6.824-new.git
func doApplyCommittedRoutine(rf *Raft) {
	// Later change this to use condition variable
	for {
		// Dlog("[%v] waitting msg to apply @%d\n", rf.me, MicroSecondNow())
		<-rf.notifyApplyCh
		Dlog("[%v] Notified to apply @%d\n", rf.me, MicroSecondNow())
		if rf.Killed() {
			break
		}
		toApply := false
		var applyMsg ApplyMsg

		rf.mu.Lock()
		for rf.commitIndex > rf.lastAppliedIndex {
			Dlog("me: %d, apply index:%d, commit:%d\n", rf.me, rf.lastAppliedIndex, rf.commitIndex)
			rf.lastAppliedIndex++
			v, err := rf.getLogEntry(rf.lastAppliedIndex)
			if err != nil {
				log.Fatal(err)
			}
			rf.lastAppliedTerm = v.Term
			applyMsg = ApplyMsg{
				Command: v.Command,
				Index:   rf.lastAppliedIndex,
			}
			toApply = true
			if rf.Killed() {
				break
			}
			rf.mu.Unlock()
			if toApply {
				Dlog("[%v] sends %v on applyCh, term: %d @%d\n", rf.me, applyMsg, rf.Term, MicroSecondNow())
				rf.applyCh <- applyMsg
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}
