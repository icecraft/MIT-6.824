package raft

import (
	log "github.com/sirupsen/logrus"
)

func doInstallSnapshot(rf *Raft, args *AppendEntryArgs, reply *AppendEntryReply) {
	Dlog("[snapshot] node: %v, leader:%v, term:%d, prevLogIndex:%d, prevLogTerm:%d, log:%v @%d\n",
		rf.me, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries, MicroSecondNow())

	rf.mu.Lock()
	reply.Term = rf.Term
	reply.Success = false

	if reply.Term > args.Term {
		rf.mu.Unlock()
		return
	}
	rf.updateElectionTimeOut()

	//判断 node 的节点状态变化
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
	if rf.maxSnapShotIndex >= args.PrevLogIndex {
		rf.mu.Unlock()
		return
	}
	//从日志判断本次 snapshot 是否是过期数据，从而不用 snapshot
	//TODO 发送消息到 state machine
	entry := ApplyMsg{SnapShot: args.SnapShot, LatestIndex: args.PrevLogIndex, LatestTerm: args.PrevLogTerm}
	rf.applyCh <- entry
	reply.Success = true //由 server 判断是否要做一些事情～
	rf.mu.Unlock()
	return
}

func sendSnapShotToPeer(rf *Raft, peerIndex int) {
	rf.mu.Lock()
	if rf.State != STATE_LEADER {
		rf.mu.Unlock()
		return
	}
	Dlog("node: %d will send SnapShot to %d, lastest entry index: %d, term: %d@%d\n",
		rf.me, peerIndex, rf.maxSnapShotIndex, rf.maxSnapShotTerm, MicroSecondNow())
	args := AppendEntryArgs{
		Term:         rf.Term,
		LeaderId:     rf.me,
		PrevLogIndex: rf.maxSnapShotIndex,
		PrevLogTerm:  rf.maxSnapShotTerm,
		SnapShot:     rf.persister.ReadSnapshot(),
	}
	var reply AppendEntryReply
	rf.mu.Unlock()

	if ok := rf.sendInstallSnapShot(peerIndex, &args, &reply); ok {
		rf.mu.Lock()
		if reply.Success == true {
			rf.nextIndex[peerIndex] = args.PrevLogIndex + 1
		}
		rf.mu.Unlock()
	}
}
