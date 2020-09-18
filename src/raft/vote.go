package raft

import (
	"sync/atomic"
	"time"
	// log "github.com/sirupsen/logrus"
)

func doRquestVote(rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
	var logIndex, logTerm int
	logTerm = -1

	rf.mu.Lock()
	reply.Term = rf.Term
	reply.VoteGranted = false

	logIndex = rf.getLogIndex()
	if logIndex > 0 {
		logTerm = rf.getLogTerm(logIndex)
	}

	Dlog("%s recv request vote from %d, term: %d, args: %v, mlogIndex:%d, mlogTerm:%d, @%d\n",
		Yellow(rf.me), args.CandidateId, args.Term, args, logIndex, logTerm, MicroSecondNow())

	// check term
	if rf.Term > args.Term {
		Dlog("%d %d reject vote for %d, node stale term: %d\n", MicroSecondNow(), rf.me, args.CandidateId, rf.Term)
		rf.mu.Unlock()
		return
	}

	// 如果 term 比我大，我直接变成 follower ！且重置 votedfor （因为新的term， 应该重新选择）
	if args.Term > rf.Term {
		if rf.State != STATE_FOLLOWER {
			rf.updateElectionTimeOut()
			// Dlog("node: %d Got args.Term: %d, my term: %d\n", rf.me, args.Term, term)
			rf.beFollower2(args)
		} else {
			rf.Term = args.Term
			rf.LeaderId = NO_LEADER
			rf.VotedFor = VOTED_FOR_NULL
			rf.persist()
		}
	}

	if rf.State != STATE_FOLLOWER || (rf.VotedFor != VOTED_FOR_NULL && rf.VotedFor != args.CandidateId) || (logTerm == args.LastLogTerm && logIndex > args.LastLogIndex) || logTerm > args.LastLogTerm {
		Dlog("node %d reject voted for %d, my_state: %d, votefor: %d, log_index: %d @%d\n", rf.me, args.CandidateId, rf.State, rf.VotedFor, logIndex, MicroSecondNow())
		rf.mu.Unlock()
		return
	}

	if rf.VotedFor != args.CandidateId {
		rf.persist()
	}
	rf.LeaderId = NO_LEADER
	rf.VotedFor = args.CandidateId
	Dlog("%d voted for %d @%d\n", rf.me, rf.VotedFor, MicroSecondNow())
	//grant it !
	reply.VoteGranted = true
	rf.updateElectionTimeOut()
	rf.mu.Unlock()
	return
}

func doBeCandidate(rf *Raft) {
	ts := MicroSecondNow()
	Dlog("%s: %d %d\n", Blue("becandidate"), rf.me, ts)
	timeExit := make(chan bool, 1)
	go func() {
		select {
		case <-time.After(rf.getElectionTimeOut() - 20*time.Millisecond):
			timeExit <- true
		}
	}()

	llastlogTerm := 0
	llastlogIndex := 0

	rf.mu.Lock()
	rf.State = STATE_CANDIDATER
	rf.VotedFor = rf.me
	rf.LeaderId = -1
	llastlogIndex = rf.getLogIndex()
	if llastlogIndex > 0 {
		llastlogTerm = rf.getLogTerm(llastlogIndex)
	}
	rf.Term++

	args := RequestVoteArgs{
		Term:         rf.Term,
		CandidateId:  rf.me,
		LastLogIndex: llastlogIndex,
		LastLogTerm:  llastlogTerm}
	rf.persist()
	rf.mu.Unlock()

	// 发送 voteMsg
	voted := int32(1)
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}

		rf.mu.Lock()
		if rf.State != STATE_CANDIDATER {
			rf.mu.Unlock()
			Dlog("%d %d exited candidate process\n", MicroSecondNow(), rf.me)
			return
		}
		rf.mu.Unlock()

		go func(peerIndex int) {
			reply := RequestVoteReply{}
			args1 := args
			// Dlog("%d send request vote to %d, term: %d @%d\n", rf.me, peerIndex, rf.Term, MicroSecondNow())
			res := rf.sendRequestVote(peerIndex, &args1, &reply)
			Dlog("%d get reply from %d @%d, res is %s\n", rf.me, peerIndex, MicroSecondNow(), Yellow(reply.VoteGranted))

			if res {
				rf.mu.Lock()
				if reply.Term > rf.Term {
					rf.Term = reply.Term
					rf.State = STATE_FOLLOWER
					Dlog("node: %d convert to follower when send vote msg @%d\n", rf.me, MicroSecondNow())
					rf.LeaderId = -1
					rf.VotedFor = VOTED_FOR_NULL
					rf.persist()
					rf.updateElectionTimeOut()
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
				}
				if reply.VoteGranted {
					atomic.AddInt32(&voted, 1)
				}
			}

		}(i)
	}

	// check data
	stepCheckInterval := 10

	for tl := 0; rf.MaxElectTime > tl; tl += stepCheckInterval {
		select {
		case <-time.After(time.Millisecond * time.Duration(stepCheckInterval)):
			rf.mu.Lock()
			state := rf.State
			rf.mu.Unlock()

			if state != STATE_CANDIDATER {
				break
			}
			if int(atomic.LoadInt32(&voted)) > len(rf.peers)/2 {
				rf.beLeader()
				return
			}
			// Dlog("node: %d, got tickets: %d @%d\n", rf.me, atomic.LoadInt32(&voted), MicroSecondNow())
		case <-timeExit:
			return
		}
	}
}
