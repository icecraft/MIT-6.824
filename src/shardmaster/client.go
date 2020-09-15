package shardmaster

//
// Shardmaster clerk.
//

import (
	"fmt"
	"sync"
	"time"

	"../labrpc"
)

var (
	clerkId = 1
	mu      sync.Mutex
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	ID           int
	SerialNumber int64
	mu           sync.Mutex
	RaftLeaderID int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.RaftLeaderID = 0
	ck.SerialNumber = 1
	mu.Lock()
	ck.ID = clerkId
	clerkId++
	mu.Unlock()
	return ck
}

func (ck *Clerk) getUpdateSN() int64 {
	var ret int64
	ck.mu.Lock()
	ret = ck.SerialNumber
	ck.SerialNumber++
	ck.mu.Unlock()
	return ret
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:          num,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	ck.mu.Lock()
	n := ck.RaftLeaderID
	ck.mu.Unlock()

	for {
		var reply QueryReply
		if ok := ck.servers[n].Call("ShardMaster.Query", &args, &reply); ok {
			switch reply.Err {
			case ErrWrongLeader, ErrStaleLeader:
				n = (n + 1) % len(ck.servers)
			default:
				ck.mu.Lock()
				ck.RaftLeaderID = n
				ck.mu.Unlock()
				return reply.Config
			}
		} else {
			n = (n + 1) % len(ck.servers)
		}
		time.Sleep(OpInterval) // 可能某一轮还在选主，所以最好 sleep 一段时间再去请求
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		Servers:      servers,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}
	ck.moveJoinLeaveOP(&args, "ShardMaster.Join")
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs:         gids,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	ck.moveJoinLeaveOP(&args, "ShardMaster.Leave")
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		GID:          gid,
		Shard:        shard,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	ck.moveJoinLeaveOP(&args, "ShardMaster.Move")
}

func (ck *Clerk) moveJoinLeaveOP(args interface{}, RPCOp string) {
	ck.mu.Lock()
	n := ck.RaftLeaderID
	ck.mu.Unlock()
	for {
		var reply JoinLeaveMoveReply
		if ok := ck.servers[n].Call(RPCOp, args, &reply); ok {
			switch reply.Err {
			case ErrWrongLeader, ErrStaleLeader:
				n = (n + 1) % len(ck.servers)
			default:
				ck.mu.Lock()
				ck.RaftLeaderID = n
				ck.mu.Unlock()
				return
			}
		} else {
			n = (n + 1) % len(ck.servers)
		}
		time.Sleep(OpInterval) // 可能某一轮还在选主，所以最好 sleep 一段时间再去请求
	}
}
