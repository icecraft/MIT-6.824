package kvraft

import (
	"../labrpc"

	crand "crypto/rand"
	// "fmt"
	"math/big"
	"sync"
	"time"
)

var (
	clerkId = 1
	mu      sync.Mutex
)

const (
	NO_RAFT_LEADER = -1
	OpInterval     = 100 * time.Millisecond
)

type Clerk struct {
	servers      []*labrpc.ClientEnd
	ID           int
	SerialNumber int64
	mu           sync.Mutex
	RaftLeaderID int
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
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
	// You'll have to add code here.
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

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// args *GetArgs, reply *GetReply
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:          key,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	ck.mu.Lock()
	n := ck.RaftLeaderID
	ck.mu.Unlock()

	for {
		var reply GetReply
		if ok := ck.servers[n].Call("KVServer.Get", &args, &reply); ok {
			switch reply.Err {
			case ErrWrongLeader, ErrStaleLeader:
				n = (n + 1) % len(ck.servers)
			case "", ErrNoKey:
				ck.mu.Lock()
				ck.RaftLeaderID = n
				ck.mu.Unlock()
				return reply.Value
			}
		} else {
			n = (n + 1) % len(ck.servers)
		}
		time.Sleep(OpInterval) // 可能某一轮还在选主，所以最好 sleep 一段时间再去请求
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// args *PutAppendArgs, reply *PutAppendReply
	args := PutAppendArgs{
		Key:          key,
		Value:        value,
		Op:           op,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	ck.mu.Lock()
	n := ck.RaftLeaderID
	ck.mu.Unlock()

	for {
		var reply PutAppendReply
		if ok := ck.servers[n].Call("KVServer.PutAppend", &args, &reply); ok {
			switch reply.Err {
			case ErrWrongLeader, ErrStaleLeader:
				n = (n + 1) % len(ck.servers)
			case "", ErrNoKey:
				ck.mu.Lock()
				ck.RaftLeaderID = n
				ck.mu.Unlock()
				return
			}
		} else {
			n = (n + 1) % len(ck.servers)
		}
		time.Sleep(OpInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
