package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"../labrpc"
	"../shardmaster"
)

var (
	clerkID = 1
	mu      sync.Mutex
)

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.

	ID           int
	SerialNumber int64
	mu           sync.Mutex
	RaftLeaderID int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.RaftLeaderID = 0
	ck.SerialNumber = 1
	mu.Lock()
	ck.ID = clerkID
	clerkID++
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
// You will have to modify this function.
//

func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:          key,
		SerialNumber: ck.getUpdateSN(),
		ClientID:     ck.ID}

	n := ck.RaftLeaderID

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			var reply GetReply
			server := ck.make_end(servers[n])
			if ok := server.Call("ShardKV.Get", &args, &reply); ok {
				switch reply.Err {
				case ErrWrongLeader, ErrStaleLeader:
					n = (n + 1) % len(servers)
				case ErrWrongGroup:
					ck.config = ck.sm.Query(-1)
				case "", ErrNoKey:
					ck.RaftLeaderID = n
					return reply.Value
				}
			} else {
				n = (n + 1) % len(servers)
			}
		} else {
			ck.config = ck.sm.Query(-1)
		}
		time.Sleep(OpInterval) // 可能某一轮还在选主，所以最好 sleep 一段时间再去请求
	}
	// You will have to modify this function.
}

//
// shared by Put and Append.
// You will have to modify this function.
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

	n := ck.RaftLeaderID

	for {

		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			var reply PutAppendReply
			server := ck.make_end(servers[n])
			if ok := server.Call("ShardKV.PutAppend", &args, &reply); ok {
				switch reply.Err {
				case ErrWrongLeader, ErrStaleLeader:
					n = (n + 1) % len(servers)
				case ErrWrongGroup:
					ck.config = ck.sm.Query(-1)
				case "", ErrNoKey:
					ck.RaftLeaderID = n
					return
				}
			} else {
				n = (n + 1) % len(servers)
			}
		} else {
			ck.config = ck.sm.Query(-1)
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
