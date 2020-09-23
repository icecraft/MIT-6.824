package shardkv

import (
	// "bytes"
	// "encoding/gob"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType       string
	Value        string
	Key          string
	SerialNumber int64
	ClientID     int
}

type Record struct {
	Err   Err
	Value string
}

type ShardKV struct {
	mu           deadlock.Mutex
	me           int
	sm           *shardmaster.Clerk
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// shardmaster config
	config shardmaster.Config

	// Your definitions here.
	dead                int32
	log0                map[string]string
	maxIndexInState     int
	inLogCompaction     bool
	history             map[string]Record
	clientSn            map[int]int64
	latestSnapShotIndex int

	// snapshot
	snapshot      []byte
	snapshotIndex int
}

func (kv *ShardKV) getHistoryKey(clientId int, SerialNumber int64) string {
	return fmt.Sprintf("%d/%d", clientId, SerialNumber)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	Dlog("Get: @%d\n", MicroSecondNow())
	defer Dlog("Ret Get: me: %d args: %v, reply: %v @%d\n", kv.me, args, reply, MicroSecondNow())

	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := kv.getHistoryKey(args.ClientID, args.SerialNumber)
	kv.mu.Lock()
	v, existed := kv.history[historyKey]
	if existed {
		reply.Err = v.Err
		reply.Value = v.Value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mayLogCompaction()

	op := Op{
		OpType:       GetOp,
		Value:        "",
		Key:          args.Key,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		kv.mu.Lock()
		t, existed := kv.history[historyKey]
		if existed {
			reply.Err = t.Err
			reply.Value = t.Value
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}
		time.Sleep(checkInterval)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	Dlog("PUT: @%d\n", MicroSecondNow())
	defer Dlog("Ret PUT: me: %d, args: %v, reply: %v @%d\n", kv.me, args, reply, MicroSecondNow())
	// 不是 leader, 可能重新选一个 leader 来做。但是不能保证 本节点之前是 leader， 但是后面成为了 follower， 后面又成为了 leader
	// client 提交给本节点的数据。不能保证不会同步，也不能保证会同步。
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := kv.getHistoryKey(args.ClientID, args.SerialNumber)
	kv.mu.Lock()
	p, ok := kv.history[historyKey]
	if ok {
		reply.Err = p.Err
		kv.mu.Unlock()
		return
	}
	if kv.clientSn[args.ClientID] > args.SerialNumber {
		log.Infof("PutAppend hitory sn: %d, req sn: %d\n", kv.clientSn[args.ClientID], args.SerialNumber)
	}
	kv.mu.Unlock()
	kv.mayLogCompaction()

	op := Op{
		OpType:       args.Op,
		Value:        args.Value,
		Key:          args.Key,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	// 暂不处理 error 情况，所以 PutAppendRelpy 中的 Err 字段是不会被用到的
	if _, _, isLeader := kv.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 暂时不考虑处理 stale client
	for {
		kv.mu.Lock()
		t, existed := kv.history[historyKey]
		if existed {
			reply.Err = t.Err
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		if _, isLeader := kv.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}

		time.Sleep(checkInterval)
	}
}

//#

func (kv *ShardKV) mayLogCompaction() {
	log.Warn("may log compaction not implemented\n")
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.AddInt32(&kv.dead, 1)
	kv.rf.Kill()
}

//#
func (kv *ShardKV) Killed() bool {
	return atomic.LoadInt32(&kv.dead) > 0
}

//#
func (kv *ShardKV) readSnapShot() {
	log.Warn("ShardKV readSnapShot NOT Implemented\n")
}

//#
func (kv *ShardKV) persiste() {
	log.Warn("ShardKV Persiste NOT Implemented\n")
}

//#
func (kv *ShardKV) pollUpdateConfig() {
	for {
		time.Sleep(ConfigUpdateInterval)
		config := kv.sm.Query(-1)
		kv.mu.Lock()
		kv.config = config
		kv.mu.Unlock()
	}
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

// 从一个 group 发送数据到另外一个 group 只发送 state machine。

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.sm = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.log0 = make(map[string]string)
	kv.history = make(map[string]Record)
	kv.maxIndexInState = -1
	kv.latestSnapShotIndex = -1
	kv.clientSn = make(map[int]int64)
	// read snapShot
	kv.readSnapShot()

	go kv.persiste()
	go kv.pollUpdateConfig()
	return kv
}
