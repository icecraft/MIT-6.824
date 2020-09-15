package kvraft

import (
	"../labrpc"
	"../raft"

	"bytes"
	"encoding/gob"
	"fmt"
	// "sync"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
	log "github.com/sirupsen/logrus"
)

var (
	checkInternal = 50 * time.Millisecond
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

type KVServer struct {
	mu deadlock.Mutex
	// mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

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

func (kv *KVServer) getHistoryKey(clientId int, SerialNumber int64) string {
	return fmt.Sprintf("%d/%d", clientId, SerialNumber)
}

//
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
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
		time.Sleep(checkInternal)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

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

		time.Sleep(checkInternal)
	}
}

func (kv *KVServer) readLog(key string) (string, bool) {
	val, existed := kv.log0[key]
	return val, existed
}

func (kv *KVServer) writeLog(key, value string) {
	kv.log0[key] = value
}

func (kv *KVServer) shrinkHistoryLog() {
	nHistory := make(map[string]Record)
	for key, val := range kv.clientSn {
		historyKey := kv.getHistoryKey(key, val)
		if v, ok := kv.history[historyKey]; ok {
			nHistory[historyKey] = v
		}
	}
	kv.history = nHistory
}

func (kv *KVServer) mayLogCompaction() {
	// 该配置表明不需要 log compaction
	go func() {
		kv.mu.Lock()
		if kv.maxraftstate == -1 || kv.maxraftstate > kv.rf.SateSize() || kv.latestSnapShotIndex >= kv.maxIndexInState {
			kv.mu.Unlock()
			return
		}

		if !kv.inLogCompaction {
			kv.inLogCompaction = true
			kv.shrinkHistoryLog()
			w := new(bytes.Buffer)
			e := gob.NewEncoder(w)
			e.Encode(kv.log0)
			e.Encode(kv.history)
			e.Encode(kv.clientSn)
			e.Encode(kv.maxIndexInState)
			kv.snapshot = w.Bytes()
			kv.snapshotIndex = kv.maxIndexInState
			Dlog("[prepare] me: %d, index: %s, map: %v\n", kv.me, Red(kv.snapshotIndex), kv.log0)
			kv.mu.Unlock()
			return
		} else if kv.inLogCompaction && kv.latestSnapShotIndex > kv.snapshotIndex {
			kv.inLogCompaction = false
			kv.mu.Unlock()
			return
		} else if kv.inLogCompaction && kv.maxIndexInState > kv.snapshotIndex {
			dropRet := kv.rf.DropBeforeIndex(kv.snapshotIndex, false, kv.snapshot)
			if dropRet == raft.NO_NEED_TO_DROP_RAFT_STATE {
				kv.inLogCompaction = false
				kv.mu.Unlock()
				return
			} else if dropRet == raft.DROP_RAFT_STATE_FAILED {
				kv.mu.Unlock()
				return
			}
			kv.inLogCompaction = false
			kv.latestSnapShotIndex = kv.snapshotIndex
			kv.mu.Unlock()
			Dlog("[done] me: %d, index: %s, End Log Compaction \n", kv.me, Red(kv.snapshotIndex))
		} else {
			kv.mu.Unlock()
		}
	}()
}

func (kv *KVServer) readSnapShot() {
	data := kv.rf.ReadSnapShot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	kv.mu.Lock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.log0)
	d.Decode(&kv.history)
	d.Decode(&kv.clientSn)
	d.Decode(&kv.latestSnapShotIndex)
	kv.maxIndexInState = kv.latestSnapShotIndex
	Dlog("me: %d, load log from map: %v\n", kv.me, kv.log0)
	kv.mu.Unlock()
}

func (kv *KVServer) applySnapShot(data []byte) {
	if kv.inLogCompaction {
		kv.inLogCompaction = false
		kv.snapshot = nil
		kv.snapshotIndex = -1
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	kv.log0 = make(map[string]string)
	kv.history = make(map[string]Record)
	kv.clientSn = make(map[int]int64)
	d.Decode(&kv.log0)
	d.Decode(&kv.history)
	d.Decode(&kv.clientSn)
	d.Decode(&kv.latestSnapShotIndex)
	kv.maxIndexInState = kv.latestSnapShotIndex

}

//
func (kv *KVServer) Persiste() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if atomic.LoadInt32(&kv.dead) > 0 {
				break
			}
			kv.mayLogCompaction()
		case val := <-kv.applyCh:
			p, _ := val.Command.(Op)
			historyKey := kv.getHistoryKey(p.ClientID, p.SerialNumber)
			Dlog("me: %d, recv msg: %v\n", kv.me, p)
			kv.mu.Lock()

			// load data from snapshot
			if val.SnapShot != nil && len(val.SnapShot) > 0 && val.LatestIndex > kv.snapshotIndex {
				if 0 == kv.rf.MaySaveSnapShot(val.SnapShot, val.LatestIndex, val.LatestTerm) {
					kv.applySnapShot(val.SnapShot)
					Dlog("me: %d, save Update from snapshot, index: %d, term: %d\n", kv.me, val.LatestIndex, val.LatestTerm)
				}
				kv.mu.Unlock()
				continue
			}
			if kv.clientSn[p.ClientID] != p.SerialNumber-1 {
				kv.mu.Unlock()
				continue
			} else {
				kv.clientSn[p.ClientID] = p.SerialNumber
			}

			Dlog("me: %d, [%s]: key: %s, value:%s, Index: %v\n", kv.me, p.OpType, p.Key, p.Value, val.Index)
			kv.maxIndexInState = val.Index
			t := Record{}
			switch p.OpType {
			case GetOp:
				Dlog("[GET] me: %d, key: %s\n", kv.me, p.Key)
				v, existed := kv.readLog(p.Key)
				if existed {
					t.Value = v
				} else {
					t.Err = ErrNoKey
				}
			case PutOp:
				kv.writeLog(p.Key, p.Value)
				// Dlog("[PUT] me: %d, key: %s, value: %s\n", kv.me, p.Key, p.Value)
			case AppendOp:
				v, existed := kv.readLog(p.Key)
				nvalue := p.Value
				if existed {
					nvalue = fmt.Sprintf("%s%s", v, p.Value)
				}
				kv.writeLog(p.Key, nvalue)
				Dlog("[Append] me: %d, key: %s, value:%s\n", kv.me, p.Key, nvalue)
			default:
				Dlog("Unkown op: %s @%d\n", p.OpType, MicroSecondNow())
			}
			kv.history[historyKey] = t
			kv.mu.Unlock()
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.AddInt32(&kv.dead, 1)
}

//
func (kv *KVServer) Killed() bool {
	return atomic.LoadInt32(&kv.dead) > 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

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

	go kv.Persiste()
	return kv
}
