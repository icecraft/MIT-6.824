package shardmaster

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sasha-s/go-deadlock"
	// log "github.com/sirupsen/logrus"
	"../labgob"
	"../labrpc"
	"../raft"
)

type Record struct {
	Err    Err
	Config Config
}

type ShardMaster struct {
	// mu      sync.Mutex
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	// Your definitions here.
	dead       int32
	history    map[string]Record
	clientSn   map[int]int64
	joinedGids map[int]bool
}

type Op struct {
	OpType       string
	ArgJoin      JoinArgs // query, join, leave, move
	ArgMove      MoveArgs
	ArgLeave     LeaveArgs
	ArgQuery     QueryArgs
	SerialNumber int64
	ClientID     int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinLeaveMoveReply) {
	// Your code here.
	Dlog("Join: @%d\n", MicroSecondNow())
	defer Dlog("Ret Join: me: %d args: %v, reply: %v @%d\n", sm.me, args, reply, MicroSecondNow())

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := sm.getHistoryKey(args.ClientID, args.SerialNumber)
	sm.mu.Lock()
	v, existed := sm.history[historyKey]
	if existed {
		reply.Err = v.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		OpType:       JoinOp,
		ArgJoin:      *args,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	if _, _, isLeader := sm.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		sm.mu.Lock()
		t, existed := sm.history[historyKey]
		if existed {
			reply.Err = t.Err
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		if _, isLeader := sm.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}
		time.Sleep(checkInterval)
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *JoinLeaveMoveReply) {
	// Your code here.
	// Your code here.
	Dlog("Leave: @%d\n", MicroSecondNow())
	defer Dlog("Ret leave: me: %d args: %v, reply: %v @%d\n", sm.me, args, reply, MicroSecondNow())

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := sm.getHistoryKey(args.ClientID, args.SerialNumber)
	sm.mu.Lock()
	v, existed := sm.history[historyKey]
	if existed {
		reply.Err = v.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		OpType:       LeaveOp,
		ArgLeave:     *args,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	if _, _, isLeader := sm.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		sm.mu.Lock()
		t, existed := sm.history[historyKey]
		if existed {
			reply.Err = t.Err
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		if _, isLeader := sm.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}
		time.Sleep(checkInterval)
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *JoinLeaveMoveReply) {
	// Your code here.
	// Your code here.
	Dlog("Move: @%d\n", MicroSecondNow())
	defer Dlog("Ret Move: me: %d args: %v, reply: %v @%d\n", sm.me, args, reply, MicroSecondNow())

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := sm.getHistoryKey(args.ClientID, args.SerialNumber)
	sm.mu.Lock()
	v, existed := sm.history[historyKey]
	if existed {
		reply.Err = v.Err
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		OpType:       MoveOp,
		ArgMove:      *args,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	if _, _, isLeader := sm.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		sm.mu.Lock()
		t, existed := sm.history[historyKey]
		if existed {
			reply.Err = t.Err
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		if _, isLeader := sm.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}
		time.Sleep(checkInterval)
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	// Your code here.
	Dlog("Query: @%d\n", MicroSecondNow())
	defer Dlog("Ret Query: me: %d args: %v, reply: %v @%d\n", sm.me, args, reply, MicroSecondNow())

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	historyKey := sm.getHistoryKey(args.ClientID, args.SerialNumber)
	sm.mu.Lock()
	v, existed := sm.history[historyKey]
	if existed {
		reply.Err = v.Err
		reply.Config = v.Config
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()

	op := Op{
		OpType:       QueryOp,
		ArgQuery:     *args,
		ClientID:     args.ClientID,
		SerialNumber: args.SerialNumber}

	if _, _, isLeader := sm.rf.Start(op); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	for {
		sm.mu.Lock()
		t, existed := sm.history[historyKey]
		if existed {
			reply.Err = t.Err
			reply.Config = t.Config
			sm.mu.Unlock()
			return
		}
		sm.mu.Unlock()
		if _, isLeader := sm.rf.GetState(); !isLeader {
			reply.Err = ErrStaleLeader
			return
		}
		time.Sleep(checkInterval)
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	atomic.AddInt32(&sm.dead, 1)
	// Your code here, if desired.
}

//
func (sm *ShardMaster) Killed() bool {
	return atomic.LoadInt32(&sm.dead) > 0
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) mayShrinkHistory() {
	// log.Warn(Red("shrink history not implemented\n"))
}

func (sm *ShardMaster) getHistoryKey(clientId int, SerialNumber int64) string {
	return fmt.Sprintf("%d/%d", clientId, SerialNumber)
}

func (sm *ShardMaster) getLatestConfig() Config {
	n := len(sm.configs)
	config := Config{Num: n}
	config.Shards = sm.configs[n-1].Shards
	config.Groups = sm.configs[n-1].Groups
	if nil == config.Groups {
		config.Groups = make(map[int][]string)
	}
	return config
}

//
func (sm *ShardMaster) Persiste() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if atomic.LoadInt32(&sm.dead) > 0 {
				break
			}
			sm.mayShrinkHistory()
		case val := <-sm.applyCh:
			p, _ := val.Command.(Op)
			historyKey := sm.getHistoryKey(p.ClientID, p.SerialNumber)
			Dlog("me: %d, recv msg: %v\n", sm.me, p)

			sm.mu.Lock()
			if sm.clientSn[p.ClientID] != p.SerialNumber-1 {
				sm.mu.Unlock()
				continue
			} else {
				sm.clientSn[p.ClientID] = p.SerialNumber
			}
			Dlog("me: %d, [%s]: Index: %v\n", sm.me, p.OpType, val.Index)

			t := Record{}
			switch p.OpType {
			case JoinOp:
				args := p.ArgJoin
				// 假设，多组 gids 加入时，如果已经加入的则不处理。没有加入的则加入进来
				newGids := make(map[int][]string)
				for key, v := range args.Servers {
					if !sm.joinedGids[key] {
						newGids[key] = v
					}
				}
				if len(newGids) > 0 {
					config := sm.getLatestConfig()
					for key, v := range newGids {
						config.Groups[key] = v
						sm.joinedGids[key] = true
					}
					sm.configs = append(sm.configs, config)
					fmt.Printf("me: %d, new configs %v\n", sm.me, sm.configs)
				} else {
					t.Err = "ErrNoNewGids"
				}
			case MoveOp:
				args := p.ArgMove
				var skipFlag bool
				if 0 > args.Shard || args.Shard > NShards-1 {
					skipFlag = true
					t.Err = ErrInvalidShard
				}
				n := len(sm.configs)
				_, existed := sm.configs[n-1].Groups[args.GID]
				if !existed {
					skipFlag = true
					t.Err = ErrGidNotFound
				}
				if !skipFlag {
					config := sm.getLatestConfig()
					config.Shards[args.Shard] = args.GID
					sm.configs = append(sm.configs, config)
				}

			case LeaveOp:
				args := p.ArgLeave
				gidsWillRemove := make([]int, 0)
				config := sm.getLatestConfig()
				for key := range args.GIDs {
					if _, existed := config.Groups[key]; existed {
						gidsWillRemove = append(gidsWillRemove, key)
					}
				}
				if len(gidsWillRemove) == 0 {
					t.Err = ErrGidNotFound
				} else {
					for _, v := range gidsWillRemove {
						delete(config.Groups, v)
						for i, gid := range config.Shards {
							if gid == v {
								config.Shards[i] = INIT_GID
							}
						}
					}
					sm.configs = append(sm.configs, config)
				}
			case QueryOp:
				args := p.ArgQuery

				if args.Num == -1 {
					if len(sm.configs) > 1 {
						t.Config = sm.configs[len(sm.configs)-1]
					} else {
						t.Err = ErrConfigNum
					}
				} else {
					if args.Num > 0 {
						t.Config = sm.configs[args.Num]
					} else {
						t.Err = ErrConfigNum
					}
				}

			default:
				Dlog("Unkown op: %s @%d\n", p.OpType, MicroSecondNow())
			}
			sm.history[historyKey] = t
			sm.mu.Unlock()
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.configs = make([]Config, 1)
	sm.history = make(map[string]Record)
	sm.clientSn = make(map[int]int64)
	sm.joinedGids = make(map[int]bool)

	//初始化第一份配置
	config := Config{Num: 0}
	var shards [NShards]int
	config.Shards = shards
	sm.configs[0] = config

	go sm.Persiste()
	return sm
}
