package shardkv

import (
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrDupCommand   = "ErrDupCommand"
	ErrStaleLeader  = "ErrStaleLeader"
	ErrInvalidShard = "ErrInvalidShard"
	ErrGidNotFound  = "ErrGidNotFound"

	// op
	GetOp    = "GET"
	PutOp    = "Put"
	AppendOp = "Append"

	// interval
	OpInterval    = 100 * time.Millisecond
	checkInterval = 50 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key          string
	Value        string
	Op           string
	ClientID     int
	SerialNumber int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	ClientID     int
	SerialNumber int64
}

type GetReply struct {
	Err   Err
	Value string
}
