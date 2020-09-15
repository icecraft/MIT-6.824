package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrDupCommand  = "ErrDupCommand"
	ErrStaleLeader = "ErrStaleLeader"
	ErrWrongLeader = "ErrWrongLeader"
	GetOp          = "GET"
	PutOp          = "Put"
	AppendOp       = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key          string
	Value        string
	Op           string // "Put" or "Append"
	SerialNumber int64
	ClientID     int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key          string
	ClientID     int
	SerialNumber int64
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
