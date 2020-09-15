package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//

const (
	NOT_FOUND_LEFT             = -1
	DROP_RAFT_STATE_FAILED     = -1
	NO_NEED_TO_DROP_RAFT_STATE = -2
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	SnapShot    []byte // ignore for lab2; only used in lab3
	LatestIndex int
	LatestTerm  int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// appendEntry RPC arguments structure.
type AppendEntryArgs struct {
	Term                int
	LeaderId            int
	PrevLogIndex        int
	PrevLogTerm         int
	Entries             []Entry
	CommitIndex         int
	SnapShot            []byte
	WillInstallSnapShot bool
}

type Entry struct {
	Command  interface{}
	Term     int
	Index    int
	SnapShot []byte
}

// appendEntry RPC reply structure.
type AppendEntryReply struct {
	Term      int // 为什么还需要 update it self
	Success   bool
	IndexHint int
}

func leftSearch(log []Entry, index int) int {

	if len(log) == 0 {
		return -1
	}
	lower := 0
	upper := len(log) - 1
	var mid, ret int
	ret = -1
	for upper >= lower {
		if 5 >= upper-lower {
			if index == log[lower].Index {
				ret = lower
				break
			}
			if index > log[lower].Index {
				ret = lower
				lower++
			} else {
				break
			}
		} else {
			mid = (upper + lower) / 2
			if index == log[mid].Index {
				ret = mid
				break
			} else if log[mid].Index > index {
				upper = mid - 1
			} else {
				lower = mid + 1
			}
		}
	}
	// Dlog("index: %d, log: %v, ret: %d\n", index, log, ret)
	return ret
}

func rightSearch(log []Entry, index int) int {
	l := leftSearch(log, index)
	if l == -1 || len(log)-1 == l || log[l].Index == index {
		return l
	}
	return l + 1
}

func search(log []Entry, index int) int {
	pos := leftSearch(log, index)
	if pos == -1 {
		return -1
	}
	if log[pos].Index != index {
		return -1
	}
	return pos
}

type ErrNotFound struct {
	Msg string
}

func (e *ErrNotFound) Error() string {
	return e.Msg
}
