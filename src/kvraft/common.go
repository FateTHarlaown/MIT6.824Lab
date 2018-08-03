package raftkv

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrTimeOut      = "ErrTimeOut"
	ErrLeaderChange = "ErrLeaderChange"
)

const (
	GET    = "get"
	APPEND = "Append"
	PUT    = "Put"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	OpType string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId uint64
	OpSeq   uint64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId uint64
	OpSeq   uint64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
