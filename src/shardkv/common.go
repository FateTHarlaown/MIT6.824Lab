package shardkv

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
	ErrTimeOut      = "ErrTimeOut"
	ErrLeaderChange = "ErrLeaderChange"
	ErrDuplicated   = "ErrDuplicated"
)

const (
	GET       = "Get"
	APPEND    = "Append"
	PUT       = "Put"
	CONFIG    = "Config"
	MIGRATION = "Migration"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	OpType string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId   uint64
	OpSeq     uint64
	ConfigNum int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkId   uint64
	OpSeq     uint64
	ConfigNum int
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type MigrationArgs struct {
	Shard     int
	ConfigNum int
}

type MigrationReply struct {
	ConfigNum int
	Shard     int
	KV        map[string]string
	OpSeq     map[uint64]uint64
	Err       Err
}
