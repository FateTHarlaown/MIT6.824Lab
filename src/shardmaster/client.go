package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"sync/atomic"
	"time"
)

var ClerkSeq uint64

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id       uint64
	opSeq    uint64
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = atomic.AddUint64(&ClerkSeq, 1)
	ck.opSeq = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	nextSeq := atomic.AddUint64(&ck.opSeq, 1)
	args.ClerkId = ck.id
	args.OpSeq = nextSeq
	for i := 0; ; i++ {
		var reply QueryReply
		DPrintf("clinet %v enter Query: args:%v", ck.id, args)
		nextSrv := (ck.leaderId + i) % len(ck.servers)
		ok := ck.servers[nextSrv].Call("ShardMaster.Query", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	nextSeq := atomic.AddUint64(&ck.opSeq, 1)
	args.Servers = servers
	args.ClerkId = ck.id
	args.OpSeq = nextSeq

	for i := 0; ; i++ {
		var reply JoinReply
		DPrintf("clinet %v enter Join: args:%v", ck.id, args)
		nextSrv := (ck.leaderId + i) % len(ck.servers)
		ok := ck.servers[nextSrv].Call("ShardMaster.Join", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	nextSeq := atomic.AddUint64(&ck.opSeq, 1)
	args.GIDs = gids
	args.ClerkId = ck.id
	args.OpSeq = nextSeq

	for i := 0; ; i++ {
		var reply LeaveReply
		DPrintf("clinet %v enter Leave: args:%v", ck.id, args)
		nextSrv := (ck.leaderId + i) % len(ck.servers)
		ok := ck.servers[nextSrv].Call("ShardMaster.Leave", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	nextSeq := atomic.AddUint64(&ck.opSeq, 1)
	args.ClerkId = ck.id
	args.OpSeq = nextSeq

	for i := 0; ; i++ {
		var reply MoveReply
		DPrintf("clinet %v enter move: args:%v", ck.id, args)
		nextSrv := (ck.leaderId + i) % len(ck.servers)
		ok := ck.servers[nextSrv].Call("ShardMaster.Move", args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
