package raftkv

import "labrpc"
import "crypto/rand"
import (
	"fmt"
	"math/big"
	"sync/atomic"
)

var ClerkSeq uint64

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id        uint64
	NextOpSeq uint64
	leaderId  int
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
	// You'll have to add code here.
	ck.id = atomic.AddUint64(&ClerkSeq, 1)
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ret := ""
	i := -1
	for {
		i++
		args := &GetArgs{
			Key:     key,
			ClerkId: ck.id,
			OpSeq:   atomic.AddUint64(&ck.NextOpSeq, 1),
		}
		reply := &GetReply{}
		DPrintf("Enter Get: args:%v", args)
		ok := ck.servers[(ck.leaderId+i)%len(ck.servers)].Call("RaftKV.Get", args, reply)
		DPrintf("A RaftKV.Get PRC return args:%v reply:%v", args, reply)
		//只有成功的调用到了leader,并且获取到值或者key不存在才认为Get操作成功
		if ok && !reply.WrongLeader && (reply.Err == OK || reply.Err == ErrNoKey) {
			ck.leaderId = (ck.leaderId + i) % len(ck.servers)
			ret = reply.Value
			break
		}
	}

	return ret
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	i := -1
	for {
		i++
		args := &PutAppendArgs{
			Key:     key,
			Value:   value,
			OpType:  op,
			ClerkId: ck.id,
			OpSeq:   atomic.AddUint64(&ck.NextOpSeq, 1),
		}

		reply := &PutAppendReply{}
		DPrintf("Enter PutAppend: args:%v", args)
		ok := ck.servers[(ck.leaderId+i)%len(ck.servers)].Call("RaftKV.PutAppend", args, reply)
		DPrintf("A RaftKV.PutAppend PRC return args:%v reply:%v", args, reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			ck.leaderId = (ck.leaderId + i) % len(ck.servers)
			break
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("client call:Put  key:%v  values:%v", key, value)
	fmt.Println("client call:Put ", key, " ", value)
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("client call:Append  key:%v  values:%v", key, value)
	ck.PutAppend(key, value, APPEND)
}
