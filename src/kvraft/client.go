package raftkv

import "labrpc"
import "crypto/rand"
import (
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
	ck.leaderId = -1
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
	var nextServer int
	if ck.leaderId == -1 {
		nextServer = 0
	} else {
		nextServer = ck.leaderId
	}

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
		ok := ck.servers[(nextServer+i)%len(ck.servers)].Call("RaftKV.Get", args, reply)
		if ok {
			//只有成功的调用到了leader才认为Get操作成功
			if !reply.WrongLeader {
				ck.leaderId = (nextServer + i) % len(ck.servers)
				if reply.Err == OK {
					ret = reply.Value
				}
				break
			}
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
