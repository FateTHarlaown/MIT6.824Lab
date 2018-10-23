package shardkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

// import "shardmaster"

const TIMEOUT = 600

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	ClerkId uint64
	Seq     uint64
}

type WaitingOp struct {
	WaitCh  chan bool
	ClerkId uint64
	OpSeq   uint64
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	ck       *shardmaster.Clerk
	KVMap    map[string]string
	OpSeqMap map[uint64]uint64
	waitOps  map[int][]*WaitingOp
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if !kv.haveThisKey(args.Key) {
		reply.Err = ErrWrongGroup
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		return
	}

	op := Op{
		Key:     args.Key,
		ClerkId: args.ClerkId,
		Seq:     args.OpSeq,
		Type:    GET,
	}

	DPrintf("shardkv %v gid %v get handler call raft start op: %v", kv.me, kv.gid, op)
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("shardkv %v gid %v get handler call raft start return index:%v isleader:%v", kv.me, kv.gid, index, isLeader)
	waiter := WaitingOp{
		WaitCh:  make(chan bool, 1),
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
	}
	kv.mu.Lock()
	kv.waitOps[index] = append(kv.waitOps[index], &waiter)
	kv.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	var ok bool
	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		select {
		case ok = <-waiter.WaitCh:
			if ok {
				kv.mu.Lock()
				if val, ok := kv.KVMap[args.Key]; ok {
					reply.Err = OK
					reply.Value = val
				} else {
					reply.Err = ErrNoKey
				}
				kv.mu.Unlock()
			} else {
				reply.Err = ErrLeaderChange
			}
		case <-timer.C:
			reply.Err = ErrTimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.waitOps, index)
	kv.mu.Unlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if !kv.haveThisKey(args.Key) {
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{
		Key:     args.Key,
		Value:   args.Value,
		ClerkId: args.ClerkId,
		Seq:     args.OpSeq,
		Type:    args.OpType,
	}
	DPrintf("shardkv %v gid %v PutAppend handler call raft start op: %v", kv.me, kv.gid, op)
	index, _, isLeader := kv.rf.Start(op)
	DPrintf("shardkv %v gid %v PutAppend handler call raft start return index:%v isleader:%v", kv.me, kv.gid, index, isLeader)
	waiter := WaitingOp{
		WaitCh:  make(chan bool, 1),
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
	}
	kv.mu.Lock()
	kv.waitOps[index] = append(kv.waitOps[index], &waiter)
	kv.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	var ok bool
	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		select {
		case ok = <-waiter.WaitCh:
			if ok {
				reply.Err = OK
			} else {
				reply.Err = ErrLeaderChange
			}
		case <-timer.C:
			reply.Err = ErrTimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.waitOps, index)
	kv.mu.Unlock()

}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	DPrintf("start shardkv %v gid %v", kv.me, kv.gid)
	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.ck = shardmaster.MakeClerk(masters)
	kv.KVMap = make(map[string]string)
	kv.OpSeqMap = make(map[uint64]uint64)
	kv.waitOps = make(map[int][]*WaitingOp)
	kv.ReadSnapshot()
	go func() {
		for {
			msg := <-kv.applyCh
			kv.ExcuteApplyMsg(msg)
		}
	}()

	return kv
}

func (kv *ShardKV) ExcuteApplyMsg(msg raft.ApplyMsg) {
	DPrintf("shardkv:%v Enter Exe msg:%v", kv.me, msg)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("shardkv:%v Exe msg:%v", kv.me, msg)
	if msg.UseSnapshot {
		DPrintf("recieve snapshot update")
		data := msg.Snapshot
		kv.UpdateBySnapshot(data)
	} else {
		//that means this msg is from a dummy log generated when loading snapshot.
		//the purpose is to make Raft.Logs not empty except after just started
		op, succ := msg.Command.(Op)
		if !succ {
			return
		}

		if seq, ok := kv.OpSeqMap[op.ClerkId]; !ok || seq < op.Seq {
			switch op.Type {
			//'get' don't need to do anything
			case PUT:
				kv.KVMap[op.Key] = op.Value
			case APPEND:
				kv.KVMap[op.Key] = kv.KVMap[op.Key] + op.Value
			}

			kv.OpSeqMap[op.ClerkId] = op.Seq

			if kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.RaftStateSize() {
				data := kv.encodeKVData(msg.Index, msg.Term)
				go func(index int, data []byte) {
					kv.SaveSnapshot(index, data)
				}(msg.Index, data)
			}
		}

		if waiters, ok := kv.waitOps[msg.Index]; ok {
			for _, w := range waiters {
				if w.ClerkId == op.ClerkId && w.OpSeq == op.Seq {
					w.WaitCh <- true
				} else {
					w.WaitCh <- false
				}
			}
		}

		DPrintf("shardkv %v Exe msg %v finish!, my kv:%v", kv.me, msg, kv.KVMap)
	}
}

func (kv *ShardKV) SaveSnapshot(index int, data []byte) {
	DPrintf("save snapshot")
	DPrintf("call rf save snapshot")
	kv.rf.SaveSnapshot(data, index)
	DPrintf("save snapshot finish")
}

func (kv *ShardKV) encodeKVData(index int, term int) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	encoder.Encode(kv.KVMap)
	encoder.Encode(kv.OpSeqMap)
	encoder.Encode(index)
	encoder.Encode(term)
	data := buf.Bytes()
	return data
}

func (kv *ShardKV) ReadSnapshot() {
	DPrintf("read snapshot")
	data := kv.rf.ReadSnapshot()
	kv.UpdateBySnapshot(data)
}

func (kv *ShardKV) UpdateBySnapshot(data []byte) {
	DPrintf("update by snapshot")
	if data == nil || len(data) < 1 {
		DPrintf("have no snapshot, return")
	}
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	decoder.Decode(&kv.KVMap)
	decoder.Decode(&kv.OpSeqMap)
}

func (kv *ShardKV) getConfigFromMaster(num int) shardmaster.Config {
	return kv.ck.Query(num)
}

func (kv *ShardKV) haveThisKey(key string) bool {
	shard := key2shard(key)
	config := kv.getConfigFromMaster(-1)
	return config.Shards[shard] == kv.gid
}
