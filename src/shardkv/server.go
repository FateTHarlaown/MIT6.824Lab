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

const (
	TIMEOUT      = 600
	POLLINTERVAL = 100
	PULLINTERVAL = 100
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	ClerkId uint64
	Seq     uint64
	Args    interface{}
}

type WaitingOp struct {
	WaitCh  chan bool
	ClerkId uint64
	OpSeq   uint64
}

type WaitingShard struct {
	WaitCh chan struct{}
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
	ck            *shardmaster.Clerk
	KVMap         map[string]string
	OpSeqMap      map[uint64]uint64
	waitOps       map[int][]*WaitingOp
	Configs       []shardmaster.Config
	pollTimer     *time.Timer
	pullTimer     *time.Timer
	PullingShards map[int]int //sahrd -> configNum, shards being pull from others
	waitShards    map[int][]*WaitingShard
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if !kv.haveThisShard(shard) {
		reply.Err = ErrWrongGroup
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		kv.mu.Unlock()
		return
	}

	if kv.keyIsPulling(shard) {
		w := WaitingShard{
			WaitCh: make(chan struct{}, 1),
		}
		kv.waitShards[shard] = append(kv.waitShards[shard], &w)
		kv.mu.Unlock()
		DPrintf("shardkv:%v gid:%v Get op wait:%v because key is pulling shard:%v ", kv.me, kv.gid, w, shard)
		<-w.WaitCh
		kv.mu.Lock()
	}
	kv.mu.Unlock()

	op := Op{
		ClerkId: args.ClerkId,
		Seq:     args.OpSeq,
		Type:    GET,
		Args:    *args,
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
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if !kv.haveThisShard(shard) {
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}

	if kv.keyIsPulling(shard) {
		w := WaitingShard{
			WaitCh: make(chan struct{}, 1),
		}
		kv.waitShards[shard] = append(kv.waitShards[shard], &w)
		kv.mu.Unlock()
		<-w.WaitCh
		kv.mu.Lock()
	}
	kv.mu.Unlock()

	op := Op{
		ClerkId: args.ClerkId,
		Seq:     args.OpSeq,
		Type:    args.OpType,
		Args:    *args,
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
	gob.Register(GetArgs{})
	gob.Register(PutAppendArgs{})
	gob.Register(shardmaster.Config{})
	gob.Register(MigrationArgs{})
	gob.Register(MigrationReply{})

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
	kv.Configs = make([]shardmaster.Config, 1)
	kv.Configs[0].Groups = map[int][]string{}
	kv.PullingShards = make(map[int]int)
	kv.pollTimer = time.NewTimer(time.Millisecond * POLLINTERVAL)
	kv.pullTimer = time.NewTimer(time.Millisecond * PULLINTERVAL)
	kv.waitShards = make(map[int][]*WaitingShard)
	kv.ReadSnapshot()

	go kv.startExeuteMsg()
	go kv.startPoll()
	go kv.startPull()

	return kv
}

func (kv *ShardKV) startExeuteMsg() {
	for {
		msg := <-kv.applyCh
		kv.ExcuteApplyMsg(msg)
	}
}

func (kv *ShardKV) startPoll() {
	for {
		<-kv.pollTimer.C
		DPrintf("shardkv:%v gid:%v poll master", kv.me, kv.gid)
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader ||
			len(kv.PullingShards) != 0 {
			kv.pollTimer.Reset(time.Millisecond * POLLINTERVAL)
			kv.mu.Unlock()
			//DPrintf("shardkv:%v gid:%v poll master but is not raft leader", kv.me, kv.gid)
			continue
		}
		newConfigNum := len(kv.Configs)
		kv.mu.Unlock()
		newConfig := kv.getConfigFromMaster(newConfigNum)
		DPrintf("shardkv:%v gid:%v poll master, and get %v", kv.me, kv.gid, newConfig)
		if newConfig.Num == newConfigNum {
			DPrintf("shardkv:%v gid:%v to start the newConfig %v", kv.me, kv.gid, newConfig)
			op := Op{
				ClerkId: 0,
				Seq:     0,
				Type:    CONFIG,
				Args:    newConfig,
			}
			kv.rf.Start(op)
		}
		kv.pollTimer.Reset(time.Millisecond * POLLINTERVAL)
	}

}

func (kv *ShardKV) startPull() {
	for {
		<-kv.pullTimer.C
		kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); !isLeader ||
			len(kv.PullingShards) == 0 {
			kv.pullTimer.Reset(time.Millisecond * PULLINTERVAL)
			kv.mu.Unlock()
			continue
		}

		count := 0
		ch := make(chan struct{})
		DPrintf("shardkv:%v gid:%v start to pull, pulling shards:%v", kv.me, kv.gid, kv.PullingShards)
		for shard, oldConfigNum := range kv.PullingShards {
			go func(s int, c int) {
				kv.pullShard(s, kv.Configs[c])
				ch <- struct{}{}
			}(shard, oldConfigNum)
			count++
		}
		kv.mu.Unlock()

		for count > 0 {
			<-ch
			count--
		}
		kv.pullTimer.Reset(time.Millisecond * PULLINTERVAL)
	}
}

func (kv *ShardKV) ExcuteApplyMsg(msg raft.ApplyMsg) {
	DPrintf("shardkv:%v gid: %v Enter Exe msg:%v", kv.me, kv.gid, msg)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("shardkv:%v gid:%v; Exe msg:%v, my map:%v, my config:%v, my OpSeqMap:%v", kv.me, kv.gid, msg, kv.KVMap, kv.Configs, kv.OpSeqMap)
	if msg.UseSnapshot {
		DPrintf("recieve snapshot update")
		data := msg.Snapshot
		kv.UpdateBySnapshot(data)
	} else {
		//that means this msg is from a dummy log generated when loading snapshot.
		//the purpose is to make Raft.Logs not empty except after just started
		op, succ := msg.Command.(Op)
		if !succ {
			DPrintf("msg Commanfd is not Op! return!")
			return
		}

		if seq, ok := kv.OpSeqMap[op.ClerkId]; !ok || seq < op.Seq {
			DPrintf("shardkv:%v, start to do Op: %v", kv.me, op)
			switch op.Type {
			//'get' don't need to do anything
			case PUT:
				if putArgs, ok := op.Args.(PutAppendArgs); ok {
					kv.KVMap[putArgs.Key] = putArgs.Value
				}
			case APPEND:
				if appendArgs, ok := op.Args.(PutAppendArgs); ok {
					kv.KVMap[appendArgs.Key] = kv.KVMap[appendArgs.Key] + appendArgs.Value
				}
			case CONFIG: //the ClerkId of Config is always 0 and client start from 1,so it can reach here.
				if newConfig, ok := op.Args.(shardmaster.Config); ok {
					DPrintf("kv %v execute add config %v ", kv.me, newConfig)
					if newConfig.Num == len(kv.Configs) {
						kv.addNewConfig(newConfig)
					}
				}
			case MIGRATION:
				if reply, ok := op.Args.(MigrationReply); ok {
					DPrintf("kv %v execute migration: %v", kv.me, reply)
					for k, v := range reply.KV {
						kv.KVMap[k] = v
					}
					delete(kv.PullingShards, reply.Shard)
					if waiters, ok := kv.waitShards[reply.Shard]; ok {
						for _, w := range waiters {
							w.WaitCh <- struct{}{}
						}
						delete(kv.waitShards, reply.Shard)
					}
				}
			}

			// CONFIG change log need not to notify waiters
			if op.ClerkId > 0 && op.Seq > 0 {
				kv.OpSeqMap[op.ClerkId] = op.Seq
			}

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

		DPrintf("shardkv %v Exe msg %v finish!, my kv:%v, my config: %v", kv.me, msg, kv.KVMap, kv.Configs)
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
	encoder.Encode(kv.Configs)
	encoder.Encode(kv.PullingShards)
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
	decoder.Decode(&kv.Configs)
	decoder.Decode(&kv.PullingShards)
}

func (kv *ShardKV) getConfigFromMaster(num int) shardmaster.Config {
	return kv.ck.Query(num)
}

func (kv *ShardKV) haveThisShard(shard int) bool {
	config := kv.Configs[len(kv.Configs)-1]
	return config.Shards[shard] == kv.gid
}

func (kv *ShardKV) keyIsPulling(shard int) bool {
	DPrintf("shardkv %v gid %v check if shard %v is pulling, pulling shards:%v", kv.me, kv.gid, shard, kv.PullingShards)
	_, ok := kv.PullingShards[shard]
	return ok
}

func (kv *ShardKV) addNewConfig(newConfig shardmaster.Config) {
	oldConfig := kv.Configs[len(kv.Configs)-1]
	kv.Configs = append(kv.Configs, newConfig)
	for shard, gid := range newConfig.Shards {
		if kv.gid == gid &&
			kv.gid != oldConfig.Shards[shard] &&
			oldConfig.Shards[shard] != 0 {
			kv.PullingShards[shard] = oldConfig.Num
		}
	}
}

func (kv *ShardKV) pullShard(shard int, oldConfig shardmaster.Config) {
	gid := oldConfig.Shards[shard]
	servers := oldConfig.Groups[gid]
	args := &MigrationArgs{
		Shard:     shard,
		ConfigNum: oldConfig.Num,
	}
	reply := &MigrationReply{}
	for i := 0; ; i++ {
		end := kv.make_end(servers[i%len(servers)])
		ok := end.Call("ShardKV.Migration", args, reply)
		if ok && reply.Err == OK {
			DPrintf("shardkv %v gid %v get Migration reply:%v", kv.me, kv.gid, reply)
			break
		} else if ok && reply.Err == ErrWrongGroup {
			return
		}
	}

	op := Op{
		ClerkId: 0,
		Seq:     0,
		Type:    MIGRATION,
		Args:    *reply,
	}
	kv.rf.Start(op)
}

// the Migration RPC handler
func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// args.ConfigNum must is not the latest config
	if args.ConfigNum >= len(kv.Configs)-1 ||
		kv.Configs[args.ConfigNum].Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
	}

	reply.KV = make(map[string]string)
	for k, v := range kv.KVMap {
		shard := key2shard(k)
		if shard == args.Shard {
			reply.KV[k] = v
		}
	}

	reply.Shard = args.Shard
	reply.Err = OK
	DPrintf("shardkv %v gid %v set a Migration reply:%v", kv.me, kv.gid, reply)

}
