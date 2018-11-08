package shardkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

// import "shardmaster"

const (
	TIMEOUT      = 600
	POLLINTERVAL = 100
	PULLINTERVAL = 250
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
	WaitCh  chan Err
	ClerkId uint64
	OpSeq   uint64
}

type MigrationData struct {
	KVMap    map[string]string
	OpSeqMap map[uint64]uint64
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
	MigratingData map[int]MigrationData
	waitShards    map[int][]*WaitingShard
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("shardkv %v gid %v get RPC handler be callled, my configs%v, my pulling:%v, args:%v",
		kv.me, kv.gid, kv.Configs, kv.PullingShards, args)
	kv.mu.Lock()

	shard := key2shard(args.Key)
	if kv.keyIsPulling(shard) {
		w := WaitingShard{
			WaitCh: make(chan struct{}, 1),
		}
		kv.waitShards[shard] = append(kv.waitShards[shard], &w)
		kv.mu.Unlock()
		DPrintf("shardkv %v gid %v Get RPC start to wait because pulling, args %v, shard %v", kv.me, kv.gid, args, shard)
		timer := time.NewTimer(time.Millisecond * TIMEOUT)
		select {
		case <-w.WaitCh:
			DPrintf("shardkv %v gid %v Get RPC stop wait because notify, args %v, shard %v", kv.me, kv.gid, args, shard)
		case <-timer.C:
			DPrintf("shardkv %v gid %v Get RPC stop wait because timeout, args %v, shard %v", kv.me, kv.gid, args, shard)
			reply.Err = ErrTimeOut
			return
		}

		kv.mu.Lock()
	}

	if !kv.haveThisShard(args.ConfigNum, shard) {
		reply.Err = ErrWrongGroup
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		kv.mu.Unlock()
		return
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
		WaitCh:  make(chan Err, 1),
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
	}
	kv.mu.Lock()
	kv.waitOps[index] = append(kv.waitOps[index], &waiter)
	kv.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	var result Err
	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		select {
		case result = <-waiter.WaitCh:
			if result == OK {
				kv.mu.Lock()
				if val, ok := kv.KVMap[args.Key]; ok {
					reply.Err = OK
					reply.Value = val
				} else {
					reply.Err = ErrNoKey
				}
				kv.mu.Unlock()
			} else {
				reply.Err = result
			}
		case <-timer.C:
			reply.Err = ErrTimeOut
		}
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("shardkv %v gid %v Put RPC handler be callled, my configs%v, my pulling:%v, args:%v",
		kv.me, kv.gid, kv.Configs, kv.PullingShards, args)
	kv.mu.Lock()

	shard := key2shard(args.Key)
	if kv.keyIsPulling(shard) {
		w := WaitingShard{
			WaitCh: make(chan struct{}, 1),
		}
		kv.waitShards[shard] = append(kv.waitShards[shard], &w)
		kv.mu.Unlock()
		DPrintf("shardkv %v gid %v Put RPC start to wait because pulling, args %v, shard %v", kv.me, kv.gid, args, shard)
		timer := time.NewTimer(time.Millisecond * TIMEOUT)
		select {
		case <-w.WaitCh:
			DPrintf("shardkv %v gid %v Put RPC stop wait because notify, args %v, shard %v", kv.me, kv.gid, args, shard)
		case <-timer.C:
			DPrintf("shardkv %v gid %v Put RPC stop wait because timeout, args %v, shard %v", kv.me, kv.gid, args, shard)
			reply.Err = ErrTimeOut
			return
		}
		kv.mu.Lock()
	}

	if !kv.haveThisShard(args.ConfigNum, shard) {
		DPrintf("shardkv %v gid %v don't have key %v", kv.me, kv.gid, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
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
		WaitCh:  make(chan Err, 1),
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
	}
	kv.mu.Lock()
	kv.waitOps[index] = append(kv.waitOps[index], &waiter)
	kv.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	var result Err
	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		select {
		case result = <-waiter.WaitCh:
			if result == OK {
				reply.Err = OK
			} else {
				reply.Err = result
			}
		case <-timer.C:
			reply.Err = ErrTimeOut
		}
	}
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
	gob.Register(MigrationData{})

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
	kv.MigratingData = make(map[int]MigrationData)
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
		if _, isLeader := kv.rf.GetState(); !isLeader {
			kv.pollTimer.Reset(time.Millisecond * POLLINTERVAL)
			DPrintf("shardkv:%v gid:%v poll master but can't my configs %v my pulling %v", kv.me, kv.gid, kv.Configs, kv.PullingShards)
			continue
		}

		kv.mu.Lock()
		if len(kv.PullingShards) != 0 {
			kv.pollTimer.Reset(time.Millisecond * POLLINTERVAL)
			DPrintf("shardkv:%v gid:%v poll master but can't my configs %v my pulling %v", kv.me, kv.gid, kv.Configs, kv.PullingShards)
			kv.mu.Unlock()
			continue
		}
		newConfigNum := len(kv.Configs)
		kv.mu.Unlock()

		newConfig := kv.getConfigFromMaster(newConfigNum)
		DPrintf("shardkv:%v gid:%v poll master, and get %v, my detail info:%v", kv.me, kv.gid, newConfig, kv)
		if newConfig.Num == newConfigNum {
			DPrintf("shardkv:%v gid:%v to start the newConfig %v", kv.me, kv.gid, newConfig)
			op := Op{
				ClerkId: 0,
				Seq:     0,
				Type:    CONFIG,
				Args:    newConfig,
			}
			index, term, leader := kv.rf.Start(op)
			DPrintf("shardkv:%v gid:%v start a config op:%v and result:%v %v %v", kv.me, kv.gid, op, index, term, leader)
		}
		kv.pollTimer.Reset(time.Millisecond * POLLINTERVAL)
	}

}

func (kv *ShardKV) startPull() {
	for {
		<-kv.pullTimer.C
		DPrintf("shardkv:%v gid:%v try to pull", kv.me, kv.gid)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			DPrintf("shardkv:%v gid:%v cannot pull, isLeader:%v, my pulling:%v", kv.me, kv.gid, isLeader, kv.PullingShards)
			kv.pullTimer.Reset(time.Millisecond * PULLINTERVAL)
			continue
		}

		kv.mu.Lock()
		if len(kv.PullingShards) == 0 {
			DPrintf("shardkv:%v gid:%v cannot pull, my pulling:%v", kv.me, kv.gid, kv.PullingShards)
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
	DPrintf("shardkv:%v gid:%v Enter Exe msg:%v", kv.me, kv.gid, msg)
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

		var result Err = OK
		if seq, ok := kv.OpSeqMap[op.ClerkId]; !ok || seq < op.Seq {
			DPrintf("shardkv:%v gid %v, start to do Op: %v", kv.me, kv.gid, op)
			result = ErrWrongGroup
			switch op.Type {
			case GET:
				if getArgs, ok := op.Args.(GetArgs); ok &&
					getArgs.ConfigNum == len(kv.Configs)-1 {
					result = OK
				}
			case PUT:
				if putArgs, ok := op.Args.(PutAppendArgs); ok &&
					putArgs.ConfigNum == len(kv.Configs)-1 {
					kv.KVMap[putArgs.Key] = putArgs.Value
					result = OK
				}
			case APPEND:
				if appendArgs, ok := op.Args.(PutAppendArgs); ok &&
					appendArgs.ConfigNum == len(kv.Configs)-1 {
					kv.KVMap[appendArgs.Key] = kv.KVMap[appendArgs.Key] + appendArgs.Value
					result = OK
				}
			case CONFIG: //the ClerkId of Config is always 0 and client start from 1,so it can reach here.
				if newConfig, ok := op.Args.(shardmaster.Config); ok &&
					newConfig.Num == len(kv.Configs) {
					DPrintf("kv %v gid %v execute add config %v ", kv.me, kv.gid, newConfig)
					kv.addNewConfig(newConfig)
					result = OK
				}
			case MIGRATION:
				if reply, ok := op.Args.(MigrationReply); ok &&
					reply.ConfigNum == len(kv.Configs)-2 {
					DPrintf("kv %v gid %v execute migration: %v", kv.me, kv.gid, reply)
					if _, exist := kv.PullingShards[reply.Shard]; exist {
						for k, v := range reply.KV {
							kv.KVMap[k] = v
						}
						for c, s := range reply.OpSeq {
							if seq, ok := kv.OpSeqMap[c]; !ok || seq < s {
								kv.OpSeqMap[c] = s
							}
						}
						delete(kv.PullingShards, reply.Shard)
						if waiters, ok := kv.waitShards[reply.Shard]; ok {
							for _, w := range waiters {
								w.WaitCh <- struct{}{}
							}
							delete(kv.waitShards, reply.Shard)
						}
						result = OK
					}
				}
			}

			// CONFIG change log need not to notify waiters
			if op.ClerkId > 0 && op.Seq > 0 && result == OK {
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
					w.WaitCh <- result
				} else {
					w.WaitCh <- ErrLeaderChange
				}
			}
			delete(kv.waitOps, msg.Index)
		}

		DPrintf("shardkv:%v gid:%v Exe msg %v finish!, my kv:%v, my config: %v", kv.me, kv.gid, msg, kv.KVMap, kv.Configs)
	}
}

func (kv *ShardKV) SaveSnapshot(index int, data []byte) {
	DPrintf("shardkv:%v gid:%v save snapshot", kv.me, kv.gid)
	DPrintf("shardkv:%v gid:%v call rf save snapshot", kv.me, kv.gid)
	kv.rf.SaveSnapshot(data, index)
	DPrintf("shardkv:%v gid:%v save snapshot finish", kv.me, kv.gid)
}

func (kv *ShardKV) encodeKVData(index int, term int) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	DPrintf("shardkv:%v gid:%v encode data my map:%v my config:%v", kv.me, kv.gid, kv.KVMap, kv.Configs)
	encoder.Encode(kv.KVMap)
	encoder.Encode(kv.OpSeqMap)
	encoder.Encode(kv.Configs)
	encoder.Encode(kv.PullingShards)
	encoder.Encode(kv.MigratingData)
	encoder.Encode(index)
	encoder.Encode(term)
	data := buf.Bytes()
	return data
}

func (kv *ShardKV) ReadSnapshot() {
	DPrintf("shardkv:%v gid:%v read snapshot", kv.me, kv.gid)
	data := kv.rf.ReadSnapshot()
	kv.UpdateBySnapshot(data)
}

func (kv *ShardKV) UpdateBySnapshot(data []byte) {
	DPrintf("shardkv:%v gid:%v update by snapshot", kv.me, kv.gid)
	if data == nil || len(data) < 1 {
		DPrintf("have no snapshot, return")
		return
	}
	DPrintf("shardkv:%v gid:%v update by snapshot!", kv.me, kv.gid)
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	tmpKVMap := make(map[string]string)
	err := decoder.Decode(&tmpKVMap)
	if err != nil {
		log.Fatal("read kvmap fatal!")
	}
	kv.KVMap = tmpKVMap

	tmpOpSeq := make(map[uint64]uint64)
	err = decoder.Decode(&tmpOpSeq)
	if err != nil {
		log.Fatal("read OpSeqMap fatal!")
	}
	kv.OpSeqMap = tmpOpSeq

	tmpConf := make([]shardmaster.Config, 0)
	err = decoder.Decode(&tmpConf)
	if err != nil {
		log.Fatal("read config snapshot fatal!")
	}
	kv.Configs = tmpConf

	tmpPulling := make(map[int]int)
	err = decoder.Decode(&tmpPulling)
	if err != nil {
		DPrintf("err:%v", err)
		log.Fatal("read pullingShards snapshot fatal!")
	}
	kv.PullingShards = tmpPulling

	tmpMigrating := make(map[int]MigrationData)
	err = decoder.Decode(&tmpMigrating)
	if err != nil {
		DPrintf("err:%v", err)
		log.Fatal("read MigratingData snapshot fatal!")
	}
	kv.MigratingData = tmpMigrating

	DPrintf("shardkv %v gid %v after update bu snapshot, KV:%v, OpSeq:%v, Configs:%v, Pulling:%v", kv.me, kv.gid, kv.KVMap, kv.OpSeqMap, kv.Configs, kv.PullingShards)
}

func (kv *ShardKV) getConfigFromMaster(num int) shardmaster.Config {
	return kv.ck.Query(num)
}

func (kv *ShardKV) haveThisShard(configNum int, shard int) bool {
	myConfig := kv.Configs[len(kv.Configs)-1]
	if myConfig.Num == configNum {
		return myConfig.Shards[shard] == kv.gid
	}
	return false
}

func (kv *ShardKV) keyIsPulling(shard int) bool {
	DPrintf("shardkv %v gid %v check if shard %v is pulling, pulling shards:%v", kv.me, kv.gid, shard, kv.PullingShards)
	_, ok := kv.PullingShards[shard]
	return ok
}

func (kv *ShardKV) addNewConfig(newConfig shardmaster.Config) {
	oldConfig := kv.Configs[len(kv.Configs)-1]
	kv.Configs = append(kv.Configs, newConfig)
	// prepare the shard to pull from others
	for shard, gid := range newConfig.Shards {
		if kv.gid == gid &&
			kv.gid != oldConfig.Shards[shard] &&
			oldConfig.Shards[shard] != 0 {
			kv.PullingShards[shard] = oldConfig.Num
		}
	}
	// prepare the data for others to pull
	mi := MigrationData{
		KVMap:    make(map[string]string),
		OpSeqMap: make(map[uint64]uint64),
	}
	for key, val := range kv.KVMap {
		shard := key2shard(key)
		if kv.gid != newConfig.Shards[shard] &&
			kv.gid == oldConfig.Shards[shard] {
			mi.KVMap[key] = val
		}
	}
	if len(mi.KVMap) != 0 {
		for id, op := range kv.OpSeqMap {
			mi.OpSeqMap[id] = op
		}
		kv.MigratingData[oldConfig.Num] = mi
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
		nextSvr := i % len(servers)
		DPrintf("shardkv %v begin Migration call args:%v", kv.me, kv.gid, args)
		end := kv.make_end(servers[nextSvr])
		ok := end.Call("ShardKV.Migration", args, reply)
		if ok && reply.Err == OK {
			DPrintf("shardkv %v gid %v get Migration reply:%v", kv.me, kv.gid, reply)
			break
		} else if ok && reply.Err == ErrWrongGroup {
			DPrintf("shardkv %v gid %v Migration call %v get wrong group", kv.me, kv.gid, servers[nextSvr])
			return
		}
	}
	/*
		for _, server := range servers {
			end := kv.make_end(server)
			DPrintf("shardkv %v begin Migration call args:%v", kv.me, kv.gid, args)
			ok := end.Call("ShardKV.Migration", args, reply)
			if ok && reply.Err == OK {
				DPrintf("shardkv %v gid %v get Migration reply:%v", kv.me, kv.gid, reply)
				break
			} else if ok && reply.Err == ErrWrongGroup {
				DPrintf("shardkv %v gid %v Migration call %v get wrong group", kv.me, kv.gid, server)
				return
			}
		}
	*/

	if reply.Err == OK {
		op := Op{
			ClerkId: 0,
			Seq:     0,
			Type:    MIGRATION,
			Args:    *reply,
		}
		_, _, isLeader := kv.rf.Start(op)
		DPrintf("shardkv %v gid %v try start Migration reply:%v, isLeader%v", kv.me, kv.gid, reply, isLeader)
	}
}

// the Migration RPC handler
func (kv *ShardKV) Migration(args *MigrationArgs, reply *MigrationReply) {
	DPrintf("shardkv:%v gid:%v get a Migration call Args:%v", kv.me, kv.gid, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// args.ConfigNum must is not the latest config and this kv must prepared the data
	if args.ConfigNum >= len(kv.Configs)-1 ||
		kv.Configs[args.ConfigNum].Shards[args.Shard] != kv.gid {
		reply.Err = ErrWrongGroup
		DPrintf("shardkv:%v gid:%v set a Migration reply:%v", kv.me, kv.gid, reply)
		return
	}

	DPrintf("shardkv:%v gid:%v start a Migration call Args:%v", kv.me, kv.gid, args)
	reply.KV = make(map[string]string)
	reply.OpSeq = make(map[uint64]uint64)
	if mi, ok := kv.MigratingData[args.ConfigNum]; ok {
		for k, v := range mi.KVMap {
			shard := key2shard(k)
			if shard == args.Shard {
				reply.KV[k] = v
			}
		}
		for c, s := range mi.OpSeqMap {
			reply.OpSeq[c] = s
		}
	}

	reply.Shard = args.Shard
	reply.ConfigNum = args.ConfigNum
	reply.Err = OK
	DPrintf("shardkv:%v gid:%v set a Migration reply:%v", kv.me, kv.gid, reply)
}
