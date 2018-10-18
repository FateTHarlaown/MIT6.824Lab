package shardmaster

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

type WaitingOp struct {
	WaitCh  chan bool
	ClerkId uint64
	OpSeq   uint64
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs  []Config // indexed by config num
	OpSeqMap map[uint64]uint64
	waitOps  map[int][]*WaitingOp
}

type Op struct {
	// Your data here.
	Type    string
	ClerkId uint64
	Seq     uint64
	Args    interface{}
}

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const TIMEOUT = 600

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.Seq = args.OpSeq
	op.Type = JOIN
	op.Args = *args
	reply.WrongLeader, reply.Err = sm.dealNoDateRequest(op)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.Seq = args.OpSeq
	op.Type = LEAVE
	op.Args = *args
	reply.WrongLeader, reply.Err = sm.dealNoDateRequest(op)

}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.Seq = args.OpSeq
	op.Type = MOVE
	op.Args = *args
	reply.WrongLeader, reply.Err = sm.dealNoDateRequest(op)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{}
	op.ClerkId = args.ClerkId
	op.Seq = args.OpSeq
	op.Type = QUERY
	op.Args = *args
	DPrintf("sm %v query handler call raft start op: %v", sm.me, op)
	index, _, isLeader := sm.rf.Start(op)
	DPrintf("sm %v query request handler call raft start finish, op: %v", sm.me, op)
	waiter := WaitingOp{
		WaitCh:  make(chan bool, 1),
		ClerkId: args.ClerkId,
		OpSeq:   args.OpSeq,
	}
	sm.mu.Lock()
	sm.waitOps[index] = append(sm.waitOps[index], &waiter)
	sm.mu.Unlock()

	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	var ok bool
	if !isLeader {
		reply.WrongLeader = true
	} else {
		reply.WrongLeader = false
		select {
		case ok = <-waiter.WaitCh:
			if ok {
				sm.mu.Lock()
				reply.Config = sm.getConfigCopy(args.Num)
				reply.Err = OK
				sm.mu.Unlock()
			} else {
				reply.Err = ErrFailed
			}
		case <-timer.C:
			reply.Err = ErrTimeout
		}
	}
}

func (sm *ShardMaster) dealNoDateRequest(op Op) (wrongLeader bool, err Err) {
	DPrintf("sm %v nodata request handler call raft start op: %v", sm.me, op)
	index, _, isLeader := sm.rf.Start(op)
	DPrintf("sm %v nodata request handler call raft start finish, op: %v", sm.me, op)
	waiter := WaitingOp{
		WaitCh:  make(chan bool, 1),
		ClerkId: op.ClerkId,
		OpSeq:   op.Seq,
	}
	sm.mu.Lock()
	sm.waitOps[index] = append(sm.waitOps[index], &waiter)
	sm.mu.Unlock()

	var ok bool
	timer := time.NewTimer(time.Millisecond * TIMEOUT)
	if !isLeader {
		wrongLeader = true
	} else {
		wrongLeader = false
		select {
		case ok = <-waiter.WaitCh:
			if ok {
				err = OK
			} else {
				err = ErrFailed
			}
		case <-timer.C:
			err = ErrTimeout
		}
	}

	sm.mu.Lock()
	delete(sm.waitOps, index)
	sm.mu.Unlock()

	return wrongLeader, err
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(LeaveArgs{})
	gob.Register(MoveArgs{})
	gob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.OpSeqMap = make(map[uint64]uint64)
	sm.waitOps = make(map[int][]*WaitingOp)
	go func() {
		for {
			msg := <-sm.applyCh
			sm.ExeuteApplyMsg(msg)
		}
	}()

	return sm
}

func (sm *ShardMaster) ExeuteApplyMsg(msg raft.ApplyMsg) {
	DPrintf("sm:%v Enter Exe msg:%v", sm.me, msg)
	sm.mu.Lock()
	defer sm.mu.Unlock()
	op, ok := msg.Command.(Op)
	DPrintf("sm:%v get op:%v", sm.me, op)
	if !ok {
		return
	}

	DPrintf("sm:%v OpSeqMap:%v", sm.me, sm.OpSeqMap)
	if seq, ok := sm.OpSeqMap[op.ClerkId]; !ok || seq < op.Seq {
		switch op.Type {
		case JOIN:
			joinArgs, ok := op.Args.(JoinArgs)
			DPrintf("ExeMsg sm:%v trans JoinArgs:%v, ok:%v", sm.me, joinArgs, ok)
			if ok {
				sm.applyJoin(joinArgs)
			}

		case LEAVE:
			leavArgs, ok := op.Args.(LeaveArgs)
			if ok {
				sm.applyLeave(leavArgs)
			}

		case MOVE:
			moveArgs, ok := op.Args.(MoveArgs)
			if ok {
				sm.applyMove(moveArgs)
			}
			// QUERY need do nothing here
		}

		sm.OpSeqMap[op.ClerkId] = op.Seq
	}

	if waiters, ok := sm.waitOps[msg.Index]; ok {
		for _, w := range waiters {
			if w.ClerkId == op.ClerkId && w.OpSeq == op.Seq {
				w.WaitCh <- true
			} else {
				w.WaitCh <- false
			}
		}
	}
	DPrintf("sm: %v Exe msg %v finish!!", sm.me, msg)
}

func (sm *ShardMaster) applyJoin(args JoinArgs) {
	newConfig := sm.getConfigCopy(-1)
	DPrintf("applyJoin: sm %v latest config: %v", sm.me, newConfig)
	newGid := make([]int, 0)
	for gid, server := range args.Servers {
		if s, ok := newConfig.Groups[gid]; ok {
			for _, ns := range server {
				tmp := make([]string, 0)
				if !existServer(s, ns) {
					tmp = append(tmp, ns)
				}
				s = append(s, tmp...)
			}
		} else {
			newConfig.Groups[gid] = append([]string{}, server...)
			newGid = append(newGid, gid)
		}
	}
	DPrintf("applyJoin: sm %v new config: %v", sm.me, newConfig)

	//no need to rebalance
	if len(newGid) == 0 {
		newConfig.Num = len(sm.configs)
		sm.configs = append(sm.configs, newConfig)
		return
	}
	//have new gid, rebalane
	var maxUsedGroups int
	if len(newConfig.Groups) > NShards {
		maxUsedGroups = NShards
	} else {
		maxUsedGroups = len(newConfig.Groups)
	}

	shardsPerGid, maxAllowedFullGid := maxShardsPerGid(NShards, maxUsedGroups)
	countTable := make(map[int]int)
	shardsToMove := make([]int, 0)
	DPrintf("apply join prepare move shard, %v shard per gid, maxUsedGroups:%v, maxAllowFull:%v", shardsPerGid, maxUsedGroups, maxAllowedFullGid)
	for i := 0; i < NShards; i++ {
		if newConfig.Shards[i] == 0 {
			shardsToMove = append(shardsToMove, i)
			continue
		}

		n, ok := countTable[newConfig.Shards[i]]
		if ok && n >= shardsPerGid-1 {
			shardsToMove = append(shardsToMove, i)
		} else if !ok {
			countTable[newConfig.Shards[i]] = 1
		} else {
			countTable[newConfig.Shards[i]]++
		}
	}

	DPrintf("applyJoin: sm %v try rebalance, shardToMove:%v, new gid:%v, maxAllowFull:%v, countTable:%v", sm.me, shardsToMove, newGid, maxAllowedFullGid, countTable)
	fullGidNum := 0
	for _, s := range shardsToMove {
		for _, gid := range newGid {
			n, ok := countTable[gid]
			if ok && n < shardsPerGid {
				if n == shardsPerGid-1 &&
					fullGidNum < maxAllowedFullGid {
					fullGidNum++
				} else if n == shardsPerGid-1 {
					continue
				}
				countTable[gid]++
				newConfig.Shards[s] = gid
				break
			} else if !ok {
				countTable[gid] = 1
				newConfig.Shards[s] = gid
				break
			}
		}
		/*
			toGid := i % len(newGid)
			newConfig.Shards[s] = newGid[toGid]
		*/
	}

	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig)
	DPrintf("applyJoin: sm %v config after rebalance: %v", sm.me, newConfig)
	DPrintf("applyJoin: sm %v join finish, my configs: %v", sm.me, sm.configs)
}

func (sm *ShardMaster) applyLeave(args LeaveArgs) {
	newConfig := sm.getConfigCopy(-1)
	DPrintf("applyLeave: sm %v latest config: %v", sm.me, newConfig)
	shardsToBalance := make([]int, 0)
	for _, gidRm := range args.GIDs {
		DPrintf("applyLeave: sm %v try to delete a gid:%v", sm.me, gidRm)
		if _, ok := newConfig.Groups[gidRm]; ok {
			DPrintf("delete gidRm:%v", gidRm)
			delete(newConfig.Groups, gidRm)
		}
		for i := 0; i < NShards; i++ {
			if newConfig.Shards[i] == gidRm {
				newConfig.Shards[i] = 0
				shardsToBalance = append(shardsToBalance, i)
			}
		}
	}
	DPrintf("applyLeave: sm %v config after delete: %v", sm.me, newConfig)

	// rebalance
	existGid := make([]int, 0)
	for g := range newConfig.Groups {
		existGid = append(existGid, g)
	}
	DPrintf("sm %v re shard, exist group:%v, NShard:%v", sm.me, existGid, NShards)
	shardsPerGid, maxAllowFullGid := maxShardsPerGid(NShards, len(existGid))
	DPrintf("sm %v after re shard, shardsPerGid:%v, maxAllowFull:%v", sm.me, shardsPerGid, maxAllowFullGid)
	countTable := make(map[int]int)
	DPrintf("apply Leave prepare move shard, %v shard per gid", shardsPerGid)
	for i := 0; i < NShards; i++ {
		_, ok := countTable[newConfig.Shards[i]]
		if !ok {
			countTable[newConfig.Shards[i]] = 1
		} else if newConfig.Shards[i] != 0 {
			countTable[newConfig.Shards[i]]++
		}
	}
	fullGid := 0
	for _, num := range countTable {
		if num == shardsPerGid {
			fullGid++
		}
	}
	DPrintf("applyLeave: sm %v shards to balance: %v", sm.me, shardsToBalance)
	DPrintf("applyLeave: sm %v exist group: %v", sm.me, existGid)
	DPrintf("applyLeave: sm %v, shardPerGid:%v, maxAllowFull:%v, fullGid:%v ", sm.me, shardsPerGid, maxAllowFullGid, fullGid)
	for _, s := range shardsToBalance {
		for _, gid := range existGid {
			n, ok := countTable[gid]
			if ok && n < shardsPerGid {
				if n == shardsPerGid-1 &&
					fullGid < maxAllowFullGid {
					fullGid++
				} else if n == shardsPerGid-1 {
					continue
				}
				newConfig.Shards[s] = gid
				countTable[gid]++
				break
			} else if !ok {
				newConfig.Shards[s] = gid
				countTable[gid] = 1
				break
			}
		}
	}

	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig)
	DPrintf("applyLeave: sm %v all configs: %v", sm.me, sm.configs)
	DPrintf("applyLeave: sm %v config after rebalance: %v", sm.me, newConfig)
}

func (sm *ShardMaster) applyMove(args MoveArgs) {
	newConfig := sm.getConfigCopy(-1)
	DPrintf("applyMove: sm %v latest config: %v", sm.me, newConfig)
	newConfig.Shards[args.Shard] = args.GID
	newConfig.Num = len(sm.configs)
	sm.configs = append(sm.configs, newConfig)
}

// get a copy of the Config indexed by num
func (sm *ShardMaster) getConfigCopy(num int) Config {
	configCopy := Config{}
	configCopy.Groups = make(map[int][]string)
	pos := num
	if num < 0 || num > NShards {
		pos = len(sm.configs) - 1
	}
	configRaw := sm.configs[pos]
	configCopy.Num = configRaw.Num
	for i := 0; i < NShards; i++ {
		configCopy.Shards[i] = configRaw.Shards[i]
	}
	for g, s := range configRaw.Groups {
		configCopy.Groups[g] = append([]string{}, s...)
	}
	return configCopy
}

func existServer(rawServers []string, s string) bool {
	for _, rs := range rawServers {
		if rs == s {
			return false
		}
	}
	return true
}

func maxShardsPerGid(shardsNum int, gidNum int) (int, int) {
	if gidNum > shardsNum {
		return 1, shardsNum
	}

	maxNum := shardsNum / gidNum
	maxAllowFullGid := gidNum
	if shardsNum%gidNum != 0 {
		maxNum++
		maxAllowFullGid = shardsNum % gidNum
	}

	return maxNum, maxAllowFullGid
}
