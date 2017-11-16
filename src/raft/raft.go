package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "fmt"

// import "bytes"
// import "encoding/gob"

//the struct for log
type Log struct {
	Command interface{}
	Index   uint64
	Term    uint64
}

//the role for raft
const (
	FOLLOWER  = "follower"
	CANDIDATE = "candidate"
	LEADER    = "leader"
)

//the heartbeat time and timemout time
const (
	HeartBeatTime   = time.Millisecond * 100
	ElectionMinTime = 300
	ElectionMaxTime = 600
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[](should be persisted)
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor    int   //have voted for which candidate in this term(should be persisted)
	currentTerm int   //(should be persisted)
	logs        []Log //(should be persisted)
	//volatile state for all servers
	commitIndex uint64
	lastApplied uint64
	//volatile state for leader
	nextIndex  []uint64
	matchIndex []uint64
	beVoted    int

	grantedFor int

	state string

	applyCh chan ApplyMsg

	timer *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//AppendEntryArgs, for heartbeats and logs
type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Log
	LeaderCommit uint64
}

//the reply for AppendEnty RPC
type AppendEntryReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("I am", rf.me, "i get a vote request:", args, "my term is:", rf.currentTerm)
	will_vote := true
	n := len(rf.logs)
	if n > 0 { // candidate's logs is older than this raft
		if rf.logs[n-1].Term > args.LastLogTerm ||
			(rf.logs[n-1].Term == args.LastLogTerm && rf.logs[n-1].Index > args.LastLogIndex) {
			will_vote = false
		}
	}

	if args.Term < rf.currentTerm { //candidate's term is out of date
		will_vote = false
	}

	if args.Term == rf.currentTerm && rf.grantedFor != -1 { //have voted for itself
		will_vote = false
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = will_vote

	if will_vote == true {
		rf.grantedFor = args.CandidateId
		rf.state = FOLLOWER
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
		}
		rf.persist()
		rf.resetTimer()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//fmt.Println("I am", rf.me, "sending vote request to", server, "args:", args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//append entry RPC handler
func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	//todo: deal with logs in next part
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = FOLLOWER
		rf.persist() //todo: optimize it, only call persist when needed
		rf.resetTimer()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//todo: deal with logs in next part
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.resetTimer()
	} else if reply.Success == true {
		//todo
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) handleVoteReply(reply_args *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == CANDIDATE && reply_args.VoteGranted == true {
		rf.beVoted++
		if rf.beVoted > len(rf.peers)/2 {
			rf.state = LEADER
			rf.resetTimer()
			//send heartbeat to others
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				args := AppendEntryArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: 0,       //complete in next part
					PrevLogTerm:  0,       //complete in next part
					Entries:      []Log{}, //complete in next part
					LeaderCommit: 0,       //complete in next part
				}

				go func(server int, args AppendEntryArgs) {
					reply := AppendEntryReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					if ok != false {
						rf.handleAppendEntriesReply(&reply)
					}
				}(i, args)
			}
		}
	} else if reply_args.VoteGranted == false && reply_args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = reply_args.Term
		rf.persist()
		rf.votedFor = -1
		rf.resetTimer()
	}
}

func (rf *Raft) handleTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		//todo: send heartbeat and append antry
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			args := AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: 0,       //complete in next part
				PrevLogTerm:  0,       //complete in next part
				Entries:      []Log{}, //complete in next part
				LeaderCommit: 0,       //complete in next part
			}
			//fmt.Println("I am", rf.me, "a", rf.state, "I am sending app heart beats, aargs:", args)
			go func(server int, args AppendEntryArgs) {
				reply := AppendEntryReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if ok != false {
					rf.handleAppendEntriesReply(&reply)
				}
			}(i, args)
		}
	} else { // start a leader election

		rf.state = CANDIDATE
		rf.beVoted = 1
		rf.grantedFor = rf.me
		rf.currentTerm++
		rf.persist()

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: 0,
			LastLogTerm:  0,
		}

		log_num := len(rf.logs)
		if log_num > 0 {
			args.LastLogIndex = rf.logs[log_num-1].Index
			args.LastLogIndex = rf.logs[log_num-1].Term
		}

		for i := 1; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}

			go func(sever int, args RequestVoteArgs) {
				//todo: send requestvote to others and deal with the reply
				reply_args := RequestVoteReply{}
				//fmt.Println("I am", rf.me, rf.state, "I am sending Request vote !", args)
				ok := rf.sendRequestVote(sever, &args, &reply_args)
				if ok != false {
					rf.handleVoteReply(&reply_args)
				}
			}(i, args)
		}
	}
	rf.resetTimer()
}

func (rf *Raft) resetTimer() {
	timeOut := time.Duration(HeartBeatTime)
	if rf.state != LEADER {
		timeOut = time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime))
	}
	initchan := make(chan int, 1)
	if rf.timer == nil { //there is no timer, create it
		rf.timer = time.NewTimer(time.Millisecond * 5000)
		go func() {
			<-initchan
			for {
				<-rf.timer.C
				rf.handleTimer()
			}
		}()
	}

	fmt.Println("Reset", rf.me, "timer, it's", rf.state, "dtime:", timeOut)
	rf.timer.Reset(timeOut)
	initchan <- 2
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	fmt.Print("start to init a raft node:  ", me, "      ")
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	// Your initialization code here (2A, 2B, 2C).
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = []Log{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]uint64, len(peers))
	rf.matchIndex = make([]uint64, len(peers))
	rf.state = FOLLOWER
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.resetTimer()

	return rf
}
