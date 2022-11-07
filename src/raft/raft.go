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

import (
	//	"bytes"

	// "// fmt"
	"math"
	"math/rand"

	"sync"
	"sync/atomic"
	"time"

	"bytes"

	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

const minTimeDur = 300
const randTimeDur = 300

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
	nextIndex     []int
	matchIndex    []int
}

type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // thsis peer's index into peers[]
	dead         int32               // set by Kill()
	currentState int
	RLock        *sync.RWMutex
	currentTerm  int     // Latest Term
	votedFor     int     // candidateId that received vote in current term
	log          []Entry // log entries Command for state machine
	commitIndex  int     // index of highest log entry known to be committed
	lastApplied  int     // index of highest log entry applied to state machine
	heartbeat    time.Time
	applyCh      chan ApplyMsg
	nextIndex    []int
	matchIndex   []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func randomTime(min int, ran int) time.Duration {
	randT := min + (rand.Intn(ran))
	return time.Duration(randT) * time.Millisecond
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func min(a int, b int) int {
	if a <= b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a >= b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.RLock.Lock()
	// fmt.Printf("======= Entry: server %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
	if rf.currentTerm <= args.Term {
		//rf.RLock.Lock()
		rf.heartbeat = time.Now()
		reply.Term = args.Term
		rf.currentTerm = args.Term
		rf.persist()
		reply.Success = false
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		// fmt.Println(strconv.Itoa(rf.me) + " : Setting Follower")
		rf.currentState = Follower
		//rf.RLock.Unlock()
		if (len(rf.log) - 1) < args.PrevLogIndex {
			// fmt.Println(strconv.Itoa(rf.me) + " : Exiting due to conflict")
			reply.ConflictIndex = max(len(rf.log)-1, 1)
			// fmt.Printf("======= Exit: server  %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
			rf.RLock.Unlock()
			return
		} else {
			reply.Term = args.Term
			//rf.RLock.Lock()
			//rf.RLock.Unlock()
			if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
				reply.ConflictIndex = 1
				for i := 0; i < args.PrevLogIndex; i++ {
					if rf.log[i].Term == reply.ConflictTerm {
						reply.ConflictIndex = i
						break
					}
				}
				// return
				// fmt.Printf("%v: Prev Terms Conflicting: rfPrev, argPrev: [%v, %v]\n", rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
				// fmt.Printf("%v: AE : Conflict Logs Truncating : Logs: %v\n", rf.me, rf.log)
				rf.log = rf.log[:args.PrevLogIndex]
				rf.persist()
				// fmt.Printf("%v: AE : Conflict Truncated Logs : %v\n", rf.me, rf.log)
				// fmt.Printf("======= Exit: server  %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
				rf.RLock.Unlock()
				return
			}

			// Truncating if existing entries do not match with the new ones
			index := args.PrevLogIndex + 1
			var i int
			for i = 0; i < len(args.Entries); i++ {
				index = args.PrevLogIndex + 1 + i
				// fmt.Printf("%v: AE : loop : [LogIndex, Entries]: [%v, %v]\n", rf.me, index, i)
				if index > len(rf.log)-1 {
					break
				}
				if rf.log[index].Term != args.Entries[i].Term {
					//rf.RLock.Lock()
					//rf.RLock.Unlock()
					// fmt.Printf("%v: AE : [LogIndex, Entries]: [%v, %v] Logs Truncating : Logs: %v\n", rf.me, index, i, rf.log)
					// fmt.Printf("%v: AE Truncating till: %v\n", rf.me, index)
					rf.log = rf.log[:index]
					rf.persist()
					// fmt.Printf("%v: AE Truncated logs: %v\n", rf.me, rf.log)
					break
				}
			}
			// // fmt.Printf("%v: AE : [LogIndex, Entries]: [%v, %v] Logs Truncating : Logs: %v\n", rf.me, index, i, rf.log)
			// // fmt.Printf("%v: AE Truncating till: %v\n", rf.me, index)
			// rf.log = rf.log[:index]
			// // fmt.Printf("%v: AE Truncated logs: %v\n", rf.me, rf.log)

			args.Entries = args.Entries[i:]
			// fmt.Printf("%v: AE Appending Entries: %v\n", rf.me, args.Entries)
			rf.log = append(rf.log, args.Entries...)
			rf.persist()
			// fmt.Printf("%v: AE : Appended Logs : %v\n", rf.me, rf.log)

			// for i := 0; i < len(args.Entries); i++ {
			// 	index := args.PrevLogIndex + 1 + i
			// 	if len(rf.log)-1 >= index {
			// 		if rf.log[index].Term == args.Entries[i].Term {
			// 			continue
			// 		}
			// 	}

			// 	//rf.RLock.Lock()
			// 	// fmt.Println(strconv.Itoa(rf.me) + ": AE : Logs Appended")
			// 	// fmt.Printf("%v: AE : Logs: %v\n", rf.me, rf.log)
			// 	//rf.RLock.Unlock()
			// }

			reply.Success = true

			// fmt.Printf("%v: AE : LeaderCommit, CommitIndex: [%v, %v]\n", rf.me, args.LeaderCommit, rf.commitIndex)
			if args.LeaderCommit > rf.commitIndex {
				//rf.RLock.Lock()
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
				//rf.RLock.Unlock()
			}
			if rf.commitIndex > rf.lastApplied {
				// fmt.Printf("%v: AE : Committing Till : %v\n", rf.me, rf.commitIndex)
				rf.RLock.Unlock()
				rf.applyOnChan()
				//rf.RLock.Unlock()
				return
			}
		}
	} else {
		reply.Term = rf.currentTerm
		reply.Success = false
	}
	// fmt.Printf("======= Exit: server  %d got AppendEntries from leader %d, args: %+v, current log: %v, reply: %+v =======\n", rf.me, args.LeaderId, args, rf.log, reply)
	rf.RLock.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.RLock.RLock()
	term = rf.currentTerm
	if rf.currentState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	// fmt.Println(strconv.Itoa(rf.me) + ": Leader State: " + strconv.Itoa(term) + " " + strconv.FormatBool(isleader))
	rf.RLock.RUnlock()
	// Your code here (2A).
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var rCurrentTerm int
	var rVotedFor int
	var rLogs []Entry
	if d.Decode(&rCurrentTerm) != nil ||
		d.Decode(&rVotedFor) != nil ||
		d.Decode(&rLogs) != nil {
		// fmt.Printf("%v : Radt read persist failed", rf.me)
	} else {
		rf.currentTerm = rCurrentTerm
		rf.votedFor = rVotedFor
		rf.log = rLogs
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (2A, 2B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (2A).
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//wtime := randomTime(13, 1)
	rf.RLock.Lock()
	defer rf.RLock.Unlock()
	//time.Sleep(wtime)
	// fmt.Println("Req: " + strconv.Itoa(args.CandidateId) + " T: " + strconv.Itoa(args.Term) + " -> Rec: " + strconv.Itoa(rf.me) + " T: " + strconv.Itoa(rf.currentTerm) + " #V " + strconv.Itoa(rf.votedFor))
	// fmt.Printf("Args: %v\n", args)

	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	reply.VoteGranted = false

	lastIndex := len(rf.log) - 1

	// if rf.votedFor != -1 || rf.votedFor != args.CandidateId {
	// 	return
	// }

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && ((lastIndex <= args.LastLogIndex && rf.log[lastIndex].Term == args.LastLogTerm) || (rf.log[lastIndex].Term < args.LastLogTerm)) {
		// fmt.Println("========" + strconv.Itoa(rf.me) + " Granting Vote ============> " + strconv.Itoa(args.CandidateId))
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.heartbeat = time.Now()
	}
	//time.Sleep(wtime)
	// Your code here (2A, 2B).
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.RLock.Lock()
	// fmt.Println(strconv.Itoa(rf.me) + ": Got Start Entry")
	isLeader = rf.currentState == Leader
	if !isLeader {
		// fmt.Println(strconv.Itoa(rf.me) + ": Start Returning No Leader")
		rf.RLock.Unlock()
		return index, term, isLeader
	}

	term = rf.currentTerm
	entry := Entry{
		Command: command,
		Term:    term,
	}
	rf.log = append(rf.log, entry)
	rf.persist()
	// fmt.Printf("%v: Start Entry: %v - Log: %v\n", rf.me, entry, rf.log)
	index = len(rf.log) - 1
	// fmt.Println(strconv.Itoa(rf.me) + ": Start Returning Leader")
	rf.RLock.Unlock()

	go rf.LeaderFlow()

	// Your code here (2B).
	// fmt.Println("Returning Start")
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) FollowerFlow() {
	tOut := randomTime(minTimeDur, randTimeDur)
	rf.RLock.RLock()
	// fmt.Printf("Timeout for %v is %v\n", rf.me, tOut)
	rf.RLock.RUnlock()
	time.Sleep(tOut)

	// fmt.Println("waking up")
	rf.RLock.RLock()
	// fmt.Println("Time Returned " + strconv.Itoa(rf.me))
	if time.Now().Sub(rf.heartbeat).Milliseconds() >= tOut.Milliseconds() {
		// fmt.Printf("%v is converting to candidate\n", rf.me)
		if (len(rf.log) - 1) >= 0 {
			// fmt.Println(strconv.Itoa(rf.me) + ": Follower Last Log [Index, Term]: [" + strconv.Itoa(len(rf.log)-1) + "," + strconv.Itoa(rf.log[len(rf.log)-1].Term) + "]")
		}
		rf.RLock.RUnlock()
		// fmt.Println("Loop")
		rf.RLock.Lock()
		rf.currentState = Candidate
		rf.votedFor = -1
		rf.persist()
		rf.RLock.Unlock()
		// fmt.Println(strconv.Itoa(rf.me) + ": Returning")
		return
	} else {
		// fmt.Printf("%v may have just received heartbeat\n", rf.me)
	}
	rf.RLock.RUnlock()
	return
}

func (rf *Raft) CandidateFlow() {
	rf.RLock.RLock()
	// fmt.Println(strconv.Itoa(rf.me) + ": Candidate Flow")
	rf.RLock.RUnlock()
	tOut := randomTime(minTimeDur, randTimeDur)
	Votes := 0
	CompletedVoting := 0
	rf.RLock.RLock()
	NetworkLen := len(rf.peers)
	rf.RLock.RUnlock()
	Consensus := int(math.Floor(float64(NetworkLen)/2) + 1)
	rf.RLock.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	// fmt.Println(strconv.Itoa(rf.me) + ": Pt1")
	rf.RLock.Unlock()
	electionTime := time.Now()
	rf.RLock.RLock() //Write Unlock
	for server := range rf.peers {
		if server == 0 {
			// fmt.Println(strconv.Itoa(rf.me) + ": Pt2 Unlocked: ")
			rf.RLock.RUnlock()
		}
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt3 : Server: " + strconv.Itoa(server))
		if server == rf.me {
			rf.RLock.Lock()
			Votes++
			CompletedVoting++
			rf.RLock.Unlock()
		} else {
			go func(server int) {
				arg := &RequestVoteArgs{}
				rf.RLock.Lock()
				arg.CandidateId = rf.me
				arg.Term = rf.currentTerm
				lastLogIndex := len(rf.log) - 1
				arg.LastLogIndex = lastLogIndex
				arg.LastLogTerm = rf.log[arg.LastLogIndex].Term

				rf.RLock.Unlock()
				reply := RequestVoteReply{}
				out := rf.sendRequestVote(server, arg, &reply)
				// fmt.Println(strconv.Itoa(rf.me) + ": Got the reply from: " + strconv.Itoa(server))
				if !out {
					rf.RLock.Lock()
					CompletedVoting++
					rf.RLock.Unlock()
				}
				if reply.VoteGranted {
					// fmt.Println(strconv.Itoa(rf.me) + ": Got the Vote from: " + strconv.Itoa(server))
					rf.RLock.Lock()
					Votes++
					CompletedVoting++
					rf.RLock.Unlock()
				} else {
					rf.RLock.Lock()
					if reply.Term > arg.Term {
						rf.currentState = Follower
						rf.votedFor = -1
						rf.persist()
					}
					CompletedVoting++
					rf.RLock.Unlock()
					return
				}
			}(server)
		}
	}
	rf.RLock.RLock()
	// fmt.Println(strconv.Itoa(rf.me) + ": Pt4 Checked")
	rf.RLock.RUnlock()
	for {
		time.Sleep(20 * time.Millisecond)
		rf.RLock.RLock()
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt4.1 Reached")
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt4.1 Checked")
		if Votes >= Consensus || CompletedVoting == NetworkLen || time.Now().Sub(electionTime).Milliseconds() >= tOut.Milliseconds() {
			rf.RLock.RUnlock()
			break
		}
		time.Sleep(20 * time.Millisecond)
		rf.RLock.RUnlock()
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt4.2 Checked")
		time.Sleep(20 * time.Millisecond)
	}
	rf.RLock.RLock()
	// fmt.Println(strconv.Itoa(rf.me) + ": Pt5 Out - " + strconv.Itoa(Consensus) + " : " + strconv.Itoa(Votes) + " Status: " + strconv.Itoa(rf.currentState))
	rf.RLock.RUnlock()
	rf.RLock.RLock()
	if Votes >= Consensus {
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt6 Out")
		rf.RLock.RUnlock()
		rf.RLock.Lock()
		rf.currentState = Leader
		lastLogIndex := len(rf.log) - 1
		for peer := range rf.peers {
			rf.nextIndex[peer] = lastLogIndex + 1
			rf.matchIndex[peer] = 0
		}
		rf.RLock.Unlock()
		// fmt.Println(strconv.Itoa(rf.me) + ": Pt7 Out")
		return
	}

	rf.RLock.RUnlock()
	return
}

func (rf *Raft) applyOnChan() {
	rf.RLock.RLock()
	// id := rf.me
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	log := make([]Entry, len(rf.log))
	copy(log, rf.log)
	// fmt.Printf("%v: ApplyLogs: %v\n", id, log)
	rf.RLock.RUnlock()
	for i := lastApplied + 1; i <= commitIndex; i++ {
		// fmt.Printf("%v: Commtted: %v\n", id, log[i])
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      log[i].Command,
			CommandIndex: i,
		}
		rf.RLock.Lock()
		rf.lastApplied = i
		rf.RLock.Unlock()
	}
}

func (rf *Raft) LeaderFlow() {
	rf.RLock.RLock()
	// fmt.Printf("%v: Leader Logs: %v\n", rf.me, rf.log)
	//if (len(rf.log) - 1) >= 0 {
	// fmt.Println(strconv.Itoa(rf.me) + ": Leader Last Log [Index, Term]: [" + strconv.Itoa(len(rf.log)-1) + "," + strconv.Itoa(rf.log[len(rf.log)-1].Term) + "]")
	//}
	// fmt.Println(strconv.Itoa(rf.me) + ": Sending Entries")
	peers := rf.peers
	rf.RLock.RUnlock()

	// fmt.Println(strconv.Itoa(rf.me) + ": Semding RPC Entries Now")

	rf.RLock.RLock()
	for follower := range rf.peers {
		if follower == 0 {
			rf.RLock.RUnlock()
		}
		if follower == rf.me {
			continue
		} else {
			go func(follower int) {
				// fmt.Println(strconv.Itoa(rf.me) + " L : Sending RPC: " + strconv.Itoa(follower))
				rf.RLock.RLock()
				arg := AppendEntriesArgs{}
				arg.LeaderId = rf.me
				arg.Term = rf.currentTerm
				// fmt.Printf("%v L : %v : nextIndex: %v\n", rf.me, follower, rf.nextIndex)
				prevLogIndex := rf.nextIndex[follower] - 1
				arg.PrevLogIndex = prevLogIndex
				arg.PrevLogTerm = rf.log[arg.PrevLogIndex].Term
				arg.Entries = append([]Entry{}, rf.log[rf.nextIndex[follower]:]...)
				// fmt.Printf("%v L : %v : commitIndex: %v\n", rf.me, follower, rf.commitIndex)
				arg.LeaderCommit = rf.commitIndex
				reply := AppendEntriesReply{}
				// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : RPC Variables Populates: [" + strconv.Itoa(arg.Term) + "," + strconv.Itoa(arg.PrevLogIndex) + "," + strconv.Itoa(arg.PrevLogTerm) + "] to : " + strconv.Itoa(follower))
				// fmt.Printf("%v L : %v : Entries: %v\n", rf.me, follower, arg.Entries)
				if rf.currentState != Leader {
					rf.RLock.RUnlock()
					return
				}
				rf.RLock.RUnlock()

				out := rf.sendAppendEntries(follower, &arg, &reply)

				rf.RLock.RLock()
				// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : RPC Recieved Reply: " + strconv.Itoa(follower))
				rf.RLock.RUnlock()
				if !out {
					rf.RLock.RLock()
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : RPC Empty Reply: " + strconv.Itoa(follower))
					rf.RLock.RUnlock()
					return
				}
				// time.Sleep(20 * time.Microsecond)
				rf.RLock.RLock()
				if reply.Term > rf.currentTerm {
					rf.RLock.RUnlock()
					rf.RLock.Lock()
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Stepping Down as Leader")
					rf.currentState = Follower
					rf.currentTerm = reply.Term
					rf.persist()
					rf.RLock.Unlock()
				} else if rf.currentState != Leader {
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Not a Leader anymore")
					rf.RLock.RUnlock()
				} else if arg.Term != rf.currentTerm {
					rf.RLock.RUnlock()
				} else if reply.Success {
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : RPC Incrementing Indices: " + strconv.Itoa(follower))
					rf.RLock.RUnlock()
					rf.RLock.Lock()
					matchIndex := prevLogIndex + len(arg.Entries)
					// if matchIndex > rf.matchIndex[follower] {
					// }
					rf.matchIndex[follower] = matchIndex

					rf.nextIndex[follower] = rf.matchIndex[follower] + 1
					// fmt.Printf("%v L : %v : matchIndex: %v\n", rf.me, follower, rf.matchIndex)
					// fmt.Printf("%v L : %v : nextIndex: %v\n", rf.me, follower, rf.nextIndex)

					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Checking Commit Entries")
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Commit Entry, Log Length: " + strconv.Itoa(rf.commitIndex) + ", " + strconv.Itoa(len(rf.log)))

					for n := rf.commitIndex + 1; n < len(rf.log); n++ {
						count := 1
						if rf.currentTerm == rf.log[n].Term {
							for i := 0; i < len(peers); i++ {
								if i != rf.me && rf.matchIndex[i] >= n {
									count++
								}
							}
						}

						if count > len(peers)/2 && rf.currentState == Leader {
							// fmt.Printf("%v L : %v : Committing Entry at index: %v\n", rf.me, follower, n)
							rf.commitIndex = n
							// fmt.Printf("%v L : %v : CommitIndex: %v\n", rf.me, follower, rf.commitIndex)
						}
					}
					rf.RLock.Unlock()

					rf.RLock.RLock()
					if rf.commitIndex > rf.lastApplied {
						rf.RLock.RUnlock()
						rf.applyOnChan()
					} else {
						rf.RLock.RUnlock()
					}
				} else if !reply.Success {
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : RPC Reducing nextIndex: " + strconv.Itoa(follower))
					rf.RLock.RUnlock()

					//rf.RLock.Lock()
					//rf.nextIndex[follower] = max(rf.nextIndex[follower]-1, 1)
					// rf.matchIndex[follower] = rf.nextIndex[follower] - 1
					//rf.RLock.Unlock()

					rf.RLock.Lock()
					if reply.ConflictTerm < 0 {
						rf.nextIndex[follower] = reply.ConflictIndex
						// rf.matchIndex[follower] = rf.nextIndex[follower] - 1
					} else {
						index := -1
						for i := len(rf.log) - 1; i >= 0; i-- {
							if rf.log[i].Term == reply.ConflictTerm {
								break
							}
						}
						if index < 0 {
							rf.nextIndex[follower] = reply.ConflictIndex
						} else {
							rf.nextIndex[follower] = index
						}
						// rf.matchIndex[follower] = rf.nextIndex[follower] - 1
					}
					rf.RLock.Unlock()
				} else {
					// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Just Unlocking: " + strconv.Itoa(follower))
					rf.RLock.RUnlock()
				}
				rf.RLock.RLock()
				// fmt.Println(strconv.Itoa(rf.me) + " L : " + strconv.Itoa(follower) + " : Returning RPC: " + strconv.Itoa(follower))
				rf.RLock.RUnlock()
			}(follower)
		}
		// time.Sleep(50 * time.Microsecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.RLock.RLock()
		// Id := rf.me
		// fmt.Println("Status of " + strconv.Itoa(rf.me) + ": " + strconv.Itoa(rf.currentState))
		rf.RLock.RUnlock()
		rf.RLock.RLock()
		switch rf.currentState {
		case Follower:
			{
				// fmt.Println("Follower: " + strconv.Itoa(rf.me))
				rf.RLock.RUnlock()
				rf.FollowerFlow()
			}
		case Candidate:
			{
				// fmt.Println("Candidate: " + strconv.Itoa(rf.me))
				rf.RLock.RUnlock()
				rf.CandidateFlow()
			}
		case Leader:
			{
				// fmt.Println("Leader: " + strconv.Itoa(rf.me))
				rf.RLock.RUnlock()
				rf.LeaderFlow()
				time.Sleep(100 * time.Millisecond)
			}
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

func (rf *Raft) initailizeRaft(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) {
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeat = time.Now()
	rf.RLock = new(sync.RWMutex)
	rf.log = make([]Entry, 0)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.initailizeRaft(peers, me, persister, applyCh)
	rf.log = append(rf.log, Entry{Term: 0, Command: nil})
	// fmt.Println("Starting Raft " + strconv.Itoa(me))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
