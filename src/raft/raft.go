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

	//"fmt"
	"math"
	"math/rand"

	//"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *Persister          // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	currentState int
	RLock        *sync.RWMutex
	currentTerm  int   // Latest Term
	votedFor     int   // candidateId that received vote in current term
	log          []int // log entries Command for state machine
	commitIndex  int   // index of highest log entry known to be committed
	lastApplied  int   // index of highest log entry applied to state machine
	heartbeat    time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func randomTime(min int, ran int) time.Duration {
	randT := min + (rand.Intn(ran))
	return time.Duration(randT) * time.Millisecond
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.RLock.Lock()
	if rf.currentTerm <= args.Term {
		reply.Term = args.Term
		//fmt.Println(strconv.Itoa(rf.me) + " : Setting Follower")
		rf.currentState = Follower
		rf.currentTerm = reply.Term
		rf.heartbeat = time.Now()
		rf.RLock.Unlock()
	} else {
		reply.Term = rf.currentTerm
		rf.RLock.Unlock()
	}
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
	//fmt.Println(strconv.Itoa(rf.me) + ": Leader State: " + strconv.Itoa(term) + " " + strconv.FormatBool(isleader))
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Term        int
	CandidateId int
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
	//fmt.Println(strconv.Itoa(rf.me) + " " + strconv.Itoa(args.CandidateId) + " -> " + strconv.Itoa(args.Term) + " " + strconv.Itoa(rf.currentTerm) + " # " + strconv.Itoa(rf.votedFor))
	if ((rf.currentTerm == args.Term) && (rf.votedFor == -1)) || (args.Term > rf.currentTerm) {
		//fmt.Println("========" + strconv.Itoa(rf.me) + " Granting Vote ============> " + strconv.Itoa(args.CandidateId))
		reply.VoteGranted = true
		reply.Term = args.Term
		rf.currentState = Follower
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.heartbeat = time.Now()
	} else if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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

	// Your code here (2B).

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
	//fmt.Printf("Timeout for %v is %v\n", rf.me, tOut)
	rf.RLock.RUnlock()
	time.Sleep(tOut)

	//fmt.Println("waking up")
	////fmt.Println("Time Returned " + strconv.Itoa(rf.me))
	rf.RLock.RLock()
	if time.Now().Sub(rf.heartbeat).Milliseconds() >= tOut.Milliseconds() {
		//fmt.Printf("%v is converting to candidate\n", rf.me)
		rf.RLock.RUnlock()
		////fmt.Println("Loop")
		rf.RLock.Lock()
		rf.currentState = Candidate
		rf.votedFor = -1
		rf.RLock.Unlock()
		return
	} else {
		//fmt.Printf("%v may have just received heartbeat\n", rf.me)
	}
	rf.RLock.RUnlock()
	return
}

func (rf *Raft) CandidateFlow() {
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
	//fmt.Println(strconv.Itoa(rf.me) + ": Pt1")
	rf.RLock.Unlock()
	electionTime := time.Now()
	rf.RLock.RLock() //Write Unlock
	for server := range rf.peers {
		if server == 0 {
			//fmt.Println(strconv.Itoa(rf.me) + ": Pt2 Unlocked: ")
			rf.RLock.RUnlock()
		}
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt3 : Server: " + strconv.Itoa(server))
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
				rf.RLock.Unlock()
				reply := RequestVoteReply{}
				out := rf.sendRequestVote(server, arg, &reply)
				//fmt.Println(strconv.Itoa(rf.me) + ": Got the reply from: " + strconv.Itoa(server))
				if !out {
					rf.RLock.Lock()
					CompletedVoting++
					rf.RLock.Unlock()
				}
				if reply.VoteGranted {
					//fmt.Println(strconv.Itoa(rf.me) + ": Got the Vote from: " + strconv.Itoa(server))
					rf.RLock.Lock()
					Votes++
					CompletedVoting++
					rf.RLock.Unlock()
				} else {
					rf.RLock.Lock()
					if reply.Term > arg.Term {
						rf.currentState = Follower
						rf.votedFor = -1
					}
					CompletedVoting++
					rf.RLock.Unlock()
					return
				}
			}(server)
		}
	}
	rf.RLock.RLock()
	//fmt.Println(strconv.Itoa(rf.me) + ": Pt4 Checked")
	rf.RLock.RUnlock()
	for {
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt4.1 Reached")
		rf.RLock.RLock()
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt4.1 Checked")
		if Votes >= Consensus || CompletedVoting == NetworkLen || time.Now().Sub(electionTime).Milliseconds() >= tOut.Milliseconds() {
			rf.RLock.RUnlock()
			break
		}
		time.Sleep(20 * time.Millisecond)
		rf.RLock.RUnlock()
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt4.2 Checked")
		time.Sleep(20 * time.Millisecond)
	}
	rf.RLock.RLock()
	//fmt.Println(strconv.Itoa(rf.me) + ": Pt5 Out - " + strconv.Itoa(Consensus) + " : " + strconv.Itoa(Votes) + " Status: " + strconv.Itoa(rf.currentState))
	rf.RLock.RUnlock()
	rf.RLock.RLock()
	if Votes >= Consensus {
		rf.RLock.RUnlock()
		rf.RLock.RLock()
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt6 Out")
		rf.RLock.RUnlock()
		rf.RLock.Lock()
		rf.currentState = Leader
		rf.currentTerm += 1
		rf.RLock.Unlock()
		//fmt.Println(strconv.Itoa(rf.me) + ": Pt7 Out")
		return
	}

	rf.RLock.RUnlock()
	return
}

func (rf *Raft) LeaderFlow() {
	rf.RLock.RLock()
	//fmt.Println(strconv.Itoa(rf.me) + ": Sending Entries")
	rf.RLock.RUnlock()
	term, leader := rf.GetState()
	arg := AppendEntriesArgs{}
	arg.LeaderId = rf.me
	arg.Term = term
	if leader {
		rf.RLock.RLock()
		for follower := range rf.peers {
			if follower == arg.LeaderId {
				continue
			} else {
				go func(follower int) {
					reply := AppendEntriesReply{}
					out := rf.sendAppendEntries(follower, &arg, &reply)
					if !out {
						return
					}
					if reply.Term > term {
						rf.RLock.Lock()
						//fmt.Println(strconv.Itoa(rf.me) + ": Stepping Down as Leader: ")
						rf.currentState = Follower
						rf.currentTerm = reply.Term
						rf.RLock.Unlock()
					}
				}(follower)
			}
			time.Sleep(50 * time.Microsecond)
		}
		rf.RLock.RUnlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.RLock.RLock()
		//Id := rf.me
		//fmt.Println("Status of " + strconv.Itoa(rf.me) + ": " + strconv.Itoa(rf.currentState))
		rf.RLock.RUnlock()
		rf.RLock.RLock()
		switch rf.currentState {
		case Follower:
			{
				rf.RLock.RUnlock()
				//fmt.Println("Follower: " + strconv.Itoa(Id))
				rf.FollowerFlow()
			}
		case Candidate:
			{
				rf.RLock.RUnlock()
				//fmt.Println("Candidate: " + strconv.Itoa(Id))
				rf.CandidateFlow()
			}
		case Leader:
			{
				rf.RLock.RUnlock()
				//fmt.Println("Leader: " + strconv.Itoa(Id))
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
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.heartbeat = time.Now()
	rf.RLock = new(sync.RWMutex)
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
	//fmt.Println("Starting Raft " + strconv.Itoa(me))

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
