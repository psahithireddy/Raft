package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//type State string
//Need to add persistance
const (
	FOLLOWER  int32 = 1
	CANDIDATE       = 2
	LEADER          = 3
)
const (
	MIN_ELECTION_INTERVAL = 200
	MAX_ELECTION_INTERVAL = 400
	HEARTBEAT_INTERVAL    = 50
)

type Raft struct {
	mu            sync.Mutex
	me            int
	othernodes    []*Raft
	state         int32
	currentterm   int32
	votedfor      int
	voteAcquired  int
	electionTimer *time.Timer

	logs              []Entry
	commitIndex       int
	nextIndex         []int
	matchIndex        []int
	lastApplied       int
	applyCh           chan ApplyMsg
	doNotBecomeLeader bool //only for testing
	doNotAppend       bool //only for testing
}

type Entry struct {
	Term    int32
	Index   int
	Command interface{}
}

type ApplyMsg struct {
	Index   int
	Command interface{}
}

type AppendEntriesArgs struct {
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int32
	Success   bool
	nextIndex int //next or conflict index
}

type RequestVoteArgs struct {
	Term         int32
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int32
}

type RequestVoteReply struct {
	Term        int32
	VoteGranted bool
}

func (rf *Raft) GetState() (int32, bool) {

	var term int32
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentterm
	if rf.state == LEADER {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) CallElection() { // make yourself candidate, append current term, vote for yourself and
	rf.mu.Lock() // take votes from other nodes
	if rf.doNotAppend {
		rf.mu.Unlock()
		return
	}
	rf.state = CANDIDATE
	rf.currentterm++
	rf.votedfor = rf.me //need to enter me details
	fmt.Println()
	fmt.Println(rf.me, " is attempting election at term", rf.currentterm)

	rf.mu.Unlock()

	// request votes
	rf.broadcastVoteReq()

	//send a heart beat
}

func (rf *Raft) broadcastAppendReq() {

	count := 0
	finished := 0

	for _, othernode := range rf.othernodes {
		if othernode.me == rf.me {
			continue
		}
		go func(node *Raft) {

			var reply AppendEntriesReply
			var args AppendEntriesArgs
			rf.mu.Lock()
			//fmt.Println("ahb")
			args.Term = rf.currentterm
			args.LeaderID = rf.me
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[node.me] - 1
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term

			if node.doNotAppend {
				rf.mu.Unlock()
				return
			}
			// if node.me == 4 {
			// 	fmt.Println("append on node 4", node.logs)
			// }
			if (len(rf.logs) - 1) >= rf.nextIndex[node.me] {
				args.Entries = rf.logs[rf.nextIndex[node.me]:]
			}

			// if len(args.Entries) == 0 {
			// 	rf.mu.Unlock()
			// 	//fmt.Println("Append for Node ", node.me, " is up to date")
			// 	return
			// }

			if rf.state == LEADER {
				//fmt.Println("ahb")
				rf.mu.Unlock()
				node.appendEntries(&args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {
					rf.nextIndex[node.me] += len(args.Entries)
					rf.matchIndex[node.me] = rf.nextIndex[node.me] - 1
					count++
				} else {
					if reply.Term > rf.currentterm {
						rf.currentterm = reply.Term
						rf.state = FOLLOWER
						rf.voteAcquired = 0
					} else if rf.state != LEADER {
						return
					} else {
						if reply.nextIndex > 0 {
							rf.nextIndex[node.me] = reply.nextIndex
						}
					}
				}
				if reply.Success {
					fmt.Println()
					fmt.Println()

					fmt.Println("Node ", node.me, " appended ", node.logs)
					fmt.Println("LEADER commitIndex: ", rf.commitIndex)
				}
				finished++
			} else {
				rf.mu.Unlock()
				return
			}
		}(othernode)
	}
}

func (rf *Raft) appendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Println("ahb")
	//log
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentterm {
		reply.Success = false
		reply.Term = rf.currentterm
		return
	} else if args.Term > rf.currentterm {
		if rf.state == LEADER {
			fmt.Println("Leader ", rf.me, " becomes a follower")
		}
		rf.currentterm = args.Term
		rf.state = FOLLOWER
		reply.Success = true
	} else {
		reply.Success = true
	}

	if args.PrevLogIndex > (len(rf.logs) - 1) {
		reply.Success = false
		reply.nextIndex = (len(rf.logs) - 1) + 1
		return
	}
	if args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term { //wrong data
		reply.Success = false
		wrongTerm := rf.logs[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for ; rf.logs[i].Term == wrongTerm; i-- {
		}
		reply.nextIndex = i + 1 //overwrite the wrong logs
		return
	}
	wrongIdStart := -1
	if (len(rf.logs) - 1) == args.PrevLogIndex {
		if len(args.Entries) == 0 {
			reply.Success = false
		} else {
			rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
		}
	} else if (len(rf.logs) - 1) < args.PrevLogIndex+len(args.Entries) { //there are logs already present from present list
		wrongIdStart = args.PrevLogIndex + 1
		reply.Success = false
		reply.nextIndex = wrongIdStart
	} else { //verify this
		for id := 0; id < len(args.Entries); id++ {
			if rf.logs[id+args.PrevLogIndex+1] != args.Entries[id] { //more logs added from wrong term, need to be overwritten
				wrongIdStart = id + args.PrevLogIndex + 1
				rf.logs = append(rf.logs[:wrongIdStart], args.Entries[id:]...)
				break
			}
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < (len(rf.logs) - 1) {
			rf.commitIndex = (len(rf.logs) - 1)
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimer.Reset(time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL))
}

func (rf *Raft) broadcastVoteReq() {
	cond := sync.NewCond(&rf.mu)
	count := 0
	finished := 0
	rf.mu.Lock()
	cterm := rf.currentterm
	rf.mu.Unlock()

	for _, othernode := range rf.othernodes {
		if othernode.me == rf.me {
			continue
		}
		go func(node *Raft) {
			var reply RequestVoteReply
			rf.mu.Lock()
			args := RequestVoteArgs{
				Term:         rf.currentterm,
				CandidateID:  rf.me,
				LastLogIndex: (len(rf.logs) - 1),
				LastLogTerm:  rf.logs[(len(rf.logs) - 1)].Term,
			}
			rf.mu.Unlock()
			if atomic.LoadInt32(&rf.state) == CANDIDATE && node.requestVote(&args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					rf.voteAcquired++
					count++
				} else {
					if reply.Term > rf.currentterm {
						rf.currentterm = reply.Term
						rf.state = FOLLOWER
					}
				}
				fmt.Println("Node ", node.me, " voted", reply.VoteGranted)
				finished++
				cond.Broadcast()
			} else if rf.state == LEADER {
				return
			} else {
				fmt.Println("Node ", rf.me, " failed to request votes")
			}
		}(othernode)
	}
	rf.mu.Lock()
	var total = len(rf.othernodes) + 1
	for count < total/2 && finished != total {
		cond.Wait()
	}
	if count >= total/2 {
		fmt.Println()
		fmt.Println(rf.me, " won election at term ", rf.currentterm)
		if cterm == rf.currentterm {
			rf.state = LEADER
			for i := range rf.othernodes {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
		}
	} else {
		fmt.Println(rf.me, " lost election at term ", rf.currentterm)
		rf.state = FOLLOWER
		rf.voteAcquired = 0
	}
	rf.mu.Unlock()
	if atomic.LoadInt32(&rf.state) == LEADER {
		go rf.sendHeartBeat()
	}
}

func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) bool {

	//log
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentLastLogTerm := rf.logs[(len(rf.logs) - 1)].Term
	if currentLastLogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		return true
	} else if (len(rf.logs) - 1) < args.LastLogIndex {
		reply.VoteGranted = false
		return true
	}

	if args.Term < rf.currentterm {
		reply.VoteGranted = false
		reply.Term = rf.currentterm
		return true
	} else if args.Term > rf.currentterm {
		if rf.state == LEADER {
			fmt.Println("Leader ", rf.me, " becomes a follower")
		}
		rf.currentterm = args.Term
		rf.state = FOLLOWER
		rf.votedfor = args.CandidateID
		reply.VoteGranted = true
		return true
	} else {
		if rf.votedfor == -1 {
			rf.currentterm = args.Term
			rf.state = FOLLOWER
			rf.votedfor = args.CandidateID
			reply.VoteGranted = true
			return true
		} else {
			reply.VoteGranted = false
			return true
		}
	}
	return false
}

func (rf *Raft) appendHeartBeat() {
	rf.broadcastAppendReq()
	rf.updateCommitIndex()
}

func (rf *Raft) sendHeartBeat() {
	w := 0

	for {
		if atomic.LoadInt32(&rf.state) != LEADER {
			return
		}
		//fmt.Println("sending heartbeat")
		go rf.appendHeartBeat()
		for _, othernode := range rf.othernodes {
			if othernode.me == rf.me {
				continue
			}
			othernode.receiveHeartBeat()
		}
		w++
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond) //manual delay for testing
		if w > 200 {
			time.Sleep(500 * time.Millisecond)
		}
	}

}

func Make(me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.state = FOLLOWER
	rf.votedfor = -1
	rf.voteAcquired = 0
	rf.currentterm = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh //should get from input
	return rf
}

func (rf *Raft) receiveHeartBeat() {
	switch atomic.LoadInt32(&rf.state) {
	case CANDIDATE:
		rf.state = FOLLOWER
		rf.voteAcquired = 0
	case LEADER:

		leader := rf.getLeader()
		if rf.me != leader.me {
			if atomic.LoadInt32(&rf.currentterm) < atomic.LoadInt32(&leader.currentterm) {
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.voteAcquired = 0
				rf.currentterm = atomic.LoadInt32(&leader.currentterm)
				rf.mu.Unlock()
				println("Previous leader has now become a follower")
			}
		}
	}
	rf.mu.Lock()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.electionTimer.Reset(time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL))
	rf.mu.Unlock()
}

func (rf *Raft) startLoop() {
	for {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL))
		<-rf.electionTimer.C
		//fmt.Println("exec", rf.me)
		switch atomic.LoadInt32(&rf.state) {
		case FOLLOWER:
			if rf.doNotBecomeLeader {
				continue
			}
			rf.CallElection()
		}
		go rf.applyLog()
	}
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := len(rf.logs) - 1; i > rf.commitIndex; i-- {
		count := 1
		for j, matchedNodeIndex := range rf.matchIndex {
			if j == rf.me {
				continue
			}
			if matchedNodeIndex > rf.commitIndex {
				count++
			}
		}
		if count > (len(rf.othernodes)+1)/2 {
			rf.commitIndex = i

		} else {
			break
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				Index:   i,
				Command: rf.logs[i].Command,
			}
			rf.applyCh <- applyMsg
		}
	}
}

func getLeader(nodes []*Raft) *Raft {
	for _, node := range nodes {
		if atomic.LoadInt32(&node.state) == LEADER {
			return node
		}
	}
	rf1 := &Raft{}
	rf1.me = -1
	return rf1
}

func (rf *Raft) getLeader() *Raft {
	for _, node := range rf.othernodes {
		if atomic.LoadInt32(&node.state) == LEADER {
			return node
		}
	}
	rf1 := &Raft{}
	rf1.me = -1
	return rf1
}

func testAppends(value int, nodes []*Raft) {
	leader := getLeader(nodes)
	if leader.me != -1 {
		leader.mu.Lock()
		entry := Entry{
			Term:    leader.currentterm,
			Index:   len(leader.logs),
			Command: value,
		}
		leader.logs = append(leader.logs, entry)
		leader.mu.Unlock()
	} else {
		time.Sleep(500 * time.Millisecond)
		testAppends(value, nodes)
	}
}

func printAllLogs(nodes []*Raft) {
	fmt.Println()
	fmt.Println("--------------Log Entries----------")

	for _, node := range nodes {
		node.mu.Lock()
		fmt.Println()
		fmt.Println(node.logs)
		if node.state == LEADER {
			fmt.Println()
			fmt.Println("Leader Commit at:", node.commitIndex)
			fmt.Println()
		}
		node.mu.Unlock()
	}

	fmt.Println("-----------------------------------")
}

func consumeApply(applyCh chan ApplyMsg) {
	for {
		<-applyCh
	}

}

func main() {
	nodes := []*Raft{}
	numberOfNodes := 5
	for i := 0; i < numberOfNodes; i++ {
		applyCh := make(chan ApplyMsg)
		rf := Make(i, applyCh)
		nodes = append(nodes, rf)
		go consumeApply(applyCh)
	}
	for _, node := range nodes {
		node.othernodes = nodes
		node.nextIndex = make([]int, len(node.othernodes))
		node.matchIndex = make([]int, len(node.othernodes))
		node.logs = make([]Entry, 1)
		node.doNotAppend = false
		if node.me != 4 {
			node.doNotBecomeLeader = false
		} else {
			node.doNotBecomeLeader = true
		}
		go node.startLoop()
	}
	fmt.Println()
	fmt.Println("------Testing Log replication--------------")

	for i := 0; i < 20; i++ {
		testAppends(i, nodes)
		fmt.Println("Sending append command: ", i)
		time.Sleep(10 * time.Millisecond)
		printAllLogs(nodes)
	}
	time.Sleep(10 * time.Millisecond)
	nodes[4].doNotAppend = true
	fmt.Println()
	fmt.Println()
	fmt.Println("------Testing Log replication on failed/network partitioned node through a election--------------")
	time.Sleep(time.Duration(1000) * time.Millisecond)
	for i := 20; i < 40; i++ {
		testAppends(i, nodes)
		fmt.Println("Sending append command: ", i)
		time.Sleep(10 * time.Millisecond)
		printAllLogs(nodes)
	}
	time.Sleep(time.Duration(10000) * time.Millisecond)
	nodes[4].doNotAppend = false
	fmt.Println()
	fmt.Println("DonotAppend made false")

	for {
	}
}
