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
	heartbeat     int32
	electionTimer *time.Timer
}
type RequestVoteArgs struct {
	Term        int32
	CandidateID int
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
	rf.state = CANDIDATE
	rf.currentterm++
	rf.votedfor = rf.me //need to enter me details
	fmt.Println(rf.me, " is attempting election at term", rf.currentterm)
	rf.mu.Unlock()

	// request votes
	rf.broadcastVoteReq()

	//send a heart beat
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
			args := RequestVoteArgs{
				Term:        rf.currentterm,
				CandidateID: rf.me,
			}
			if rf.state == CANDIDATE && node.requestVote(&args, &reply) {
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
		fmt.Println(rf.me, " won election at term ", rf.currentterm)
		if cterm == rf.currentterm {
			rf.state = LEADER
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

	if args.Term < rf.currentterm {
		reply.VoteGranted = false
		reply.Term = rf.currentterm
		return true
	} else if args.Term > rf.currentterm {
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

func (rf *Raft) sendHeartBeat() {
	w := 0
	for {
		for _, othernode := range rf.othernodes {
			if othernode.me == rf.me {
				if atomic.LoadInt32(&rf.state) != LEADER {
					return
				}
				continue
			}
			othernode.mu.Lock()
			othernode.heartbeat = 1
			othernode.mu.Unlock()
		}
		w++
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
		if w > 200 {
			time.Sleep(500 * time.Millisecond)
		}
	}

}

func Make(me int) *Raft {
	rf := &Raft{}
	rf.me = me
	rf.state = FOLLOWER
	rf.votedfor = -1
	rf.voteAcquired = 0
	rf.currentterm = 0
	rf.heartbeat = 0
	return rf
}
func main() {
	nodes := []*Raft{}
	numberOfNodes := 5
	for i := 0; i < numberOfNodes; i++ {
		rf := Make(i)
		nodes = append(nodes, rf)
	}
	for _, node := range nodes {
		node.othernodes = nodes
		go node.startLoop()
	}
	for {
	}
}
func (rf *Raft) startLoop() {
	for {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		rf.electionTimer = time.NewTimer(time.Millisecond * time.Duration(r.Int63n(MAX_ELECTION_INTERVAL-MIN_ELECTION_INTERVAL)+MIN_ELECTION_INTERVAL))
		<-rf.electionTimer.C
		//fmt.Println("exec", rf.me)
		switch atomic.LoadInt32(&rf.state) {
		case FOLLOWER:
			if atomic.LoadInt32(&rf.heartbeat) == 0 {

				rf.CallElection()
			}
		case CANDIDATE:

			if atomic.LoadInt32(&rf.heartbeat) == 1 {
				rf.state = FOLLOWER
				rf.voteAcquired = 0
			}
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
		atomic.StoreInt32(&rf.heartbeat, 0)
	}
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
