package main

import(
	"log"
	"sync"
	"time"
	"math/rand"
	"fmt"
)

//type State string

const (
	Follower = "follower"
	Candidate = "candidate"
	Leader = "leader"

)

type Raft struct {
	mu sync.Mutex
	me int
	othernodes []int
	state string
	currentterm int
	votedfor int
}
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentterm
	if rf.state == Leader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) CallElection(){  // make yourself candidate, append current term, vote for yourself and 
	rf.mu.Lock()                 // take votes from other nodes
	rf.state = Candidate
	rf.currentterm++
	rf.votedfor=rf.me  //need to enter me details
	fmt.Println("[%d] is attempting election at term [%d]", rf.me, rf.currentterm)
	rf.mu.Unlock()
	myterm := rf.currentterm
	cond:=sync.NewCond(&rf.mu)
	count :=0
	finished :=0
    
	// request votes
	for i, node := range rf.othernodes{
		if node == rf.me {
			continue
		}
		go func (node int){
			vote:= requestVote(node, myterm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if vote{
				count++
			}
			finished++
			cond.Broadcast() //wakes up whover is waiting on this var
		}(node)
	}
	rf.mu.Lock()
	var total = len(rf.othernodes)+1
	for count < total/2 && finished!=total {
		cond.Wait()
	}
	if count >= total/2{
		println("[%d] won election at term [%d]", rf.me, rf.currentterm)
		if( myterm==rf.currentterm) {
		rf.state = Leader}
	} else {
		println("[%d] lost election at term [%d]", rf.me, rf.currentterm)
		rf.state = Follower
	}
	rf.mu.Unlock()
	//send a heart beat
}

func (rf *Raft) requestVote (node int, term int) bool {

	//log 
	args := RequestVoteArgs{
		Term: term,
		CandidateID : rf.me,
	}
	var reply RequestVoteReply
	ok := rf.sendvote(node,&args,&reply)
	if ok {
		return true
	} else {
		return false
	}

}