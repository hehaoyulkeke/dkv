package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

func randTimeout(lower int, upper int) int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	diff := upper - lower
	return lower + r.Intn(diff)
}

const (
	LEADER int = iota
	CANDIDATE
	FOLLOWER
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
	Granted int
}

type Raft struct {
	mu    sync.Mutex
	peers []int
	s     *Server
	me    int

	CurrentTerm           int
	VotedFor              int
	commitIndex           int
	lastApplied           int
	nextIndex             []int
	matchIndex            []int
	role                  int
	lastHeartBeatReceived time.Time
	applyCh               chan ApplyMsg
	Logs                  []LogEntry
	Applycond             *sync.Cond
}

func (rf *Raft) FirstEntryWithTerm(term int) int {
	findTerm := false
	index := -1
	for i := len(rf.Logs) - 1; i >= 0; i-- {
		if rf.Logs[i].Term != term && findTerm {
			index = i + 1
			break
		}
		if rf.Logs[i].Term == term {
			findTerm = true
		}
	}
	return index + 1
}

func (rf *Raft) LastEntryWithTerm(term int) int {
	index := -1
	for i := len(rf.Logs) - 1; i >= 0; i-- {
		if rf.Logs[i].Term < term {
			break
		}
		if rf.Logs[i].Term == term {
			index = i
			break
		}
	}
	return index + 1
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	{
		rf.mu.Lock()
		term = rf.CurrentTerm
		if rf.role == LEADER {
			isleader = true
		} else {
			isleader = false
		}
		rf.mu.Unlock()
	}

	return term, isleader
}
func (rf *Raft) Role() int {
	var role int
	rf.mu.Lock()
	role = rf.role
	rf.mu.Unlock()
	return role
}

func (rf *Raft) GetLastLogEntryWithLock() LogEntry {
	entry := LogEntry{}
	if len(rf.Logs) == 0 {
		entry.Term = rf.CurrentTerm
		entry.Index = 0
	} else {
		entry = rf.Logs[len(rf.Logs)-1]
	}
	return entry
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	entry := LogEntry{}
	rf.mu.Lock()
	entry = rf.GetLastLogEntryWithLock()
	rf.mu.Unlock()
	return entry
}
func (rf *Raft) LastHeartBeatReceived() time.Time {
	var time time.Time
	rf.mu.Lock()
	time = rf.lastHeartBeatReceived
	rf.mu.Unlock()
	return time
}

func (rf *Raft) setLastHeartBeatReceived(receivedTime time.Time) {
	rf.mu.Lock()
	rf.lastHeartBeatReceived = receivedTime
	rf.mu.Unlock()
}

func (rf *Raft) handleElectionTimeout() {
	finished := 0
	granted := 0
	reply_ := RequestVoteReply{}
	request_ := RequestVoteArgs{}
	cond := sync.NewCond(&rf.mu)
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.role = CANDIDATE
	rf.lastHeartBeatReceived = time.Now()
	granted++
	finished++
	request_.CandidateId = rf.me
	request_.Term = rf.CurrentTerm
	entry := rf.GetLastLogEntryWithLock()
	request_.LastLogIndex = entry.Index
	request_.LastLogTerm = entry.Term
	rf.mu.Unlock()
	go rf.electionTimer()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(index int, request RequestVoteArgs, reply RequestVoteReply) {
			ok := rf.s.sendRequestVote(index, request, &reply)
			if ok {
				ok = rf.handleRequestVoteResponse(request, reply)
			}
			rf.mu.Lock()
			finished++
			if ok {
				granted++
			}
			cond.Signal()
			rf.mu.Unlock()
		}(i, request_, reply_)
	}

	rf.mu.Lock()
	for finished != len(rf.peers) && granted < len(rf.peers)/2+1 {
		cond.Wait()
	}
	rf.mu.Unlock()
	term, _ := rf.GetState()
	if request_.Term == term && granted >= len(rf.peers)/2+1 && rf.Role() == CANDIDATE {
		rf.becomeLeader()
	} else {
	}

}

func (rf *Raft) becomeLeader() {
	lastLogIndex := 0
	rf.mu.Lock()
	rf.role = LEADER
	lastLogIndex = rf.GetLastLogEntryWithLock().Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}

	rf.mu.Unlock()
	go rf.heartBeatTimer()
}

func (rf *Raft) becomeFollowerWithLock() {
	if rf.role == LEADER {
		rf.role = FOLLOWER
		go rf.electionTimer()
	} else if rf.role == CANDIDATE {
		rf.role = FOLLOWER
	}
}

func (rf *Raft) electionTimer() {
	for rf.Role() != LEADER {
		interval := randTimeout(500, 800)
		time.Sleep(time.Millisecond * time.Duration(interval))
		role := rf.Role()
		if role == FOLLOWER {
			diff := time.Since(rf.LastHeartBeatReceived())
			if diff < time.Duration(interval)*time.Millisecond {
				continue
			} else {
				rf.handleElectionTimeout()
				return
			}
		} else if role == CANDIDATE {
			rf.handleElectionTimeout()
			return
		} else {
			return
		}

	}
}

func (rf *Raft) heartBeatTimer() {
	for rf.Role() == LEADER {
		time.Sleep(time.Millisecond * 250)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.RequestAppendNewEntries(i, true)
		}
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
	LogLen        int
}

func (rf *Raft) updateTerm(term int) {
	rf.mu.Lock()
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.VotedFor = -1
		rf.becomeFollowerWithLock()
	}
	rf.mu.Unlock()
}
func (rf *Raft) handleRequestVoteResponse(request RequestVoteArgs, reply RequestVoteReply) bool {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	if rf.CurrentTerm != request.Term {
		rf.mu.Unlock()
		return false
	}
	granted := reply.VoteGranted
	rf.mu.Unlock()
	return granted
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateTerm(args.Term)
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false
	if args.Term < rf.CurrentTerm {
		reply.VoteGranted = false
	} else if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		entry := rf.GetLastLogEntryWithLock()
		logok := (args.LastLogTerm > entry.Term) ||
			(args.LastLogTerm == entry.Term && args.LastLogIndex >= entry.Index)
		if logok {
			reply.VoteGranted = true
			rf.VotedFor = args.CandidateId
			rf.lastHeartBeatReceived = time.Now()
		} else {
			reply.VoteGranted = false
		}
	} else {
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) handleAppendEntriesResponse(request AppendEntriesArgs, reply AppendEntriesReply, peerIndex int) bool {
	rf.updateTerm(reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if request.Term != rf.CurrentTerm {
		return false
	}
	isContinue := false
	isUpdated := false
	if reply.Success {
		rf.matchIndex[peerIndex] = len(request.Entries) + request.PrevLogIndex
		rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1
		isUpdated = rf.updateCommitForLeader()
	} else {
		if reply.LogLen != -1 {
			rf.nextIndex[peerIndex] = reply.LogLen
		} else {
			index := rf.LastEntryWithTerm(reply.ConflictTerm)
			if index == -1 {
				rf.nextIndex[peerIndex] = reply.ConflictIndex
			} else {
				rf.nextIndex[peerIndex] = index
			}
		}
		if rf.nextIndex[peerIndex] < 1 {
			rf.nextIndex[peerIndex] = 1
		}
		isContinue = true
	}
	if isUpdated {
		rf.Applycond.Signal()
	}
	return isContinue
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := args.Term
	reply.Success = false
	reply.LogLen = -1
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1
	isUpdateCommit := false
	rf.updateTerm(term)
	rf.mu.Lock()
	if term == rf.CurrentTerm {
		rf.lastHeartBeatReceived = time.Now()
		if rf.role == CANDIDATE {
			rf.becomeFollowerWithLock()
		}
		logOk := args.PrevLogIndex == 0 ||
			(args.PrevLogIndex <= len(rf.Logs) && rf.Logs[args.PrevLogIndex-1].Term == args.PrevLogTerm)
		if !logOk {
			if len(rf.Logs) < args.PrevLogIndex {
				reply.LogLen = len(rf.Logs)
			} else {
				reply.ConflictTerm = rf.Logs[args.PrevLogIndex-1].Term
				reply.ConflictIndex = rf.FirstEntryWithTerm(reply.ConflictTerm)
			}
		}
		if logOk {
			reply.Success = true
			isconflicts := false
			conflictIndex := -1
			for _, entry := range args.Entries {
				if entry.Index <= len(rf.Logs) {
					//If an existing entry conflicts with a new one
					if entry.Term != rf.Logs[entry.Index-1].Term {
						isconflicts = true
						conflictIndex = entry.Index
						break
					}
				} else {
					rf.Logs = append(rf.Logs, entry)
				}
			}
			if isconflicts {
				rf.Logs = rf.Logs[:conflictIndex-1]
				beginIndex := rf.GetLastLogEntryWithLock().Index + 1
				for _, entry := range args.Entries {
					if entry.Index >= beginIndex {
						rf.Logs = append(rf.Logs, entry)

					}
				}
			}
			if args.LeaderCommit > rf.commitIndex {
				isUpdateCommit = true
				if rf.GetLastLogEntryWithLock().Index < args.LeaderCommit {
					rf.commitIndex = rf.GetLastLogEntryWithLock().Index
				} else {
					rf.commitIndex = args.LeaderCommit
				}
			}
		}
	}
	reply.Term = rf.CurrentTerm
	if isUpdateCommit {
		rf.Applycond.Signal()
	}
	rf.mu.Unlock()
}

func (rf *Raft) RequestAppendNewEntries(peerIndex int, isHeartBeat bool) {
	var entries []LogEntry
	if rf.Role() == LEADER {
		rf.mu.Lock()
		for i := rf.nextIndex[peerIndex]; i <= rf.GetLastLogEntryWithLock().Index; i++ {
			if i < 1 || i-1 >= len(rf.Logs) {
				fmt.Printf("%v %v\n", i, len(rf.Logs))
			}
			entry := rf.Logs[i-1]
			entries = append(entries, entry)
		}

		if len(entries) == 0 && isHeartBeat == false {
			rf.mu.Unlock()
			return
		}
		request := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		request.Entries = entries
		request.Term = rf.CurrentTerm
		request.LeaderId = rf.me
		request.PrevLogIndex = rf.nextIndex[peerIndex] - 1
		request.LeaderCommit = rf.commitIndex
		if request.PrevLogIndex == 0 {
			request.PrevLogTerm = rf.CurrentTerm
		} else {
			request.PrevLogTerm = rf.Logs[request.PrevLogIndex-1].Term
		}
		rf.mu.Unlock()
		ok := rf.s.sendAppendEntries(peerIndex, request, &reply)
		if ok {
			isContinue := rf.handleAppendEntriesResponse(request, reply, peerIndex)
			if isContinue {
				go rf.RequestAppendNewEntries(peerIndex, false)
			}
		}
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	if rf.role == LEADER {
		isLeader = true
		entry := LogEntry{}
		entry.Term = rf.CurrentTerm
		entry.Index = len(rf.Logs) + 1
		entry.Command = command
		entry.Granted = 1
		index = entry.Index
		term = entry.Term
		rf.Logs = append(rf.Logs, entry)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.RequestAppendNewEntries(i, false)
		}
	} else {
		isLeader = false
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

func (rf *Raft) updateCommitForLeader() bool {
	beginIndex := rf.commitIndex + 1
	lastCommittedIndex := -1
	updated := false
	for ; beginIndex <= rf.GetLastLogEntryWithLock().Index; beginIndex++ {
		granted := 1
		for peerIndex := 0; peerIndex < len(rf.matchIndex); peerIndex++ {
			if peerIndex == rf.me {
				continue
			}
			if rf.matchIndex[peerIndex] >= beginIndex {
				granted++
			}
		}

		if granted >= len(rf.peers)/2+1 && (rf.Logs[beginIndex-1].Term == rf.CurrentTerm) {
			lastCommittedIndex = beginIndex
		}
	}
	if lastCommittedIndex > rf.commitIndex {
		rf.commitIndex = lastCommittedIndex
		updated = true
	}
	return updated
}

func (rf *Raft) doCommit() {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.Applycond.Wait()
		}
		i := rf.lastApplied + 1
		for ; i <= rf.commitIndex; i++ {
			msg := ApplyMsg{true, rf.Logs[i-1].Command, i}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = i
		}
		rf.mu.Unlock()
	}
}

func Make(me int, peers []int,
	s *Server, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.s = s
	rf.me = me
	rf.mu = sync.Mutex{}
	rf.Applycond = sync.NewCond(&rf.mu)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.applyCh = applyCh
	rf.role = FOLLOWER
	rf.lastHeartBeatReceived = time.Now()
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	rf.lastHeartBeatReceived = time.Now()
	go rf.electionTimer()
	go rf.doCommit()

	return rf
}
