package raft


import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const Debugrf = 0

// CommitEntry is the data reported by Raft to the commit channel. Each commit
// entry notifies the client that consensus was reached on a command and it can
// be applied to the client's state machine.
type CommitEntry struct {
	// Command is the client command being committed.
	Command interface{}

	// Index is the log index at which the client command is committed.
	Index int

	// Term is the Raft term at which the client command is committed.
	Term int
}

type RfState int

const (
	Follower RfState = iota
	Candidate
	Leader
	Dead
)

func (s RfState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("unreachable")
	}
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// Raft implements a single node of Raft consensus.
type Raft struct {
	// mu protects concurrent access to a rf.
	mu sync.Mutex

	// id is the server ID of this rf.
	id int

	// peerIds lists the IDs of our peers in the cluster.
	peerIds []int

	// server is the server containing this rf. It's used to issue RPC calls
	// to peers.
	server *Server

	// commitChan is the channel where this rf is going to report committed log
	// entries. It's passed in by the client during construction.
	commitChan chan<- CommitEntry

	// newCommitReadyChan is an internal notification channel used by goroutines
	// that commit new entries to the log to notify that these entries may be sent
	// on commitChan.
	newCommitReadyChan chan struct{}

	// triggerAEChan is an internal notification channel used to trigger
	// sending new AEs to followers when interesting changes occurred.
	triggerAEChan chan struct{}

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile Raft state on all servers
	commitIndex        int
	lastApplied        int
	state              RfState
	electionResetEvent time.Time

	// Volatile Raft state on leaders
	nextIndex  map[int]int
	matchIndex map[int]int
}

// NewRaft creates a new rf with the given ID, list of peer IDs and
// server. The ready channel signals the rf that all peers are connected and
// it's safe to start its state machine. commitChan is going to be used by the
// rf to send log entries that have been committed by the Raft cluster.
func NewRaft(me int, peerIds []int, server *Server, commitChan chan<- CommitEntry) *Raft {
	rf := new(Raft)
	rf.id = me
	rf.peerIds = peerIds
	rf.server = server
	rf.commitChan = commitChan
	rf.newCommitReadyChan = make(chan struct{}, 16)
	rf.triggerAEChan = make(chan struct{}, 1)
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)


	go func() {
		time.Sleep(300*time.Millisecond)
		rf.mu.Lock()
		rf.electionResetEvent = time.Now()
		rf.mu.Unlock()
		rf.runElectionTimer()
	}()

	go rf.commitChanSender()
	return rf
}

// Report reports the state of this rf.
func (rf *Raft) Report() (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)-1, rf.currentTerm, rf.state == Leader
}

// Submit submits a new command to the rf. This function doesn't block; clients
// read the commit channel passed in the constructor to be notified of new
// committed entries. It returns true iff this rf is the leader - in which case
// the command is accepted. If false is returned, the client will have to find
// a different rf to submit this command to.
func (rf *Raft) Submit(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	rf.dlog("Submit received by %v: %v", rf.state, command)
	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})
		rf.dlog("... log=%v", rf.log)
		rf.mu.Unlock()
		rf.triggerAEChan <- struct{}{}
		return len(rf.log), rf.currentTerm, true
	}
	rf.mu.Unlock()
	return len(rf.log), rf.currentTerm, false
}

// Stop stops this rf, cleaning up its state. This method returns quickly, but
// it may take a bit of time (up to ~election timeout) for all goroutines to
// exit.
func (rf *Raft) Stop() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Dead
	rf.dlog("becomes Dead")
	close(rf.newCommitReadyChan)
}


// dlog logs a debugging message is Debugrf > 0.
func (rf *Raft) dlog(format string, args ...interface{}) {
	if Debugrf > 0 {
		format = fmt.Sprintf("[%d] ", rf.id) + format
		log.Printf(format, args...)
	}
}

// See figure 2 in the paper.
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

// RequestVote RPC.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return nil
	}
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()
	rf.dlog("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, rf.currentTerm, rf.votedFor, lastLogIndex, lastLogTerm)

	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in RequestVote")
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm == args.Term &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.dlog("... RequestVote reply: %+v", reply)
	return nil
}

// See figure 2 in the paper.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Faster conflict resolution optimization (described near the end of section
	// 5.3 in the paper.)
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return nil
	}
	rf.dlog("AppendEntries: %+v", args)

	if args.Term > rf.currentTerm {
		rf.dlog("... term out of date in AppendEntries")
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()

		// Does our log contain an entry at PrevLogIndex whose term matches
		// PrevLogTerm? Note that in the extreme case of PrevLogIndex=-1 this is
		// vacuously true.
		if args.PrevLogIndex == -1 ||
			(args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true

			// Find an insertion point - where there's a term mismatch between
			// the existing log starting at PrevLogIndex+1 and the new entries sent
			// in the RPC.
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			// At the end of this loop:
			// - logInsertIndex points at the end of the log, or an index where the
			//   term mismatches with an entry from the leader
			// - newEntriesIndex points at the end of Entries, or an index where the
			//   term mismatches with the corresponding log entry
			if newEntriesIndex < len(args.Entries) {
				rf.dlog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				rf.dlog("... log is now: %v", rf.log)
			}

			// Set commit index.
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = intMin(args.LeaderCommit, len(rf.log)-1)
				rf.dlog("... setting commitIndex=%d", rf.commitIndex)
				rf.newCommitReadyChan <- struct{}{}
			}
		} else {
			// No match for PrevLogIndex/PrevLogTerm. Populate
			// ConflictIndex/ConflictTerm to help the leader bring us up to date
			// quickly.
			if args.PrevLogIndex >= len(rf.log) {
				reply.ConflictIndex = len(rf.log)
				reply.ConflictTerm = -1
			} else {
				// PrevLogIndex points within our log, but PrevLogTerm doesn't match
				// rf.log[PrevLogIndex].
				reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

				var i int
				for i = args.PrevLogIndex - 1; i >= 0; i-- {
					if rf.log[i].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = i + 1
			}
		}
	}

	reply.Term = rf.currentTerm
	rf.dlog("AppendEntries reply: %+v", *reply)
	return nil
}

// electionTimeout generates a pseudo-random election timeout duration.
func (rf *Raft) electionTimeout() time.Duration {
	// If RAFT_FORCE_MORE_REELECTION is set, stress-test by deliberately
	// generating a hard-coded number very often. This will create collisions
	// between different servers and force more re-elections.
	return time.Duration(400+rand.Intn(150)) * time.Millisecond
}

// runElectionTimer implements an election timer. It should be launched whenever
// we want to start a timer towards becoming a candidate in a new election.
//
// This function is blocking and should be launched in a separate goroutine;
// it's designed to work for a single (one-shot) election timer, as it exits
// whenever the rf state changes from follower/candidate or the term changes.
func (rf *Raft) runElectionTimer() {
	timeoutDuration := rf.electionTimeout()
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()
	rf.dlog("election timer started (%v), term=%d", timeoutDuration, termStarted)

	// This loops until either:
	// - we discover the election timer is no longer needed, or
	// - the election timer expires and this rf becomes a candidate
	// In a follower, this typically keeps running in the background for the
	// duration of the rf's lifetime.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C

		rf.mu.Lock()
		if rf.state != Candidate && rf.state != Follower {
			rf.dlog("in election timer state=%s, bailing out", rf.state)
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			rf.dlog("in election timer term changed from %d to %d, bailing out", termStarted, rf.currentTerm)
			rf.mu.Unlock()
			return
		}

		// Start an election if we haven't heard from a leader or haven't voted for
		// someone for the duration of the timeout.
		if elapsed := time.Since(rf.electionResetEvent); elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

// startElection starts a new election with this rf as a candidate.
// Expects rf.mu to be locked.
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm += 1
	savedCurrentTerm := rf.currentTerm
	rf.electionResetEvent = time.Now()
	rf.votedFor = rf.id
	rf.dlog("becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rf.log)

	var votesReceived int32 = 1

	// Send RequestVote RPCs to all other servers concurrently.
	for _, peerId := range rf.peerIds {
		go func(peerId int) {
			rf.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm()
			rf.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			rf.dlog("sending RequestVote to %d: %+v", peerId, args)
			var reply RequestVoteReply
			if err := rf.server.sendRequestVote(peerId, args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.dlog("received RequestVoteReply %+v", reply)

				if rf.state != Candidate {
					rf.dlog("while waiting for reply, state = %v", rf.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.dlog("term out of date in RequestVoteReply")
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(rf.peerIds)+1 {
							// Won the election!
							rf.dlog("wins election with %d votes", votes)
							rf.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}

	// Run another election timer, in case this election is not successful.
	go rf.runElectionTimer()
}

// becomeFollower makes rf a follower and resets its state.
// Expects rf.mu to be locked.
func (rf *Raft) becomeFollower(term int) {
	rf.dlog("becomes Follower with term=%d; log=%v", term, rf.log)
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionResetEvent = time.Now()

	go rf.runElectionTimer()
}

// startLeader switches rf into a leader state and begins process of heartbeats.
// Expects rf.mu to be locked.
func (rf *Raft) startLeader() {
	rf.state = Leader

	for _, peerId := range rf.peerIds {
		rf.nextIndex[peerId] = len(rf.log)
		rf.matchIndex[peerId] = -1
	}
	rf.dlog("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", rf.currentTerm, rf.nextIndex, rf.matchIndex, rf.log)

	// This goroutine runs in the background and sends AEs to peers:
	// * Whenever something is sent on triggerAEChan
	// * ... Or every 50 ms, if no events occur on triggerAEChan
	go func(heartbeatTimeout time.Duration) {
		// Immediately send AEs to peers.
		rf.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true

				// Reset timer to fire again after heartbeatTimeout.
				t.Stop()
				t.Reset(heartbeatTimeout)
			case _, ok := <-rf.triggerAEChan:
				if ok {
					doSend = true
				} else {
					return
				}

				// Reset timer for heartbeatTimeout.
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				rf.leaderSendAEs()
			}
		}
	}(100 * time.Millisecond)
}

// leaderSendAEs sends a round of AEs to all peers, collects their
// replies and adjusts rf's state.
func (rf *Raft) leaderSendAEs() {
	rf.mu.Lock()
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for _, peerId := range rf.peerIds {
		go func(peerId int) {
			rf.mu.Lock()
			ni := rf.nextIndex[peerId]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()
			rf.dlog("sending AppendEntries to %v: ni=%d, args=%+v", peerId, ni, args)
			var reply AppendEntriesReply
			if err := rf.server.sendAppendEntries(peerId, args, &reply); err == nil {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > savedCurrentTerm {
					rf.dlog("term out of date in heartbeat reply")
					rf.becomeFollower(reply.Term)
					return
				}

				if rf.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						rf.nextIndex[peerId] = ni + len(entries)
						rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1

						savedCommitIndex := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
								matchCount := 1
								for _, peerId := range rf.peerIds {
									if rf.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(rf.peerIds)+1 {
									rf.commitIndex = i
								}
							}
						}
						rf.dlog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peerId, rf.nextIndex, rf.matchIndex, rf.commitIndex)
						if rf.commitIndex != savedCommitIndex {
							rf.dlog("leader sets commitIndex := %d", rf.commitIndex)
							// Commit index changed: the leader considers new entries to be
							// committed. Send new entries on the commit channel to this
							// leader's clients, and notify followers by sending them AEs.
							rf.newCommitReadyChan <- struct{}{}
							rf.triggerAEChan <- struct{}{}
						}
					} else {
						if reply.ConflictTerm >= 0 {
							lastIndexOfTerm := -1
							for i := len(rf.log) - 1; i >= 0; i-- {
								if rf.log[i].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm >= 0 {
								rf.nextIndex[peerId] = lastIndexOfTerm + 1
							} else {
								rf.nextIndex[peerId] = reply.ConflictIndex
							}
						} else {
							rf.nextIndex[peerId] = reply.ConflictIndex
						}
						rf.dlog("AppendEntries reply from %d !success: nextIndex := %d", peerId, ni-1)
					}
				}
			}
		}(peerId)
	}
}

// lastLogIndexAndTerm returns the last log index and the last log entry's term
// (or -1 if there's no log) for this server.
// Expects rf.mu to be locked.
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		return lastIndex, rf.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

// commitChanSender is responsible for sending committed entries on
// rf.commitChan. It watches newCommitReadyChan for notifications and calculates
// which new entries are ready to be sent. This method should run in a separate
// background goroutine; rf.commitChan may be buffered and will limit how fast
// the client consumes new committed entries. Returns when newCommitReadyChan is
// closed.
func (rf *Raft) commitChanSender() {
	for range rf.newCommitReadyChan {
		// Find which entries we have to apply.
		rf.mu.Lock()
		savedTerm := rf.currentTerm
		savedLastApplied := rf.lastApplied
		var entries []LogEntry
		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()
		rf.dlog("commitChanSender entries=%v, savedLastApplied=%d", entries, savedLastApplied)

		for i, entry := range entries {
			rf.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + i + 1,
				Term:    savedTerm,
			}
		}
	}
	rf.dlog("commitChanSender done")
}

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}
