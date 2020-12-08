package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

type Message = map[string]interface{}

// Server wraps a raft.Raft along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.Raft has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex
	dbLock sync.RWMutex

	serverId int
	serverStr string
	peerIds  []int

	rf      *Raft
	db  Storage
	commitChan  chan CommitEntry
	wg    sync.WaitGroup
	enc *json.Encoder
	applyChs 	map[int]chan int
	rpcSeq int
	rpcChs map[int]chan Message
}

func NewServer(serverId string, peerIds []int) *Server {
	s := new(Server)
	s.serverStr = serverId
	s.serverId = str2Int(serverId)
	s.peerIds = peerIds
	s.db = NewMapStorage()
	s.commitChan = make(chan CommitEntry)
	s.applyChs = make(map[int]chan int)
	s.rpcSeq = 1
	s.rpcChs = make(map[int]chan Message)
	return s
}

func (s *Server) Serve() {
	s.rf = NewRaft(s.serverId, s.peerIds, s, s.commitChan)

	conn, err := net.Dial("unixpacket", s.serverStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	s.enc = json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	s.wg.Add(1)
	go func() {
		s.processAppliedOps()
		s.wg.Done()
	}()
	//read message from net
	for {
		req := make(Message)
		err := dec.Decode(&req)
		if err != nil {
			fmt.Println(err)
			break
		} else {
			s.wg.Add(1)
			go func() {
				s.handleMessage(req)
				s.wg.Done()
			}()
		}
	}
	s.wg.Wait()
}

func (s *Server) handleMessage(msg Message) {
	msgType := msg["type"]
	switch msgType {
	case GET:
		reply := s.handleGet(msg)
		s.enc.Encode(&reply)
	case PUT:
		reply := s.handlePut(msg)
		s.enc.Encode(&reply)
	case REQUEST_VOTE_SEND:
		reply := s.handleRequestVoteSend(msg)
		s.enc.Encode(&reply)
	case REQUEST_VOTE_RECV:
		s.handleRequestVoteRecv(msg)
	case APPEND_ENTRIES_SEND:
		reply := s.handleAppendEntriesSend(msg)
		s.enc.Encode(&reply)
	case APPEND_ENTRIES_RECV:
		s.handleAppendEntriesRecv(msg)
	}
}

func (s *Server) newReply(msg Message) Message{
	reply := make(Message)
	reply["src"] = s.serverStr
	reply["dst"] = msg["src"]
	reply["MID"] = msg["MID"]
	reply["leader"] = UNKNOWN
	return reply
}

func (s *Server) nextServer() string {
	next := (s.serverId+1)%(len(s.peerIds)+1)
	return int2Str(next)
}

func (s *Server) handleGet(msg Message) Message {
	reply := s.newReply(msg)
	op := Op{msg["type"].(string), msg["key"].(string), ""}

	index, term, isLeader := s.rf.Submit(op)
	if !isLeader{
		reply["type"] = REDIRECT
		reply["leader"] = s.nextServer()
		return reply
	}

	s.mu.Lock()
	ch := make(chan int)
	s.applyChs[index] = ch
	s.mu.Unlock()

	select{
	case msgTerm := <- ch:
		if msgTerm == term {
			s.dbLock.RLock()
			if val, ok := s.db.Get(msg["key"].(string)); ok{
				reply["value"] = val
			} else {
				reply["value"] = ""
			}
			s.dbLock.RUnlock()
		}
	}

	s.wg.Add(1)
	go func() {
		s.closeApplyCh(index)
		s.wg.Done()
	}()
	reply["leader"] = s.serverStr
	reply["type"] = OK
	return reply
}

func (s *Server) handlePut(msg Message) Message {
	reply := s.newReply(msg)
	op := Op{msg["type"].(string), msg["key"].(string), msg["value"].(string)}

	index, _, isLeader := s.rf.Submit(op)
	if !isLeader{
		reply["type"] = REDIRECT
		reply["leader"] = s.nextServer()
		return reply
	}

	s.mu.Lock()
	ch := make(chan int)
	s.applyChs[index] = ch
	s.mu.Unlock()

	select{
	case <- ch:
	}
	s.wg.Add(1)
	go func() {
		s.closeApplyCh(index)
		s.wg.Done()
	}()
	reply["leader"] = s.serverStr
	reply["type"] = OK
	return reply
}


func (s *Server) handleRequestVoteSend(msg Message) Message {
	args := RequestVoteArgs{
		Term:         int(msg["Term"].(float64)),
		CandidateId:  int(msg["CandidateId"].(float64)),
		LastLogIndex: int(msg["LastLogIndex"].(float64)),
		LastLogTerm:  int(msg["LastLogTerm"].(float64)),
	}
	requestReply := new(RequestVoteReply)
	reply := s.newReply(msg)
	if err := s.rf.RequestVote(args, requestReply); err != nil {
		reply["type"] = FAIL
	} else {
		reply["type"] = REQUEST_VOTE_RECV
		reply["Term"] = requestReply.Term
		reply["VoteGranted"] = requestReply.VoteGranted
		reply["rpcSeq"] = msg["rpcSeq"]
	}
	return reply
}

func (s *Server) handleRequestVoteRecv(msg Message) {
	s.mu.Lock()
	index := int(msg["rpcSeq"].(float64))
	ch := s.rpcChs[index]
	s.mu.Unlock()
	ch <- msg
}

func (s *Server) sendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) error {
	s.mu.Lock()
	rpcSeq := s.rpcSeq
	s.rpcSeq++
	s.mu.Unlock()
	msg := Message{"src": s.serverStr, "dst": int2Str(peerId),
		"type": REQUEST_VOTE_SEND, "leader": UNKNOWN,
		"Term": args.Term,
		"CandidateId": args.CandidateId,
		"LastLogIndex": args.LastLogIndex,
		"LastLogTerm": args.LastLogTerm,
		"rpcSeq": rpcSeq,
	}

	err := s.enc.Encode(&msg)
	if err != nil {
		return err
	}
	for {
		s.mu.Lock()
		ch := make(chan Message)
		index := rpcSeq
		s.rpcChs[index] = ch
		s.mu.Unlock()
		resp := <- ch
		reply.Term = int(resp["Term"].(float64))
		reply.VoteGranted = resp["VoteGranted"].(bool)
		s.wg.Add(1)
		go func() {
			s.closeRpcCh(rpcSeq)
			s.wg.Done()
		}()
		return nil
	}
}

func (s *Server) handleAppendEntriesSend(msg Message) Message {
	args := AppendEntriesArgs{
		Term:         int(msg["Term"].(float64)),
		LeaderId:     int(msg["LeaderId"].(float64)),
		PrevLogIndex: int(msg["PrevLogIndex"].(float64)),
		PrevLogTerm:  int(msg["PrevLogTerm"].(float64)),
		LeaderCommit: int(msg["LeaderCommit"].(float64)),
	}
	if msg["Entries"] != nil {
		entries := msg["Entries"].([]interface{})
		args.Entries = make([]LogEntry, len(entries))
		for i := range entries {
			tmp := entries[i].(map[string]interface{})
			e := LogEntry{
				Command: tmp["Command"],
				Term:    int(tmp["Term"].(float64)),
			}
			args.Entries[i] = e
		}
	} else {
		args.Entries = nil
	}
	appendReply := new(AppendEntriesReply)
	reply := s.newReply(msg)
	if err := s.rf.AppendEntries(args, appendReply); err != nil {
		reply["type"] = FAIL
	} else {
		reply["type"] = APPEND_ENTRIES_RECV
		reply["Term"] = appendReply.Term
		reply["Success"] = appendReply.Success
		reply["ConflictIndex"] = appendReply.ConflictIndex
		reply["ConflictTerm"] = appendReply.ConflictTerm
		reply["rpcSeq"] = msg["rpcSeq"]
	}
	return reply
}

func (s *Server) handleAppendEntriesRecv(msg Message) {
	s.mu.Lock()
	index := int(msg["rpcSeq"].(float64))
	ch := s.rpcChs[index]
	s.mu.Unlock()
	ch <- msg
}

func (s *Server) sendAppendEntries(peerId int, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.mu.Lock()
	rpcSeq := s.rpcSeq
	s.rpcSeq++
	s.mu.Unlock()
	msg := Message{"src": s.serverStr, "dst": int2Str(peerId), "type": APPEND_ENTRIES_SEND,
		"leader": UNKNOWN, "rpcSeq": rpcSeq,
		"Term": args.Term,
		"LeaderId": args.LeaderId,
		"PrevLogIndex": args.PrevLogIndex,
		"PrevLogTerm": args.PrevLogTerm,
		"Entries": args.Entries,
		"LeaderCommit": args.LeaderCommit,
	}

	err := s.enc.Encode(&msg)
	if err != nil {
		return err
	}
	for {
		s.mu.Lock()
		ch := make(chan Message)
		index := rpcSeq
		s.rpcChs[index] = ch
		s.mu.Unlock()
		resp := <- ch
		reply.Term = int(resp["Term"].(float64))
		reply.Success = resp["Success"].(bool)
		reply.ConflictIndex = int(resp["ConflictIndex"].(float64))
		reply.ConflictTerm = int(resp["ConflictTerm"].(float64))
		s.wg.Add(1)
		go func() {
			s.closeRpcCh(rpcSeq)
			s.wg.Done()
		}()
		return nil
	}

}


func (s *Server) processAppliedOps(){
	for commitEntry := range s.commitChan {
		index := commitEntry.Index
		term := commitEntry.Term

		var op Op
		switch commitEntry.Command.(type) {
		case Op:
			op = commitEntry.Command.(Op)
		case map[string]interface{}:
			tmp := commitEntry.Command.(map[string]interface{})
			op = Op{tmp["Method"].(string), tmp["Key"].(string), tmp["Value"].(string)}
		}
		switch op.Method {
		case PUT:
			s.dbLock.Lock()
			s.db.Set(op.Key, op.Value)
			s.dbLock.Unlock()
		case GET:
		}
		s.mu.Lock()
		if ch, ok := s.applyChs[index]; ok{
			s.wg.Add(1)
			go func() {
				ch <- term
				s.wg.Done()
			}()
		}
		s.mu.Unlock()

	}
}

func (s *Server) closeApplyCh(index int){
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.applyChs[index])
	delete(s.applyChs, index)
}

func (s *Server) closeRpcCh(index int){
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.rpcChs[index])
	delete(s.rpcChs, index)
}