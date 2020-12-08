package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
)

// Server wraps a raft.Raft along with a rpc.Server that exposes its
// methods as RPC endpoints. It also manages the peers of the Raft server. The
// main goal of this type is to simplify the code of raft.Server for
// presentation purposes. raft.Raft has a *Server to do its peer
// communication and doesn't have to worry about the specifics of running an
// RPC server.
type Server struct {
	mu sync.Mutex

	serverId int
	serverStr string
	peerIds  []int

	rf      *Raft
	db  Storage
	commitChan  chan CommitEntry
	quit  chan interface{}
	wg    sync.WaitGroup
	enc *json.Encoder
	applyChs 	map[int]chan int
	rpcSeq int
	rpcCh chan Message
}

func NewServer(serverId string, peerIds []int) *Server {
	s := new(Server)
	s.serverStr = serverId
	s.serverId = str2Int(serverId)
	s.peerIds = peerIds
	s.commitChan = make(chan CommitEntry)
	s.quit = make(chan interface{})
	s.applyChs = make(map[int]chan int)
	s.rpcSeq = 1
	s.rpcCh = make(chan Message, 100)
	return s
}

func (s *Server) Serve() {
	s.rf = NewRaft(s.serverId, s.peerIds, s, s.commitChan)

	conn, err := net.Dial("unixpacket", s.serverStr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	req := make(map[string]interface{})
	s.enc = json.NewEncoder(conn)
	dec := json.NewDecoder(conn)
	go s.receiveNewMsg()

	for {
		err := dec.Decode(&req)
		if err != nil {
			fmt.Println(err)
			break
		} else {
			s.wg.Add(1)
			go func() {
				s.handleGet(req)
				s.wg.Done()
			}()
		}
	}
}

type Message = map[string]interface{}

func (s *Server) handleMessage(conn net.Conn, msg Message) {
	msgType := msg["type"]
	switch msgType {
	case GET:
		reply := s.handleGet(msg)
		s.enc.Encode(&reply)
	case PUT:
		reply := s.handlePut(msg)
		s.enc.Encode(&reply)
	case REQUEST_VOTE_SEND:
		reply := s.handleAppendEntriesSend(msg)
		s.enc.Encode(&reply)
	case REQUEST_VOTE_RECV:
		s.handleAppendEntriesRecv(msg)
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
			s.mu.Lock()
			if val, ok := s.db.Get(msg["key"].(string)); ok{
				reply["value"] = val
				reply["type"] = OK
			}
			s.mu.Unlock()
		}
	}

	go func() {s.closeCh(index)}()
	return reply
}

func (s *Server) closeCh(index int){
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.applyChs[index])
	delete(s.applyChs, index)
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
	go func() {s.closeCh(index)}()
	return reply
}


func (s *Server) handleRequestVoteSend(msg Message) Message {
	args := RequestVoteArgs{}
	requestReply := new(RequestVoteReply)
	reply := s.newReply(msg)
	if err := s.rf.RequestVote(args, requestReply); err != nil {
		reply["type"] = FAIL
	} else {
		reply["type"] = REQUEST_VOTE_RECV
		reply["Term"] = requestReply.Term
		reply["VoteGranted"] = requestReply.VoteGranted
	}
	return reply
}

func (s *Server) handleRequestVoteRecv(msg Message) {
	s.rpcCh <- msg
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
		resp := <- s.rpcCh
		if resp["rpcSeq"].(int) != rpcSeq {
			s.rpcCh <- resp
		} else {
			reply.Term = resp["Term"].(int)
			reply.VoteGranted = resp["VoteGranted"].(bool)
			return nil
		}
	}
}

func (s *Server) handleAppendEntriesSend(msg Message) Message {
	args := AppendEntriesArgs{}
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
	}
	return reply
}

func (s *Server) handleAppendEntriesRecv(msg Message) {
	s.rpcCh <- msg
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
		resp := <- s.rpcCh
		if resp["rpcSeq"].(int) != rpcSeq {
			s.rpcCh <- resp
		} else {
			reply.Term = resp["Term"].(int)
			reply.Success = resp["Success"].(bool)
			reply.ConflictIndex = resp["ConflictIndex"].(int)
			reply.ConflictTerm = resp["ConflictTerm"].(int)
			return nil
		}
	}

}



func (s *Server) receiveNewMsg(){
	for commitEntry := range s.commitChan {
		s.mu.Lock()
		index := commitEntry.Index
		term := commitEntry.Term

		op := commitEntry.Command.(Op)

		switch op.Method {
		case PUT:
			s.db.Set(op.Key, op.Value)
		case GET:
		}
		if ch, ok := s.applyChs[index]; ok{
			ch <- term
		}

		s.mu.Unlock()
	}
}