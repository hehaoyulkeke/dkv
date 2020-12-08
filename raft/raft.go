package raft

import (
	"math/rand"
	"sync"
	"time"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//term
const (
	Leader           = "Leader"
	Candidate        = "Candidate"
	Follower         = "Follower"
	votedNull        = -1                                    //本轮没有投票给任何人
	heartBeat        = time.Duration(100)                    //leader的心跳时间
	RPC_CALL_TIMEOUT = time.Duration(500) * time.Millisecond //rpc超时时间
)

// field names must start with capital letters!
type AppendEntriesArgs struct {
	Term     int //Leader's term
	LeaderId int
	//PreLogIndex和PrevLogTerm用来确定leader和收到这条信息的follower上一条同步的信息
	//方便回滚，或者是新leader上线后覆盖follower的日志
	PrevLogIndex int       //index of log entry immediately preceding new ones
	PrevLogTerm  int       //term of prevLogIndex entry
	Entries      []Entries //log entries to store(empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int       //leader's commitIndex
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term    int  //接收到信息的follower的currentTerm，方便过期leader更新信息。
	Success bool // true if follower contained entry matching prevLogIndex and PrevLogTerm

	//follower节点第一个与args.Term不相同的日志下标。
	//一个冲突的term一次append RPC就能排除
	//如果follower和leader隔了好几个term
	//只要找到leader中等于confilctTerm的最后一个日志，就能一次性添加所有follower缺失的日志
	ConflictIndex int //冲突日志处的下标
	ConflictTerm  int //冲突日志处（term不匹配或者follower日志较少）的term
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int //发出选票的candidate的id，这里是server中的下标
	//LastLogIndex和LastLogTerm合在一起用来比较candidate和follower谁更“新”
	LastLogIndex int //发出选票的candidate的最后一个日志的下标
	LastLogTerm  int //发出选票的candidate的最后一个日志对应的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //返回接收者的currentTerm，一般是针对过期leader
	//如果candidate term < 接收者 term ==>false
	//如果接收者的votedFor == (null or candidateId)
	//并且candidate的日志和接收者的日志一样“新“ ==> true 表示我投票给你了
	//接收者投票之后会改变自己的voterFor
	VoteGranted bool
}

type Entries struct {
	Term    int //该日志所属的Term
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
//每一个raft peer都叫server，然后分为leader,candidate,follower三种角色，但是内部的状态都是一样的
type Raft struct {
	mu    sync.Mutex // Lock to protect shared access to this peer's state
	peers []int      // RPC end points of all peers
	me    int        // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role string //Leader or candidate or follower
	//int32固定格式进行persist
	currentTerm   int //该server属于哪个term
	votedFor      int //代表所投的server的下标,初始化为-1,表示本follower的选票还在，没有投票给其他人
	votedThisTerm int //判断rf.votedFor是在哪一轮承认的leader，防止一个节点同一轮投票给多个candidate

	logEntries []Entries //保存执行的命令，下标从1开始

	commitIndex int //最后一个提交的日志，下标从0开始
	lastApplied int //最后一个应用到状态机的日志，下标从0开始

	//leader独有，每一次election后重新初始化
	nextIndex  []int //保存发给每个follower的下一条日志下标。初始为leader最后一条日志下标+1
	matchIndex []int //对于每个follower，已知的最后一条与该follower同步的日志，初始化为0。也相当于follower最后一条commit的日志

	appendCh chan *AppendEntriesArgs //用于follower判断在election timeout时间内有没有收到心跳信号
	voteCh   chan *RequestVoteArgs   //投票后重启定时器

	applyCh chan ApplyMsg //每commit一个log，就执行这个日志的命令，在实验中，执行命令=给applyCh发送信息

	log bool //是否输出这一个raft实例的log,若为false则不输出，相当于这个实例“死了”

	electionTimeout *time.Timer //选举时间定时器
	heartBeat       int         //心跳时间定时器

}

func (rf *Raft) leader() {

	rf.mu.Lock()
	curLogLen := len(rf.logEntries) - 1
	//记录成为leader时的term，防止被后面的操作修改
	//leader发送的term应该是创建goroutine时的term
	//否则考虑LeaderA term1给followerB term1发送第一次消息，然而延迟，B没收到消息，超时
	//B变成candidate，term=2发送选票
	//此时A已经停了一个心跳的时间，已经开启了给B发的第二次goroutine，但是还未执行
	//A投票给B,并且term=2，此时A变成follower
	//然而由于发送消息是并发goroutine，A变为follower不会停止这个goroutine的执行。
	// 如果用rf.currentTerm,此时A的term为2，执行第二个发送给B消息的goroutine。
	//candidate B收到了来自term相同的leader的消息，变为follower。
	//最后就是A和B同时变成了follower。
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	for followerId, _ := range rf.peers {
		if followerId == rf.me {
			continue
		}
		rf.nextIndex[followerId] = curLogLen + 1
		rf.matchIndex[followerId] = 0
	}

	for {
		commitFlag := true //超半数commit只修改一次leader的logEntries
		commitNum := 1     //记录commit某个日志的节点数量
		commitL := new(sync.Mutex)
		//用来通知前一半commit的节点，这个日志commit了，可以修改对应的leader.nextIndex[server]
		commitCond := sync.NewCond(commitL)

		//广播消息
		for followerId, _ := range rf.peers {
			if followerId == rf.me {
				continue
			}
			//每一个节点的请求参数都不一样
			rf.mu.Lock()
			//每个节点由于nextIndex不同，每次需要更新的日志也不同
			//不使用matchIndex而使用nextIndex的原因是
			//当发送了12没收到回复,然后发送345时
			//使用matchIndex需要发送1~5
			//而使用nextIndex只需要发送345即可，可以节省带宽
			//不用怕如果follower先收到345，因为345的prevLogIndex和follower的不匹配.
			//appendArgs := &AppendEntriesArgs{curTerm,
			//	rf.me,
			//	rf.matchIndex[followerId],
			//	rf.logEntries[rf.matchIndex[followerId]].Term,
			//	rf.logEntries[rf.matchIndex[followerId]+1:],
			//	rf.commitIndex}
			appendArgs := &AppendEntriesArgs{curTerm,
				rf.me,
				rf.nextIndex[followerId] - 1,
				rf.logEntries[rf.nextIndex[followerId]-1].Term,
				rf.logEntries[rf.nextIndex[followerId]:],
				rf.commitIndex}
			rf.mu.Unlock()

			//发送心跳信息
			go func(server int) {
				reply := &AppendEntriesReply{}
				//DPrintf(rf.log, "info", "me:%2d term:%3d | leader send message to %3d\n", rf.me, curTerm, server)
				if ok := rf.peers[server].Call("Raft.AppendEntries", appendArgs, reply); ok {
					//本轮新增的日志数量
					appendEntriesLen := len(appendArgs.Entries)

					rf.mu.Lock()
					defer rf.mu.Unlock()

					if rf.currentTerm != curTerm || appendEntriesLen == 0 {
						return
					}

					if reply.Success {
						curCommitLen := appendArgs.PrevLogIndex + appendEntriesLen
						if curCommitLen >= rf.nextIndex[server] {
							rf.nextIndex[server] = curCommitLen + 1
						} else {
							return
						}

						commitCond.L.Lock()
						defer commitCond.L.Unlock()

						commitNum = commitNum + 1
						if commitFlag && commitNum > len(rf.peers)/2 {
							//第一次超半数commit
							commitFlag = false
							DPrintf(rf.log, "info", "me:%2d term:%3d | curCommitLen:%3d  rf.commitIndex:%3d\n",
								rf.me, rf.currentTerm, curCommitLen, rf.commitIndex)

							if curCommitLen > rf.lastApplied {
								for i := rf.lastApplied + 1; i <= curCommitLen; i++ {
									//leader将本条命令应用到状态机
									rf.applyCh <- ApplyMsg{true, rf.logEntries[i].Command, i}
									//通知client，本条命令成功commit
									//上面必须for循环，因为消息要按顺序执行
									DPrintf(rf.log, "info", "me:%2d term:%3d | Leader Commit:%4d OK, commitIndex:%3d\n",
										rf.me, rf.currentTerm, rf.logEntries[i].Command, i)
								}
								rf.lastApplied = curCommitLen
								rf.commitIndex = curCommitLen
							}

						}
						//只要是成功commit的follower，就修改其matchIndex
						rf.matchIndex[server] = curCommitLen

					} else {
						//append失败
						if reply.Term > curTerm {
							//返回的term比发送信息时leader的term还要大
							rf.currentTerm = reply.Term
							rf.changeRole(Follower)
							//暂时不知道新leader是谁，等待新leader的心跳信息
							rf.appendCh <- &AppendEntriesArgs{rf.currentTerm, -1, -1, -1, make([]Entries, 0), -1}
						} else {
							//prevLogIndex or prevLogTerm不匹配
							rf.nextIndex[server] = reply.ConflictIndex
							DPrintf(rf.log, "info", "me:%2d term:%3d | Msg to %3d append fail,decrease nextIndex to:%3d\n",
								rf.me, rf.currentTerm, server, rf.nextIndex[server])
						}
					}
				}
			}(followerId)
		}

		select {
		case args := <-rf.appendCh:
			DPrintf(rf.log, "warn", "me:%2d term:%3d | new leader:%3d , leader convert to follower!\n",
				rf.me, rf.currentTerm, args.LeaderId)
			rf.changeRole(Follower)
			return
		case args := <-rf.voteCh:
			DPrintf(rf.log, "warn", "me:%2d term:%3d | role:%12s vote to candidate %3d\n", rf.me, rf.currentTerm, rf.role, args.CandidateId)
			rf.changeRole(Follower)
			return
		case <-time.After(time.Duration(rf.heartBeat) * time.Millisecond):
			//do nothing
		}
	}
}

func (rf *Raft) candidate() {
	rf.mu.Lock()
	//candidate term已经在changeRole里+1了
	//发给每个节点的请求参数都是一样的。
	logLen := len(rf.logEntries) - 1
	requestArgs := &RequestVoteArgs{rf.currentTerm, rf.me, logLen, rf.logEntries[logLen].Term}
	rf.persist()
	rf.mu.Unlock()

	voteCnt := 1              //获得的选票，自己肯定是投给自己啦
	voteFlag := true          //收到过半选票时管道只通知一次
	voteOK := make(chan bool) //收到过半选票
	voteL := sync.Mutex{}

	rf.resetTimeout()
	for followerId, _ := range rf.peers {
		if followerId == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, requestArgs, reply); ok {
				//ok仅仅代表得到回复，
				//ok==false代表本次发送的消息丢失，或者是回复的信息丢失
				if reply.VoteGranted {
					//收到投票
					voteL.Lock()
					voteCnt = voteCnt + 1
					if voteFlag && voteCnt > len(rf.peers)/2 {
						voteFlag = false
						voteL.Unlock()
						voteOK <- true
					} else {
						voteL.Unlock()
					}
				}

			}
		}(followerId)
	}

	select {
	case <-rf.appendCh:
		//收到心跳信息
		//case args <- rf.appendCh:
		//DPrintf(rf.log, "info", "me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.LeaderId)
		rf.changeRole(Follower)
		return
	case args := <-rf.voteCh:
		//投票给某人
		DPrintf(rf.log, "warn", "me:%2d term:%3d | role:%12s vote to candidate %3d\n", rf.me, rf.currentTerm, rf.role, args.CandidateId)
		rf.changeRole(Follower)
		return
	case <-voteOK:
		rf.changeRole(Leader)
		return
	case <-rf.electionTimeout.C:
		//超时
		DPrintf(rf.log, "warn", "me:%2d term:%3d | candidate timeout!\n", rf.me, rf.currentTerm)
		rf.changeRole(Follower)
		return
	}
}

func (rf *Raft) follower() {
	for {
		rf.resetTimeout()
		select {
		case <-rf.appendCh:
			//收到心跳信息
			//DPrintf(rf.log, "info", "me:%2d term:%3d | receive heartbeat from leader %3d\n", rf.me, rf.currentTerm, args.LeaderId)
		case args := <-rf.voteCh:
			//投票给某人
			DPrintf(rf.log, "warn", "me:%2d term:%3d | role:%12s vote to candidate %3d\n", rf.me, rf.currentTerm, rf.role, args.CandidateId)
		case <-rf.electionTimeout.C:
			//超时
			DPrintf(rf.log, "warn", "me:%2d term:%3d | follower timeout!\n", rf.me, rf.currentTerm)
			rf.changeRole(Candidate)
			return
		}
	}
}

func Make(peers []int, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.role = Follower

	//log下标从1开始，0是占位符，没有意义
	rf.logEntries = make([]Entries, 1)
	//term为-1,表示第一个leader的term编号是0,test只接受int型的command
	rf.logEntries[0] = Entries{-1, -1}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendCh = make(chan *AppendEntriesArgs)
	rf.voteCh = make(chan *RequestVoteArgs)

	rf.log = true

	//要加这个，每次的rand才会不一样
	rand.Seed(time.Now().UnixNano())

	//心跳间隔：rf.heartBeat * time.Millisecond
	rf.heartBeat = 100

	DPrintf(rf.log, "info", "Create a new server:[%3d]! term:[%3d]\n", rf.me, rf.currentTerm)
	go rf.run()

	return rf
}

func (rf *Raft) run() {
	for {
		switch rf.role {
		case Leader:
			rf.leader()
		case Candidate:
			rf.candidate()
		case Follower:
			rf.follower()
		}
	}
}

//修改角色
func (rf *Raft) changeRole(role string) {
	switch role {
	case Leader:
		DPrintf(rf.log, "warn", "me:%2d term:%3d | %12s change role to Leader!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Leader
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		rf.votedThisTerm = rf.currentTerm
		rf.votedFor = rf.me
		DPrintf(rf.log, "warn", "me:%2d term:%3d | %12s change role to candidate!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Candidate
	case Follower:
		DPrintf(rf.log, "warn", "me:%2d term:%3d | %12s change role to follower!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Follower
	}
}

//这里是站在接收者follower的角度写的
//实现接收者接到一个请求投票时的逻辑
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.currentTerm > args.Term {
		//过期candidate
		return
	}
	if rf.currentTerm == args.Term && rf.role == Leader {
		//同一个term的leader忽略同一个term的candidate发起的投票
		return
	}
	//只要接到candidate的投票
	//就会改变自己的currentTerm
	rf.currentTerm = args.Term

	curLogLen := len(rf.logEntries) - 1

	DPrintf(rf.log, "info", "me:%2d term:%3d curLogLen:%3d logTerm:%3d | candidate:%3d lastLogIndex:%3d lastLogTerm:%3d\n",
		rf.me, rf.currentTerm, curLogLen, rf.logEntries[curLogLen].Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
	if args.LastLogTerm > rf.logEntries[curLogLen].Term || (args.LastLogTerm == rf.logEntries[curLogLen].Term && args.LastLogIndex >= curLogLen) {
		//candidate日志比本节点的日志“新”

		//判断这一轮选举内是否已经投票给某人
		if rf.votedThisTerm < args.Term {
			rf.votedFor = args.CandidateId
			rf.votedThisTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			go func() { rf.voteCh <- args }()
		}
	}

}

//leader调用follower的AppendEntries RPC服务
//站在follower角度完成下面这个RPC调用
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//follower收到leader的信息
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.ConflictIndex = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	curLogLength := len(rf.logEntries) - 1
	log_less := curLogLength < args.PrevLogIndex
	//接收者日志大于等于leader发来的日志  且 日志项不匹配
	log_dismatch := !log_less && rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm
	if log_less || log_dismatch {
		reply.Term = rf.currentTerm
		reply.Success = false

		if log_dismatch {
			//日志项不匹配，将follower这一个term所有的日志回滚
			for index := curLogLength - 1; index >= 0; index-- {
				if rf.logEntries[index].Term != rf.logEntries[index+1].Term {
					reply.ConflictIndex = index + 1
				}
			}
		}
		if log_less {
			//如果follower日志较少
			reply.ConflictIndex = curLogLength + 1
		}
		DPrintf(rf.log, "info", "me:%2d term:%3d | receive leader:[%3d] message but not match!\n", rf.me, rf.currentTerm, args.LeaderId)
	} else {
		//接收者的日志大于等于prevlogindex
		rf.currentTerm = args.Term
		reply.Success = true

		//修改日志长度
		//找到接收者和leader（如果有）第一个不相同的日志
		leng := min(curLogLength-args.PrevLogIndex, len(args.Entries))
		i := 0
		for ; i < leng; i++ {
			if rf.logEntries[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.logEntries = rf.logEntries[:args.PrevLogIndex+i+1]
				break
			}
		}
		if i != len(args.Entries) {
			rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
		}

		if len(args.Entries) != 0 {
			//心跳信号不输出
			//心跳信号可能会促使follower执行命令
			DPrintf(rf.log, "info", "me:%2d term:%3d | receive new command:%3d from leader:%3d, size:%3d\n", rf.me, rf.currentTerm, rf.logEntries[len(rf.logEntries)-1].Command, args.LeaderId, args.PrevLogIndex+len(args.Entries)-rf.commitIndex)

		}

		//修改commitIndex
		if args.LeaderCommit > rf.commitIndex {
			newCommitIndex := min(args.LeaderCommit, len(rf.logEntries)-1)
			//不能用goroutine，因为程序要求log按顺序交付
			for i := rf.lastApplied + 1; i <= newCommitIndex; i++ {
				rf.applyCh <- ApplyMsg{true, rf.logEntries[i].Command, i}
			}
			rf.commitIndex = newCommitIndex
			rf.lastApplied = newCommitIndex
			DPrintf(rf.log, "info", "me:%2d term:%3d | Follower commit! cmd:%3d CommitIndex:%3d\n", rf.me, rf.currentTerm, rf.logEntries[rf.commitIndex].Command, rf.commitIndex)
		}
	}
	//通知follower，接收到来自leader的消息
	//即便日志不匹配，但是也算是接收到了来自leader的心跳信息。
	go func() { rf.appendCh <- args }()
}

//获取随机时间，用于选举
func (rf *Raft) randTime() int {
	basicTime := 400
	randNum := 150
	//随机时间：basicTime + rand.Intn(randNum)
	timeout := basicTime + rand.Intn(randNum)
	return timeout
}

//重置定时器
func (rf *Raft) resetTimeout() {
	if rf.electionTimeout == nil {
		rf.electionTimeout = time.NewTimer(time.Duration(rf.randTime()) * time.Millisecond)
	} else {
		rf.electionTimeout.Reset(time.Duration(rf.randTime()) * time.Millisecond)

	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
	if isLeader {
		//logEntries有一个占位符，所以其长度为3时，表明里面有2个命令，而新来的命令的提交index就是3,代表是第三个提交的。
		index = len(rf.logEntries)
		rf.logEntries = append(rf.logEntries, Entries{rf.currentTerm, command})
		DPrintf(rf.log, "info", "me:%2d term:%3d | Leader receive a new command:%3d\n", rf.me, rf.currentTerm, command.(int))

	}

	return index, term, isLeader
}
