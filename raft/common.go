package raft

import (
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
)

const (
	GET                 = "get"
	PUT                 = "put"
	APPEND_ENTRIES_SEND = "append_entries_send"
	APPEND_ENTRIES_RECV = "append_entries_recv"
	REQUEST_VOTE_SEND   = "request_vote_send"
	REQUEST_VOTE_RECV   = "request_vote_recv"
	REDIRECT            = "redirect"
	OK                  = "ok"
	FAIL                = "fail"
	UNKNOWN             = "FFFF"
)

func str2Int(src string) int {
	num, _ := strconv.Atoi(src)
	return num
}

func int2Str(n int) string {
	return fmt.Sprintf("%04d", n)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method string //Put or Append or Get
	Key    string
	Value  string
}

const Debug = 1

var (
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

//初始化log
func init() {
	infoFile, err := os.OpenFile("info.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open infoFile failed.\n", err)
	}
	warnFile, err := os.OpenFile("warn.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open warnFile failed.\n", err)
	}
	errFile, err := os.OpenFile("err.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Open warnFile failed.\n", err)
	}
	//log.Lshortfile打印出错的函数位置
	Info = log.New(io.MultiWriter(os.Stderr, infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
	Warn = log.New(io.MultiWriter(os.Stderr, warnFile), "Warn:", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
}

func DPrintf(show bool, level string, format string, a ...interface{}) (n int, err error) {
	if Debug == 0 {
		return
	}
	if show == false {
		//是否打印当前raft实例的log
		return
	}

	if level == "info" {
		Info.Printf(format, a...)
	} else if level == "warn" {
		Warn.Printf(format, a...)
	} else {
		Error.Fatalln("log error!")
	}
	return
}
