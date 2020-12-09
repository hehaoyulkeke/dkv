package raft

import (
	"fmt"
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
