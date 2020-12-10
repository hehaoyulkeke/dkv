package main

import (
	"3700kvstore/raft"
	"os"
	"strconv"
)

func main() {
	args := os.Args
	id := args[1]
	otherArgs := args[2:]
	peerIds := make([]int, 0)
	for i := range otherArgs {
		n, _ := strconv.Atoi(otherArgs[i])
		peerIds = append(peerIds, n)
	}

	server := raft.NewServer(id, peerIds)
	server.Serve()
}
