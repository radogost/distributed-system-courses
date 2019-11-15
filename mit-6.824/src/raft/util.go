package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

// The Raft paper mentions elections timeouts in the rang eof 150 to 300 ms.
// However, in this lab, the tester limits to 10 heartbeats per second.
// Therefore, this function returns a higher election timeout,
// one between 500ms and 1s.
func randomTimeout() time.Duration {
	minVal := 500
	maxVal := 1000
	timeout := rand.Intn(maxVal-minVal) + minVal
	return time.Duration(timeout) * time.Millisecond
}
