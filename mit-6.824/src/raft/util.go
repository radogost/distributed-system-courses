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

func max(x, y int) int {
	if x > y {
		return x
	} else {
		return y
	}
}

func randomTimeout(minVal, maxVal int) time.Duration {
	timeout := rand.Intn(maxVal-minVal) + minVal
	return time.Duration(timeout) * time.Millisecond
}
