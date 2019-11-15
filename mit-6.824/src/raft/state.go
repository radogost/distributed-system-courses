package raft

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)
