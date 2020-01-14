package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type CommandType int

const (
	GetType CommandType = iota
	PutType
	AppendType
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  CommandType
	Key   string
	Value string

	ClientId  int64
	RequestId int64
}

type KVPersistentState struct {
	Store         map[string]string
	LastRequestId map[int64]int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string
	replies map[int]chan raft.ApplyMsg // chan per log index

	persister *raft.Persister

	// stores last handled requestId per client
	lastRequestId map[int64]int64
}

func (kv *KVServer) request(args interface{}) (raft.ApplyMsg, bool) {
	var msg raft.ApplyMsg

	var op Op
	switch v := args.(type) {
	case GetArgs:
		op = Op{GetType, v.Key, "", v.ClientId, v.RequestId}
	case PutAppendArgs:
		if v.Op == "Put" {
			op = Op{PutType, v.Key, v.Value, v.ClientId, v.RequestId}
		} else {
			op = Op{AppendType, v.Key, v.Value, v.ClientId, v.RequestId}
		}
	}

	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return msg, false
	}

	kv.mu.Lock()
	requestChan := make(chan raft.ApplyMsg, 1)
	kv.replies[index] = requestChan
	kv.mu.Unlock()

	leaderCheckTicker := time.Tick(50 * time.Millisecond)

	for {
		select {
		case msg := <-requestChan:
			kv.mu.Lock()
			delete(kv.replies, index)
			kv.mu.Unlock()
			return msg, true
		case <-leaderCheckTicker:
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.mu.Lock()
				delete(kv.replies, index)
				kv.mu.Unlock()
				return msg, false
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	msg, isLeader := kv.request(*args)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	if !msg.CommandValid {
		reply.Err = "Invalid Message"
		return
	}

	reply.Err = ""

	kv.mu.Lock()
	val, ok := kv.store[args.Key]
	kv.mu.Unlock()

	if ok {
		reply.Value = val
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	msg, isLeader := kv.request(*args)

	if !isLeader {
		reply.WrongLeader = true
		return
	}

	reply.WrongLeader = false

	if !msg.CommandValid {
		reply.Err = "Invalid Message"
		return
	}

	reply.Err = ""
}

func (kv *KVServer) run() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()

			if msg.IsSnapshot {
				kv.loadSnapshot(msg.SnapshotData)
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)
			if op.Type == PutType || op.Type == AppendType {

				prevRequestId, ok := kv.lastRequestId[op.ClientId]
				if !ok || prevRequestId != op.RequestId {
					if op.Type == PutType {
						kv.store[op.Key] = op.Value
					} else if op.Type == AppendType {
						kv.store[op.Key] += op.Value
					}
					kv.lastRequestId[op.ClientId] = op.RequestId
				}
			}

			index := msg.CommandIndex
			ch, ok := kv.replies[index]
			if ok {
				ch <- msg
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
				kv.takeLogSnapshot(index)
			}

			kv.mu.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *KVServer) takeLogSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	persistentState := &KVPersistentState{
		Store:         kv.store,
		LastRequestId: kv.lastRequestId,
	}
	e.Encode(persistentState)

	kv.rf.TruncateLog(commandIndex, w.Bytes())
}

func (kv *KVServer) loadSnapshot(data []byte) {
	if data != nil && len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)

		persistentState := &KVPersistentState{}
		d.Decode(persistentState)
		kv.store = persistentState.Store
		kv.lastRequestId = persistentState.LastRequestId
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.replies = make(map[int]chan raft.ApplyMsg)
	kv.store = make(map[string]string)
	kv.lastRequestId = make(map[int64]int64)
	kv.persister = persister

	kv.loadSnapshot(kv.persister.ReadSnapshot())

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.run()

	return kv
}
