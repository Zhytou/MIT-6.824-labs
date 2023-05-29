package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

const ExecuteTimeout = 1000 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType    string
	ClientId  int64
	CommandId int64
	Key       string
	Value     string
}

type OperationContext struct {
	CommandId int64
	LastReply *CommandReply
}

type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister *raft.Persister

	// Your definitions here.
	lastApplied    int
	lastOperations map[int64]OperationContext // last operation for each client
	stateMachine   KVStateMachine             // KV stateMachine
	notifyChans    map[int]chan *CommandReply // notify client goroutine by applier goroutine to response
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) Command(args *CommandArgs, reply *CommandReply) {
	// Your code here.
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.rf.Me(), args, reply)
	// return result directly without raft layer's participation if request is duplicated
	kv.mu.RLock()
	if args.OpType != "Get" && kv.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	// do not hold lock to improve throughput
	// when KVServer holds the lock to take snapshot, underlying raft can still commit raft logs
	index, _, isLeader := kv.rf.Start(Op{OpType: args.OpType, ClientId: args.ClientId, CommandId: args.CommandId, Key: args.Key, Value: args.Value})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()
	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}
	// release notifyChan to reduce memory footprint
	// why asynchronously? to improve throughput, here is no need to block client request
	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int64) bool {
	if lastOperation, ok := kv.lastOperations[clientId]; ok {
		return lastOperation.CommandId >= commandId
	}
	return false
}

func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	if ch, ok := kv.notifyChans[index]; ok {
		close(ch)
		delete(kv.notifyChans, index)
	}
}

func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	if ch, ok := kv.notifyChans[index]; ok {
		return ch
	}
	ch := make(chan *CommandReply, 1)
	kv.notifyChans[index] = ch
	return ch
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		message := <-kv.applyCh
		DPrintf("{Node %v} tries to apply message %v", kv.rf.Me(), message)
		if message.CommandValid {
			kv.mu.Lock()
			if message.CommandIndex <= kv.lastApplied {
				DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.Me(), message, kv.lastApplied)
				kv.mu.Unlock()
				continue
			}
			
			var cmd Op = message.Command.(Op)
			kv.lastApplied = message.CommandIndex
			var reply *CommandReply
			// no need to apply to stateMachine if request is duplicated
			if cmd.OpType != "Get" && kv.isDuplicateRequest(cmd.ClientId, cmd.CommandId) {
				DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.Me(), message, kv.lastOperations[cmd.ClientId], cmd.ClientId)
				reply = kv.lastOperations[cmd.ClientId].LastReply
			} else {
				// apply log to stateMachine
				reply = &CommandReply{}
				switch cmd.OpType {
				case "Get":
					{
						reply.Value, reply.Err = kv.stateMachine.Get(cmd.Key)
					}
				case "Put":
					{
						reply.Err = kv.stateMachine.Put(cmd.Key, cmd.Value)
					}
				case "Append":
					{
						reply.Err = kv.stateMachine.Append(cmd.Key, cmd.Value)
					}
				}
				// add to lastOperations
				if cmd.OpType != "Get" {
					kv.lastOperations[cmd.ClientId] = OperationContext{CommandId: cmd.CommandId, LastReply: reply}
				}
			}

			// only notify related channel for currentTerm's log when node is leader
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
				ch := kv.getNotifyChan(message.CommandIndex)
				ch <- reply
			}

			// need snapshot
			// if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
			// 	DPrintf("{Node %v} tries to take snapshot for message %v", kv.rf.Me(), message)
			// 	kv.takeSnapshot(message.CommandIndex)
			// }
			
			kv.mu.Unlock()
			// } else if message.SnapshotValid {
			// 	kv.mu.Lock()
			// 	if kv.rf.CondInstallSnapshot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
			// 		kv.restoreSnapshot(message.Snapshot)
			// 		kv.lastApplied = message.SnapshotIndex
			// 	}
			// 	kv.mu.Unlock()
		} else {
			panic(fmt.Sprintf("unexpected Message %v", message))
		}
	}

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.dead = 0
	kv.lastApplied = 0
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandReply)
	kv.stateMachine = NewMemoryKV()
	go kv.applier()
	return kv
}
