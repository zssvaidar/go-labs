package kvraft

import "encoding/gob"

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
)

// ClientId and Seq identify a request so a retried Put or Append is
// applied only once, even if the first attempt did commit.
type PutAppendArgs struct {
	Key      string
	Value    string
	Append   bool
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

func init() {
	// Requests travel through the Raft log inside rsm.Op.Req (an `any`),
	// so gob needs to know their concrete types.
	gob.Register(PutAppendArgs{})
	gob.Register(GetArgs{})
}
