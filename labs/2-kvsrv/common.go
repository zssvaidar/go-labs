package kvsrv

// ClientId and Seq identify a Put or Append, so a retry after a lost reply
// is recognized and not applied twice.
type PutAppendArgs struct {
	Key      string
	Value    string
	ClientId int64
	Seq      int64
}

type PutAppendReply struct {
	Value string // Append returns the value before the append
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
