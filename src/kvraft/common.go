package kvraft

type Err string

const (
	OK				Err = "OK"
	ErrNoKey		Err = "ErrNoKey"
	ErrWrongLeader	Err = "ErrWrongLeader"
)

type Action String

const (
	GET		Action = "GET"
	PUT		Action = "PUT"
	APPEND	Action = "APPEND"
)

// Put or Append
type PutAppendArgs struct {
	OpId		string
	Key			string
	Value		string
	Op			Action // "Put" or "Append"
}

type PutAppendReply struct {
	Err 		Err
}

type GetArgs struct {
	OpId		string
	Key			string
}

type GetReply struct {
	Err			Err
	Value		string
}
