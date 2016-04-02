package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrServerError = "ErrServerError"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Operation string
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// Client Id to identify the identity of client
	ClientId int64

	// Message id to handle at most one scenarios
	MessageId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
// Below strcture will be used to transfer Primary store to Backup if new backup is
// registered in views.
type PutBackupArgs struct {
	PrimaryStore map[string]string
}

type PutBackupReply struct {
	Err Err
}
