package viewservice

import "time"

type View struct {
	Viewnum uint
	Primary string
	Backup  string
}

const PingInterval = time.Millisecond * 100

const DeadPings = 5

type PingArgs struct {
	Me      string // "host:port"
	Viewnum uint   // caller's notion of current view #
}

type PingReply struct {
	View View
}

type GetArgs struct {
}

type GetReply struct {
	View View
}
