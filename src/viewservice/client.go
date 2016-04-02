package viewservice

import "net/rpc"
import "fmt"
import "log"
import "os"
import "bytes"

//
// the viewservice Clerk lives in the client
// and maintains a little state.
//
type Clerk struct {
	me     string // client's name (host:port)
	server string // viewservice's host:port
	logger *log.Logger
}

func MakeClerk(me string, server string) *Clerk {
	ck := new(Clerk)
	ck.me = me
	ck.server = server
	ck.logger = log.New(os.Stdout, "logger: ", log.Lshortfile)
	if DISABLE_STDOUT_LOGING {
		var buf bytes.Buffer
		ck.logger = log.New(&buf, "logger: ", log.Lshortfile)
	}
	return ck
}

func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (ck *Clerk) Ping(viewnum uint) (View, error) {
	// prepare the arguments.
	args := &PingArgs{}
	args.Me = ck.me
	args.Viewnum = viewnum
	var reply PingReply

	// send an RPC request, wait for the reply.
	ok := call(ck.server, "ViewServer.Ping", args, &reply)
	if ok == false {
		return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
	}

	ck.logger.Printf("Received ping Response - ViewName = %s  ViewNo = %d", reply.View.Primary, reply.View.Viewnum)
	return reply.View, nil
}

func (ck *Clerk) Get() (View, bool) {
	args := &GetArgs{}
	var reply GetReply
	ok := call(ck.server, "ViewServer.Get", args, &reply)
	if ok == false {
		return View{}, false
	}
	return reply.View, true
}

func (ck *Clerk) Primary() string {
	v, ok := ck.Get()
	if ok {
		ck.logger.Printf("Success Received, Primary = %s \n", v.Primary)
		return v.Primary
	}
	return ""
}
