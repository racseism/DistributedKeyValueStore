package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"
import "log"
import "os"
import "bytes"
import "time"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary  string
	logger   *log.Logger
	clientId int64
}

const (
	// To Enable stdout logging, set it to false.
	DISABLE_CLIENT_STDOUT_LOGING = true
)

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.logger = log.New(os.Stdout, "PB:client: ", log.Lshortfile)
	ck.clientId = nrand()
	if DISABLE_CLIENT_STDOUT_LOGING {
		var buf bytes.Buffer
		ck.logger = log.New(&buf, "PB:client: ", log.Lshortfile)
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

func (ck *Clerk) Get(key string) string {

	value := ""
	for true {
		if ck.primary == "" {
			ck.primary = ck.vs.Primary()
		} else {
			args := &GetArgs{}
			args.Key = key
			var reply GetReply
			ck.logger.Printf("Request:Get clientId = %d, Key = %s", ck.clientId, key)
			ok := call(ck.primary, "PBServer.Get", args, &reply)
			if ok == false || reply.Err == ErrWrongServer {
				ck.logger.Printf("Response:Get Error = %s", reply.Err)
				ck.primary = ""
				time.Sleep(100 * time.Millisecond)
			} else {
				// If the Key does not exits its value would be emty
				ck.logger.Printf("Response:Get Value = %s", reply.Value)
				value = reply.Value
				break
			}
		}
	}
	return value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	messageId := nrand()
	for true {
		if ck.primary == "" {
			ck.primary = ck.vs.Primary()
		} else {
			args := &PutAppendArgs{}
			args.Key = key
			args.Value = value
			args.Operation = op
			args.ClientId = ck.clientId
			args.MessageId = messageId
			var reply PutAppendReply
			ck.logger.Printf("PutAppend ClientId = %d, key = %s, value = %s, operation = %s", ck.clientId, key, value, op)
			ok := call(ck.primary, "PBServer.PutAppend", args, &reply)
			if ok == false || reply.Err == ErrWrongServer || reply.Err == ErrServerError {
				ck.primary = ""
				time.Sleep(10 * time.Millisecond)
				ck.logger.Printf("PutAppen Response error = %s", reply.Err)
			} else {
				// If the Key does not exits its value would be emty
				ck.logger.Printf("PutAppend response successfull. Response Code  = %s\n", reply.Err)
				break
			}
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
