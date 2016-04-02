package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "bytes"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	currentView viewservice.View

	logger *log.Logger

	// key value store.
	store map[string]string

	// Match to store client last put message to handle at-most one RPC.
	clientPutMesage map[int64]int64

	// This is server Id - provide better name for server. It is useful in logging and debugging for server identification.
	serverId int64
}

const (
	// To Enable stdout logging, set it to false.
	DISABLE_STDOUT_LOGING = true
)

// Utility function to get the value from store if present.
func (pb *PBServer) GetFromStore(args *GetArgs, reply *GetReply) error {
	value, ok := pb.store[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
		reply.Err = ErrNoKey
	}

	return nil
}

// Utility function to check whether last current message is same as current message.
// This utility functions handles at-most-one semantics.
func (pb *PBServer) isAlreadyCommitedRequest(clientId int64, messageId int64) bool {
	value, ok := pb.clientPutMesage[clientId]
	if ok {
		if value == messageId {
			return true
		}
	}

	return false
}

// Utility function to store the put message corresponding each client to handle at-most one semantics.
func (pb *PBServer) cacheClientLastCommitMessageId(clientId int64, messageId int64) bool {
	pb.clientPutMesage[clientId] = messageId
	return true
}

// Utility function to Put or Append in Key-Value store.
func (pb *PBServer) putAppendToStore(args *PutAppendArgs, reply *PutAppendReply) error {
	if args.Operation == "Put" {
		pb.store[args.Key] = args.Value
	} else {
		value, ok := pb.store[args.Key]

		if ok {
			// Append operation
			pb.store[args.Key] = value + args.Value
			pb.logger.Printf("ServerId = %d, operation = %s, currentData = %s\n", pb.serverId, args.Operation, pb.store[args.Key])

		} else {
			// Put operation
			pb.store[args.Key] = args.Value
		}
	}
	pb.logger.Printf("ServerId = %d, Updated data = %s\n", pb.serverId, pb.store[args.Key])
	return nil

}

// Get RPC used by client to get the value from key value store.
func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	var ok error
	pb.mu.Lock()
	pb.logger.Printf("ServerId = %d GET: Key = %s\n", pb.serverId, args.Key)
	// Your code here.
	if pb.currentView.Primary == pb.me {
		ok = pb.GetFromStore(args, reply)
	} else {
		pb.logger.Println("Reuqest from invalid server is received")
		reply.Err = ErrWrongServer
	}

	pb.mu.Unlock()
	return ok
}

// Put RPC used by client.
// It follows following rule -
// a. It first checks, whether it is duplicate request from client due to network failure (at-most one RPC).
//		If, request is duplicate, then it reply with success without re-updating the store.
//	b. If View has only primary, it updates the Key-Value store of Primary.
//	c. If the view also has Backup. It forward request to Backup for update. If update request in backup is successful,
//		then only it updates its value.
//   Corner case - It might happen that backup has updated its store, but success response is not received by Primary.
//				   It retries upto dead ping intervals. Retry interval is sufficient to response the Primary response successfully,
//					if it is a live. It keep retrying until viewsevice confirms its dead or succeed.
func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	pb.mu.Lock()
	var ok error
	ok = nil
	pb.logger.Printf("ServerId = %d PUTAPPEND: Key = %s, Value = %s, (%d, %d)\n", pb.serverId, args.Key, args.Value, args.ClientId, args.MessageId)
	if pb.currentView.Primary == pb.me {
		if pb.isAlreadyCommitedRequest(args.ClientId, args.MessageId) {
			pb.logger.Printf("ServerId = %d Already-Commited-PUTAPPEND: Key = %s, Value = %s, (%d, %d)\n", pb.serverId, args.Key, args.Value, args.ClientId, args.MessageId)
		} else {
			retryCount := 2 * viewservice.DeadPings
			successBackup := true
			for pb.currentView.Primary == pb.me && pb.currentView.Backup != "" && retryCount >= 0 {
				successBackup = false
				reply := &PutAppendReply{}
				backResponse := call(pb.currentView.Backup, "PBServer.ForwardedPutAppend", args, reply)

				if backResponse == false || reply.Err != OK {
					pb.logger.Printf("ServerId = %d, Error from backup = %s", pb.serverId, pb.currentView.Backup)
					time.Sleep(viewservice.PingInterval)
					retryCount = retryCount - 1
					view, _error := pb.vs.Ping(pb.currentView.Viewnum)
					if _error == nil {
						if view.Primary == pb.me && view.Backup != "" && view.Backup != pb.currentView.Backup {
							pb.sendCurrentStoreToNewBackUp(view)
						}

						pb.currentView = view
					}

				} else {
					successBackup = true
					break
				}
			}
			if successBackup {
				ok = pb.putAppendToStore(args, reply)
				pb.cacheClientLastCommitMessageId(args.ClientId, args.MessageId)
			} else {
				reply.Err = ErrServerError
				pb.logger.Printf("Server = %d is no longer primary, while retrying for backup", pb.serverId)
			}

		}
	} else {
		pb.logger.Printf("ServerId = %d PutAppend: Dropping as It is no longer primary", pb.serverId)
		reply.Err = ErrServerError
	}

	pb.mu.Unlock()
	return ok
}

// RPC function to sync Primary store to Backup.
func (pb *PBServer) SyncWithPrimary(args *PutBackupArgs, reply *PutBackupReply) error {
	pb.mu.Lock()
	pb.logger.Printf("ServerId = %d ,SyncWithPrimary\n", pb.serverId)
	pb.store = args.PrimaryStore
	reply.Err = OK
	pb.mu.Unlock()
	return nil
}

// Put or Append RPC forwarded by Primary server to Backup server.
func (pb *PBServer) ForwardedPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	pb.logger.Printf("ServerId = %d FWDPUTAPPEND: Key = %s, Value = %s, (%d, %d)\n", pb.serverId, args.Key, args.Value, args.ClientId, args.MessageId)
	if pb.currentView.Backup == pb.me {
		if pb.isAlreadyCommitedRequest(args.ClientId, args.MessageId) {
			pb.logger.Printf("ServerId = %d Already-Commited-PUTAPPEND: Key = %s, Value = %s, (%d, %d)\n", pb.serverId, args.Key, args.Value, args.ClientId, args.MessageId)
		} else {
			pb.putAppendToStore(args, reply)
			pb.cacheClientLastCommitMessageId(args.ClientId, args.MessageId)
		}

		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

// Utility function to send the Primary store to backup server.
func (pb *PBServer) sendCurrentStoreToNewBackUp(currentView viewservice.View) bool {
	args := &PutBackupArgs{}
	args.PrimaryStore = pb.store
	var reply PutBackupReply
	ok := call(currentView.Backup, "PBServer.SyncWithPrimary", args, &reply)
	if ok == false || reply.Err != OK {
		pb.logger.Printf("ServerId = %d Error in restoring \n", pb.serverId)
		return false
	} else {
		pb.logger.Printf("ServerId = %d Successfully synced backup\n", pb.serverId)
	}
	return true
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	pb.mu.Lock()
	view, _error := pb.vs.Ping(pb.currentView.Viewnum)

	pb.logger.Printf("Server Id = %d, CurrentServer = %s, Primary =%s, Backup =%s, Viewnum =%d", pb.serverId, pb.me, view.Primary, view.Backup, view.Viewnum)
	if _error == nil {
		if view.Primary == pb.me {
			if view.Backup != "" && view.Backup != pb.currentView.Backup {
				if pb.sendCurrentStoreToNewBackUp(view) {
					pb.currentView = view
					pb.vs.Ping(pb.currentView.Viewnum)
				} else {
					pb.currentView = view
				}

			} else {
				pb.currentView = view
			}
		} else {
			pb.currentView = view
		}
	} else {
		pb.currentView.Viewnum = 0
		pb.currentView.Primary = ""
		pb.currentView.Backup = ""
	}

	pb.mu.Unlock()
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.

	pb.store = make(map[string]string)
	pb.clientPutMesage = make(map[int64]int64)
	pb.serverId = nrand()

	pb.logger = log.New(os.Stdout, "logger: ", log.Lshortfile)
	if DISABLE_STDOUT_LOGING {
		var buf bytes.Buffer
		pb.logger = log.New(&buf, "logger: ", log.Lshortfile)
	}

	pb.logger.Printf("Server = %s ==>  ServerId = %d", pb.me, pb.serverId)
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
