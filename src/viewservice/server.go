package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import "bytes"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	// Keeping track of last ping received time for each server
	serverLastHeartBeat map[string]time.Time

	// Current server view
	currentView View

	// Current state of view server. It starts from NO_PRIMARY_CURRENTLY
	currentState int32

	// This view maintains - view, which has not been acknowledged, but multiple server registers during
	// the waiting for acknowledgement from Primary. So We store it as future views.
	unacknowledgedView View

	// Logging
	logger *log.Logger
}

const (
	// This is bootstrapping stage. Sysmte is starting up.
	NO_PRIMARY_CURRENTLY = 0

	// Primary Server is choosen and view server is waiting for ACK from primary server.
	WAITING_FOR_PRIMARY_ACK = 1

	// Primary Server ACK is confirmed.
	PRIMARY_ACK_CONFIRMED = 2

	// system reached to error state.
	ERROR_STATE = 3
)

const (
	// To Enable stdout logging, set it to false.
	DISABLE_STDOUT_LOGING = true
)

// This function convert state to string. Useful for logging.
func (vs *ViewServer) convertStateToString() string {
	switch vs.currentState {
	case NO_PRIMARY_CURRENTLY:
		return "NO_PRIMARY_CURRENTLY"
	case WAITING_FOR_PRIMARY_ACK:
		return "WAITING_FOR_PRIMARY_ACK"
	case PRIMARY_ACK_CONFIRMED:
		return "PRIMARY_ACK_CONFIRMED"
	}

	return "ERROR_STATE"
}

// This function returns idle server. Idle sever should not be Primary or Backup server.
// If Backup becomes primary, Idle server from machine pool become Backup if available.
func (vs *ViewServer) getIdleServer() string {
	currentTime := time.Now()
	for server := range vs.serverLastHeartBeat {
		lastPing := vs.serverLastHeartBeat[server]
		duration := currentTime.Sub(lastPing)
		if duration < (DeadPings * PingInterval) {
			if vs.currentView.Primary != server && vs.currentView.Backup != server {
				return server
			}
		}
	}

	return ""
}

func (vs *ViewServer) logIfViewChanged(oldView View, newView View, oldState int32, newState int32) {
	if oldView.Primary == newView.Primary && oldView.Backup == newView.Backup && oldView.Viewnum == newView.Viewnum && oldState == newState {

	} else {
		vs.logger.Printf("Before Tick:CS = %d, Primary =%s, Backup = %s, viewNo = %d", oldState, oldView.Primary, oldView.Backup, oldView.Viewnum)
		vs.logger.Printf("After  Tick:CS = %d, Primary =%s, Backup = %s, viewNo = %d", newState, newView.Primary, newView.Backup, newView.Viewnum)
	}
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	vs.logger.Printf("Ping:CS = %s, SVR = %s, viewNo = %d", vs.convertStateToString(), args.Me, args.Viewnum)

	reply.View = vs.currentView

	// Updating the server last ping time.
	vs.serverLastHeartBeat[args.Me] = time.Now()

	switch vs.currentState {
	case NO_PRIMARY_CURRENTLY:
		// Currently No Primary or Backup server. This scenarios arises -
		// a. When system is restarting first time.
		// b. Complete serving server is crashed and now recovering after restert.
		vs.currentView.Primary = args.Me
		vs.currentView.Viewnum = args.Viewnum + 1
		reply.View = vs.currentView
		vs.currentState = WAITING_FOR_PRIMARY_ACK
	case WAITING_FOR_PRIMARY_ACK:
		// Primary is not acknowledged. Hence backup server will not be added in view.
		if vs.currentView.Primary == args.Me && vs.currentView.Viewnum == args.Viewnum {
			// Primary is acknowledge.
			if vs.unacknowledgedView.Primary != "" {
				vs.currentView = vs.unacknowledgedView
				vs.unacknowledgedView = View{}
				vs.currentState = WAITING_FOR_PRIMARY_ACK

			} else {
				reply.View = vs.currentView
				vs.currentState = PRIMARY_ACK_CONFIRMED
			}

		} else {
			// Primary is not acknowledged. And received new server which might work as Backup.
			reply.View = vs.currentView
			if vs.currentView.Backup == "" && vs.currentView.Primary != args.Me {
				vs.unacknowledgedView = vs.currentView
				vs.unacknowledgedView.Viewnum = vs.currentView.Viewnum + 1
				vs.unacknowledgedView.Backup = args.Me
			}
		}
	case PRIMARY_ACK_CONFIRMED:
		//Handling dead server restarted when serving request.
		if args.Viewnum == 0 {
			// Server crashed and restarted quickly
			if args.Me == vs.currentView.Primary {
				//Primary crashed And restarted.
				vs.currentView.Primary = vs.currentView.Backup
				vs.currentView.Backup = args.Me
				vs.currentView.Viewnum += 1
				vs.currentState = WAITING_FOR_PRIMARY_ACK
			} else if args.Me == vs.currentView.Backup || vs.currentView.Backup == "" {
				// Backup server crashed or New Server just started.
				vs.currentView.Backup = vs.getIdleServer()
				vs.currentView.Viewnum += 1
				vs.currentState = WAITING_FOR_PRIMARY_ACK
			}
			// Make Backup as Primary and If there is idle server, make it as backup and WAIT FOR CONFIRMATION OF PRIMARY
		}

		if vs.currentView.Primary != args.Me && vs.currentView.Backup == "" {
			// Backup server found. Hence generating new view with Backup.
			vs.currentView.Backup = args.Me
			vs.currentView.Viewnum = vs.currentView.Viewnum + 1
			vs.currentState = WAITING_FOR_PRIMARY_ACK
		}

	}
	reply.View = vs.currentView
	vs.logger.Printf("PingRes:CS = %s, SVR = %s, Primary =%s, Backup = %s, viewNo = %d", vs.convertStateToString(), args.Me, vs.currentView.Primary, vs.currentView.Backup, vs.currentView.Viewnum)
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	reply.View = vs.currentView
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	// If the ping of server is missed consecutively 5 times, It will be assumed that server is dead
	vs.logger.Println("Tick Expired....")
	currentTime := time.Now()
	oldState := vs.currentState
	oldView := vs.currentView
	idleServer := ""
	backupServer := ""
	primaryServer := ""

	vs.mu.Lock()

	// Generate Idle server and find out whether Primary or Backup server is dead.
	for server := range vs.serverLastHeartBeat {
		lastPing := vs.serverLastHeartBeat[server]
		duration := currentTime.Sub(lastPing)
		if duration < (DeadPings * PingInterval) {
			if vs.currentView.Primary != server && vs.currentView.Backup != server {
				idleServer = server
			} else if vs.currentView.Primary == server {
				primaryServer = server
			} else if vs.currentView.Backup == server {
				backupServer = server
			} else {
				vs.logger.Printf("Server %s is not responding", server)
			}
		}
	}

	// If primaryServer or backupServer is "" . It means it is dead.
	// We should not change the views if PRIMARY has not acknowledged.
	if vs.currentState != WAITING_FOR_PRIMARY_ACK {
		if primaryServer != vs.currentView.Primary {
			// Primary server has responed in Dead Ping Intervals. view needs to be changed.
			// If there is Backup, Backup will become Primary.
			// If there is idle server, IdleServer will become Backup.
			vs.currentView.Primary = vs.currentView.Backup
			vs.currentView.Backup = idleServer
			vs.currentState = WAITING_FOR_PRIMARY_ACK
			vs.currentView.Viewnum += 1
			vs.logger.Printf("Primary Server is dead. New Primary = %s, Backup = %s", vs.currentView.Primary, vs.currentView.Backup)

		} else if backupServer != vs.currentView.Backup {
			vs.currentView.Backup = idleServer
			vs.currentView.Viewnum += 1
			vs.currentState = WAITING_FOR_PRIMARY_ACK
			vs.logger.Printf("Backup Server is dead. New Primary = %s, Backup = %s", vs.currentView.Primary, vs.currentView.Backup)
		}
	}
	newState := vs.currentState
	newView := vs.currentView
	vs.logIfViewChanged(oldView, newView, oldState, newState)
	vs.mu.Unlock()

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me

	// Your vs.* initializations here.
	vs.serverLastHeartBeat = make(map[string]time.Time)
	vs.currentView = View{}
	vs.unacknowledgedView = View{}
	vs.mu = sync.Mutex{}
	vs.logger = log.New(os.Stdout, "viewServer: ", log.Lshortfile)
	if DISABLE_STDOUT_LOGING {
		var buf bytes.Buffer
		vs.logger = log.New(&buf, "viewServer: ", log.Lshortfile)
	}
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
