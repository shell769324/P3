package storageserver

import (
	//"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	isAlive       bool       // DO NOT MODIFY
	mux           sync.Mutex // DO NOT MODIFY
	listener      net.Listener
	masterStorage *rpc.Client
	listStore     map[string][]string
	stringStore   map[string]string
	hostPort      string
	servers       []storagerpc.Node
	numNodes      int
	virtualIDs 	  []uint32

	listLocks 	  map[string]sync.Mutex
	stringLocks   map[string]sync.Mutex

	stringLock sync.Mutex
	listLock sync.Mutex
	serverLock sync.Mutex
	// TODO: implement this!
}

// USED FOR TESTS, DO NOT MODIFY
func (ss *storageServer) SetAlive(alive bool) {
	ss.mux.Lock()
	ss.isAlive = alive
	ss.mux.Unlock()
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	/****************************** DO NOT MODIFY! ******************************/
	fmt.Println("our ss is called\n\n")
	ss := new(storageServer)
	ss.isAlive = true
	/****************************************************************************/
	// TODO: implement this!
	// Its master
	// storageS := storageServer{}
	// storageMethods := storagerpc.StorageServer{}
	ss.stringStore = make(map[string]string)
	ss.listStore = make(map[string][]string)
	ss.hostPort = "localhost:" + strconv.Itoa(port)
	ss.virtualIDs = virtualIDs

	if masterServerHostPort == "" {
		ss.servers = make([]storagerpc.Node, 0)
		ss.servers = append(ss.servers, storagerpc.Node{HostPort: ss.hostPort, VirtualIDs: virtualIDs})
		ss.numNodes = numNodes
	}
	fmt.Printf("Listening on %v\n", "localhost"+":"+strconv.Itoa(port))
	err := rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	rpc.HandleHTTP()

	masterListener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// TODO:
	// List of other servers for slave
	if masterServerHostPort != "" {
		node := storagerpc.Node{HostPort: ss.hostPort, VirtualIDs: virtualIDs}
		args := &storagerpc.RegisterArgs{ServerInfo: node}
		reply := &storagerpc.RegisterReply{}
		for {
			client, err := rpc.DialHTTP("tcp", masterServerHostPort)
			if err == nil {
				ss.masterStorage = client
				break
			}
			time.Sleep(1 * time.Second)
		}
		for {
			err := ss.masterStorage.Call("StorageServer.RegisterServer", args, reply)
			fmt.Println(err == nil, reply.Status)
			if err == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}
	err = http.Serve(masterListener, nil)
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (ss *storageServer) registerServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.serverLock.Lock()
	ss.servers = append(ss.servers, args.ServerInfo)
	reply.Servers = ss.servers
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}
	ss.serverLock.Unlock()
	return nil
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.serverLock.Lock()
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}
	reply.Servers = ss.servers
	ss.serverLock.Unlock()
	return nil
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	ss.stringLock.Lock()
	if val, ok := ss.stringStore[args.Key]; ok {
		reply.Value = val
		reply.Status = storagerpc.OK
		reply.Lease = storagerpc.Lease{Granted: args.WantLease,ValidSeconds: storagerpc.LeaseSeconds}

	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.stringLock.Unlock()
	return nil
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.stringLock.Lock()
	if _, ok := ss.stringStore[args.Key]; ok {
		delete(ss.stringStore, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.stringLock.Unlock()
	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	ss.listLock.Lock()
	print("Hurray")
	if list, ok := ss.listStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Lease= storagerpc.Lease{Granted: args.WantLease,ValidSeconds: storagerpc.LeaseSeconds}
		
		if len(list) > 100 {
			list = list[len(list) - 100:]
		}
		reply.Value = list
	} else {
		reply.Status = storagerpc.KeyNotFound
		reply.Value = make([]string, 0)
	}
	ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.stringLock.Lock()
	ss.stringStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	ss.stringLock.Unlock()
	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.listLock.Lock()
	defer ss.listLock.Unlock()
	reply.Status = storagerpc.OK
	if list, ok := ss.listStore[args.Key]; ok {
		for _, val := range list {
			if val == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		ss.listStore[args.Key] = append(list, args.Value)
		return nil
	}
	ss.listStore[args.Key] = make([]string, 0)
	ss.listStore[args.Key] = append(ss.listStore[args.Key], args.Value)
	return nil
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.listLock.Lock()
	defer ss.listLock.Unlock()
	//fmt.Println(ss.listStore[args.Key])
	if list, ok := ss.listStore[args.Key]; ok {
		for i, val := range list {
			if val == args.Value {
				ss.listStore[args.Key] = append(list[:i], list[i+1:]...)
				//fmt.Printf("List after delete: %v", ss.listStore[args.Key])
				reply.Status = storagerpc.OK
				return nil
			}
		}
		reply.Status = storagerpc.ItemNotFound
		return nil

	}
	reply.Status = storagerpc.KeyNotFound
	return nil
}
