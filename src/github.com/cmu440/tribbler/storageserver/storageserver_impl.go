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
	isAlive     bool       // DO NOT MODIFY
	mux         sync.Mutex // DO NOT MODIFY
	listener    net.Listener
	masterStorage *rpc.Client
	listStore   map[string][]string
	stringStore map[string]string
	hostPort string
	servers []storagerpc.Node
	numNodes int
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
	ss := new(storageServer)
	ss.isAlive = true
	/****************************************************************************/
	// TODO: implement this!
	// Its master
	// storageS := storageServer{}
	// storageMethods := storagerpc.StorageServer{}
	ss.stringStore = make(map[string]string)
	ss.listStore = make(map[string][]string)
	ss.hostPort = "localhost:"+strconv.Itoa(port)

	if masterServerHostPort == "" {
		ss.servers = make([]storagerpc.Node, 0)
		ss.servers = append(ss.servers,  storagerpc.Node{HostPort:ss.hostPort})
		ss.numNodes = numNodes
	}
	fmt.Printf("Listening on %v", "localhost"+":"+strconv.Itoa(port))
	err := rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	rpc.HandleHTTP()

	masterListener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))
	ss.listener = masterListener
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// TODO: virtual ids
	// List of other servers for slave
	if masterServerHostPort != "" {
		node := storagerpc.Node{HostPort: ss.hostPort}
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
			err := ss.masterStorage.Call("StorageServer.registerServer", args, reply)
			if err == nil && reply.Status == storagerpc.OK {
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
	//fmt.Println("total: ", ss.numNodes, "\ncurrent: ", len(ss.servers))
	ss.servers = append(ss.servers, storagerpc.Node{HostPort:args.ServerInfo.HostPort})
	reply.Servers = ss.servers
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}
	return nil
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.NotReady
	}
	reply.Servers = ss.servers
	return nil
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if val, ok := ss.stringStore[args.Key]; ok {
		reply.Value = val
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if _, ok := ss.stringStore[args.Key]; ok {
		delete(ss.stringStore, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if list, ok := ss.listStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Value = list
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if _, ok := ss.stringStore[args.Key]; ok {
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.stringStore[args.Key] = args.Value
	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if list, ok := ss.listStore[args.Key]; ok {
		for _, val := range list {
			if val == args.Value {
				reply.Status = storagerpc.ItemExists
				return nil
			}
		}
		ss.listStore[args.Key] = append(list, args.Value)
		reply.Status = storagerpc.OK
		return nil
	}

	ss.listStore[args.Key] = make([]string, 0)
	ss.listStore[args.Key] = append(ss.listStore[args.Key], args.Value)
	reply.Status = storagerpc.KeyNotFound
	return nil
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println(ss.listStore[args.Key])
	if list, ok := ss.listStore[args.Key]; ok {
		for i, val := range list {
			if val == args.Value {
				ss.listStore[args.Key] = append(list[:i], list[i+1:]...)
				fmt.Printf("List after delete: %v", ss.listStore[args.Key])
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
