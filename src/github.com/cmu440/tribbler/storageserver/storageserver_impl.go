package storageserver

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	isAlive     bool       // DO NOT MODIFY
	mux         sync.Mutex // DO NOT MODIFY
	listener    net.Listener
	listStore   map[string][]string
	stringStore map[string]string
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

	if masterServerHostPort == "" {
		fmt.Printf("Listening on %v", "localhost"+":"+strconv.Itoa(port))
		err := rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
		if err != nil {
			print("Error")
		}
		rpc.HandleHTTP()

		masterListener, err := net.Listen("tcp", "localhost"+":"+strconv.Itoa(port))
		ss.listener = masterListener
		if err != nil {
			fmt.Println("Error listening:", err.Error())
			os.Exit(1)
		}
		err = http.Serve(masterListener, nil)
		if err != nil {
			return nil, err
		}

	}

	return ss, nil

	return nil, errors.New("not implemented")
}

func (ss *storageServer) registerServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) getServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	if val, ok := ss.stringStore[args.Key]; ok {
		reply.Value = val
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
		return errors.New("Key does not exist in map")
	}
	return nil
}

func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	if _, ok := ss.stringStore[args.Key]; ok {
		delete(ss.stringStore, args.Key)
		reply.Status = storagerpc.OK
		return nil
	}

	reply.Status = storagerpc.KeyNotFound
	return errors.New("Key does not exist in map")

}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	if list, ok := ss.listStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		reply.Value = list
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
	return errors.New("Key does not exist in list storage")

}

func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.stringStore[args.Key] = args.Value
	reply.Status = storagerpc.OK

	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	if list, ok := ss.listStore[args.Key]; ok {
		for _, val := range list {
			if val == args.Value {

				reply.Status = storagerpc.ItemExists
				return errors.New("Key already exists in map")
			}
		}

		list = append(list, args.Value)
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
		return errors.New("Item not found")

	}
	reply.Status = storagerpc.KeyNotFound
	return errors.New("Key does not exist in list storage")
}
