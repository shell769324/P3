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

type leaserStruct struct {
	hostPort  string
	client    *rpc.Client
	leaseTime time.Time
}

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
	virtualIDs    []uint32

	maplistLock   sync.Mutex
	mapstringLock sync.Mutex
	listLocks     map[string]sync.Mutex //locks for individual lists
	stringLocks   map[string]sync.Mutex // locks for individual strings

	listLeaser   map[string][]leaserStruct
	stringLeaser map[string][]leaserStruct

	ongoingRevoke map[string]bool
	revokeLock    sync.Mutex

	// stringLock sync.Mutex
	// listLock   sync.Mutex
	serverLock sync.Mutex

	libClients    map[string]*rpc.Client
	libClientLock sync.Mutex

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
	fmt.Println("Storage server is called")
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
	ss.listLeaser = make(map[string][]leaserStruct)
	ss.stringLeaser = make(map[string][]leaserStruct)
	ss.listLocks = make(map[string]sync.Mutex)
	ss.stringLocks = make(map[string]sync.Mutex)
	ss.libClients = make(map[string]*rpc.Client)
	ss.ongoingRevoke = make(map[string]bool)

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
	go http.Serve(masterListener, nil)
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
	// ss.stringLock.Lock()
	// Get/GetList is where the client for ask for lease for the first time
	//fmt.Println("Get started", args)
	//defer fmt.Println("Get done")
	// ss.revokeLock.Lock()
	if ss.ongoingRevoke[args.Key] {
		fmt.Println("Ongoing revoke. Not getting lock")
		args.WantLease = false
	}
	// ss.revokeLock.Unlock()
	ss.mapstringLock.Lock()
	if _, ok := ss.stringLocks[args.Key]; !ok {
		var newlock sync.Mutex
		ss.stringLocks[args.Key] = newlock
	}
	keyLock := ss.stringLocks[args.Key]
	ss.mapstringLock.Unlock()
	keyLock.Lock()
	defer keyLock.Unlock()

	if args.WantLease {
		if _, ok := ss.stringLeaser[args.Key]; !ok {
			ss.stringLeaser[args.Key] = make([]leaserStruct, 0)
		}
	}

	if val, ok := ss.stringStore[args.Key]; ok {
		reply.Value = val
		reply.Status = storagerpc.OK
		if args.WantLease {
			found := false
			i := 0
			// Connect to libstore and cache it
			ss.libClientLock.Lock()
			if _, ok := ss.libClients[args.HostPort]; !ok {
				fmt.Println("Connecting to libstore and caching it")

				client, err := rpc.DialHTTP("tcp", args.HostPort)
				ss.libClients[args.HostPort] = client
				if err != nil {
					fmt.Println("Connecting to libstore failed")
				}
			}
			// libClientHandle := ss.libClients[args.HostPort]
			ss.libClientLock.Unlock()

			for i, _ = range ss.stringLeaser[args.Key] {
				if ss.stringLeaser[args.Key][i].hostPort == args.HostPort {
					found = true
					break
				}
			}
			if !found {
				//establish connection, cache it
				newLeaserStruct := leaserStruct{hostPort: args.HostPort, leaseTime: time.Now()}
				// newLeaserStruct.client, _ = rpc.DialHTTP("tcp", args.HostPort)
				// fmt.Println("Hostport: ", args.HostPort)
				// // set the leasetime
				ss.stringLeaser[args.Key] = append(ss.stringLeaser[args.Key], newLeaserStruct)
			} else {
				timeNow := time.Now()
				leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
				if timeNow.Before(ss.stringLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
					//lease is valid, dont grant lease
					fmt.Println("Old lease is still valid")

					args.WantLease = false
				} else {
					ss.stringLeaser[args.Key][i].leaseTime = time.Now()
				}
			}

		}

		reply.Lease = storagerpc.Lease{Granted: args.WantLease, ValidSeconds: storagerpc.LeaseSeconds}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	// ss.stringLock.Unlock()
	//fmt.Println(reply)

	return nil
}
func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("put started:", args)
	//defer fmt.Println("put completed")
	ss.mapstringLock.Lock()
	if _, ok := ss.stringLocks[args.Key]; !ok {
		var newlock sync.Mutex
		ss.stringLocks[args.Key] = newlock
	}
	keyLock := ss.stringLocks[args.Key]
	ss.mapstringLock.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	//Revoke leases
	if _, ok := ss.stringLeaser[args.Key]; !ok {
		ss.stringLeaser[args.Key] = make([]leaserStruct, 0)
	}

	timeNow := time.Now()
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = true
	ss.revokeLock.Unlock()

	leaseHosts := ss.stringLeaser[args.Key]
	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	timeout := time.After(time.Duration(leaseValidTime) * time.Second)

	for i, _ := range leaseHosts {
		fmt.Println("Iter:", i)
		//Check if lease is valid and call revoke list
		fmt.Println("Lease valid time:", leaseValidTime)
		fmt.Println("Timebefore: ", ss.stringLeaser[args.Key][i].leaseTime)

		timeCheck := ss.stringLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)
		fmt.Println("Timecheck: ", timeCheck)
		fmt.Println("Timenow: ", timeNow)

		if timeNow.After(timeCheck) {
			// lease is invalid, do nothing with that server
			fmt.Println("Lease is invalid, continuing pro")
			go func() {
				doneChan <- true
			}()
			continue
		} else {
			fmt.Println("Calling revoke lease")
			reply := &storagerpc.RevokeLeaseReply{}
			// ss.libClientLock.Lock()
			libClientHandle := ss.libClients[ss.stringLeaser[args.Key][i].hostPort]
			// ss.libClientLock.Unlock()
			fmt.Println("Calling the client hangle:", libClientHandle)
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				doneChan <- true
				ss.stringLeaser[args.Key][i].leaseTime = time.Time{}

			}()
			// print(reply.Status)
			// print(err)
		}

	}
	if len(leaseHosts) > 0 {
		replies := 0
		breakLoop := false
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leaseHosts) {
					breakLoop = true
				}
			case <-timeout:
				fmt.Println("Didnt receive all revokes, but lease must have expired")
				for i, _ := range ss.stringLeaser[args.Key] {
					ss.stringLeaser[args.Key][i].leaseTime = time.Time{}
				}

				breakLoop = true
			}
		}
	}

	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = false
	ss.revokeLock.Unlock()

	// ss.stringLock.Lock()
	ss.stringStore[args.Key] = args.Value
	reply.Status = storagerpc.OK
	// ss.stringLock.Unlock()
	return nil
}
func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	ss.mapstringLock.Lock()
	if _, ok := ss.stringLocks[args.Key]; !ok {
		var newlock sync.Mutex
		ss.stringLocks[args.Key] = newlock
	}
	keyLock := ss.stringLocks[args.Key]
	ss.mapstringLock.Unlock()

	keyLock.Lock()
	defer keyLock.Unlock()

	//Revoke leases
	if _, ok := ss.stringLeaser[args.Key]; !ok {
		ss.stringLeaser[args.Key] = make([]leaserStruct, 0)
	}

	timeNow := time.Now()
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = true
	ss.revokeLock.Unlock()

	leaseHosts := ss.stringLeaser[args.Key]
	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	timeout := time.After(time.Duration(leaseValidTime) * time.Second)

	for i, _ := range ss.stringLeaser[args.Key] {

		//Check if lease is valid and call revoke list
		// leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
		if timeNow.After(ss.stringLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
			// lease is invalid, do nothing with that server
			go func() {
				doneChan <- true
			}()

		} else {
			//print("Calling revoke lease")
			reply := &storagerpc.RevokeLeaseReply{}
			// ss.libClientLock.Lock()
			libClientHandle := ss.libClients[ss.stringLeaser[args.Key][i].hostPort]
			// ss.libClientLock.Unlock()

			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				doneChan <- true
				ss.stringLeaser[args.Key][i].leaseTime = time.Time{}

			}()
		}

	}
	if len(leaseHosts) > 0 {
		replies := 0
		breakLoop := false
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leaseHosts) {
					breakLoop = true
				}
			case <-timeout:
				for i, _ := range ss.stringLeaser[args.Key] {
					ss.stringLeaser[args.Key][i].leaseTime = time.Time{}
				}

				fmt.Println("Didnt receive all revokes, but lease must have expired")
				breakLoop = true
			}
		}
	}
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = false
	ss.revokeLock.Unlock()

	// ss.stringLock.Lock()
	if _, ok := ss.stringStore[args.Key]; ok {
		delete(ss.stringStore, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	// ss.stringLock.Unlock()
	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// ss.listLock.Lock()

	if ss.ongoingRevoke[args.Key] {
		fmt.Println("Ongoing revoke. Not getting lock")
		args.WantLease = false
	}
	if args.WantLease {
		ss.maplistLock.Lock()
		if _, ok := ss.listLocks[args.Key]; !ok {
			var newlock sync.Mutex
			ss.listLocks[args.Key] = newlock
		}
		keyLock := ss.listLocks[args.Key]
		ss.maplistLock.Unlock()
		keyLock.Lock()
		defer keyLock.Unlock()

		if _, ok := ss.listLeaser[args.Key]; !ok {
			ss.listLeaser[args.Key] = make([]leaserStruct, 0)
		}
	}

	if list, ok := ss.listStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		if args.WantLease {
			found := false
			i := 0

			// Connect to libstore and cache it
			ss.libClientLock.Lock()
			if _, ok := ss.libClients[args.HostPort]; !ok {
				fmt.Println("Caching the connection during getlist")
				client, err := rpc.DialHTTP("tcp", args.HostPort)
				ss.libClients[args.HostPort] = client
				if err != nil {
					fmt.Println("Coudnt cache connection: ", err)
				}
			}
			// libClientHandle := ss.libClients[args.HostPort]
			ss.libClientLock.Unlock()

			for i, _ = range ss.listLeaser[args.Key] {
				if ss.listLeaser[args.Key][i].hostPort == args.HostPort {
					found = true
					break
				}
			}
			if !found {
				//establish connection, cache it
				newLeaserStruct := leaserStruct{hostPort: args.HostPort}
				// newLeaserStruct.client, _ = rpc.DialHTTP("tcp", args.HostPort)
				// set the leasetime
				newLeaserStruct.leaseTime = time.Now()
				ss.listLeaser[args.Key] = append(ss.listLeaser[args.Key], newLeaserStruct)
			} else {
				timeNow := time.Now()
				leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
				if timeNow.Before(ss.listLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
					//lease is valid, dont grant lease
					fmt.Println("Old lease is still valid")

					args.WantLease = false
				} else {
					ss.listLeaser[args.Key][i].leaseTime = time.Now()
				}

				// ss.listLeaser[args.Key][i].leaseTime = time.Now()
			}
		}
		//fmt.Println("Granting lease: ", args.WantLease)
		reply.Lease = storagerpc.Lease{Granted: args.WantLease, ValidSeconds: storagerpc.LeaseSeconds}

		if len(list) > 100 {
			list = list[len(list)-100:]
		}
		reply.Value = list
	} else {
		//fmt.Println("key not found")
		reply.Status = storagerpc.KeyNotFound
		reply.Value = make([]string, 0)
	}
	// ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	ss.maplistLock.Lock()
	if _, ok := ss.listLocks[args.Key]; !ok {
		var newlock sync.Mutex
		ss.listLocks[args.Key] = newlock
	}
	keyLock := ss.listLocks[args.Key]
	ss.maplistLock.Unlock()
	keyLock.Lock()
	defer keyLock.Unlock()

	//Revoke leases
	if _, ok := ss.listLeaser[args.Key]; !ok {
		ss.listLeaser[args.Key] = make([]leaserStruct, 0)
	}

	timeNow := time.Now()
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = true
	ss.revokeLock.Unlock()
	leaseHosts := ss.listLeaser[args.Key]
	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

	for i, _ := range leaseHosts {
		//fmt.Println("Iter: ", i)
		//Check if lease is valid and call revoke list
		//fmt.Println(ss.listLeaser[args.Key][i].leaseTime, timeNow)
		if timeNow.After(ss.listLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
			// lease is invalid, do nothing with that server
			go func() {
				doneChan <- true
			}()
		} else {
			//print("Calling revoke lease")
			reply := &storagerpc.RevokeLeaseReply{}
			ss.libClientLock.Lock()
			libClientHandle := ss.libClients[ss.listLeaser[args.Key][i].hostPort]
			ss.libClientLock.Unlock()
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				doneChan <- true
				ss.listLeaser[args.Key][i].leaseTime = time.Time{}
			}()
		}

	}

	replies := 0
	timeout := time.After(time.Duration(leaseValidTime)*time.Second + 1)
	breakLoop := false
	if len(leaseHosts) > 0 {
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leaseHosts) {
					breakLoop = true
				}
			case <-timeout:
				breakLoop = true
				for i, _ := range leaseHosts {
					ss.listLeaser[args.Key][i].leaseTime = time.Time{}
				}
			}
		}
	}

	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = false
	ss.revokeLock.Unlock()
	//fmt.Println("Appending value to list")
	// ss.listLock.Lock()
	// defer ss.listLock.Unlock()
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
	ss.maplistLock.Lock()
	if _, ok := ss.listLocks[args.Key]; !ok {
		var newlock sync.Mutex
		ss.listLocks[args.Key] = newlock
	}
	keyLock := ss.listLocks[args.Key]
	ss.maplistLock.Unlock()
	keyLock.Lock()
	defer keyLock.Unlock()

	//Revoke leases
	if _, ok := ss.listLeaser[args.Key]; !ok {
		ss.listLeaser[args.Key] = make([]leaserStruct, 0)
	}

	timeNow := time.Now()
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = true
	ss.revokeLock.Unlock()

	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	leaseHosts := ss.listLeaser[args.Key]
	doneChan := make(chan bool)

	for i, _ := range leaseHosts {
		//Check if lease is valid and call revoke list

		// leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
		if timeNow.After(ss.listLeaser[args.Key][i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
			// lease is invalid, do nothing with that server
			go func() {
				doneChan <- true
			}()

		} else {
			reply := &storagerpc.RevokeLeaseReply{}
			libClientHandle := ss.libClients[ss.listLeaser[args.Key][i].hostPort]

			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				// fmt.Println("RPC CALL error : ", err)
				doneChan <- true
				ss.listLeaser[args.Key][i].leaseTime = time.Time{}

			}()
		}

	}

	replies := 0
	timeout := time.After(time.Duration(leaseValidTime) * time.Second)
	breakLoop := false
	if len(leaseHosts) > 0 {
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leaseHosts) {
					breakLoop = true
				}
			case <-timeout:
				breakLoop = true
				for i, _ := range leaseHosts {
					ss.listLeaser[args.Key][i].leaseTime = time.Time{}
				}

			}
		}
	}
	ss.revokeLock.Lock()
	ss.ongoingRevoke[args.Key] = false
	ss.revokeLock.Unlock()

	// ss.listLock.Lock()
	// defer ss.listLock.Unlock()
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
