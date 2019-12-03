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

type stringInfo struct {
	value      string
	isRevoking bool
	leasers    []leaserStruct
}

type listInfo struct {
	value      []string
	isRevoking bool
	leasers    []leaserStruct
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

	// maplistLock   sync.Mutex
	// mapstringLock sync.Mutex
	// listLocks     map[string]sync.Mutex //locks for individual lists
	// stringLocks   map[string]sync.Mutex // locks for individual strings

	// listLeaser   map[string][]leaserStruct
	// stringLeaser map[string][]leaserStruct

	// ongoingRevoke map[string]bool
	// revokeLock    sync.Mutex

	stringMap     map[string]*stringInfo
	stringMapLock sync.Mutex

	listMap     map[string]*listInfo
	listMapLock sync.Mutex

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
	// ss.stringStore = make(map[string]string)
	// ss.listStore = make(map[string][]string)
	ss.hostPort = "localhost:" + strconv.Itoa(port)
	ss.stringMap = make(map[string]*stringInfo)
	ss.listMap = make(map[string]*listInfo)

	ss.virtualIDs = virtualIDs
	// ss.listLeaser = make(map[string][]leaserStruct)
	// ss.stringLeaser = make(map[string][]leaserStruct)
	// ss.listLocks = make(map[string]sync.Mutex)
	// ss.stringLocks = make(map[string]sync.Mutex)
	ss.libClients = make(map[string]*rpc.Client)
	// ss.ongoingRevoke = make(map[string]bool)

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
	ss.stringMapLock.Lock()
	defer ss.stringMapLock.Unlock()
	if _, ok := ss.stringMap[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	if ss.stringMap[args.Key].isRevoking {
		args.WantLease = false
	}
	// if ss.ongoingRevoke[args.Key] {
	// 	fmt.Println("Ongoing revoke. Not getting lock")
	// 	args.WantLease = false
	// }
	//	ss.revokeLock.Unlock()
	// ss.revokeLock.Unlock()

	reply.Value = ss.stringMap[args.Key].value
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
		for i, _ = range ss.stringMap[args.Key].leasers {
			if ss.stringMap[args.Key].leasers[i].hostPort == args.HostPort {
				found = true
				break
			}
		}
		if !found {
			//establish connection, cache it
			newLeaserStruct := leaserStruct{hostPort: args.HostPort, leaseTime: time.Now()}
			ss.stringMap[args.Key].leasers = append(ss.stringMap[args.Key].leasers, newLeaserStruct)
		} else {
			timeNow := time.Now()
			leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
			if timeNow.Before(ss.stringMap[args.Key].leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
				//lease is valid, dont grant lease CHECK
				fmt.Println("Old lease is still valid")
				args.WantLease = false
			} else {
				ss.stringMap[args.Key].leasers[i].leaseTime = time.Now()
			}
		}
	}
	reply.Lease = storagerpc.Lease{Granted: args.WantLease, ValidSeconds: storagerpc.LeaseSeconds}
	// ss.stringLock.Unlock()
	//fmt.Println(reply)

	return nil
}
func (ss *storageServer) put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	//fmt.Println("put started:", args)
	//defer fmt.Println("put completed")
	reply.Status = storagerpc.OK

	ss.stringMapLock.Lock()
	if _, ok := ss.stringMap[args.Key]; !ok {
		ss.stringMap[args.Key] = &stringInfo{value: args.Value, isRevoking: false, leasers: make([]leaserStruct, 0)}
		ss.stringMapLock.Unlock()
		return nil
	}

	leasers := ss.stringMap[args.Key].leasers
	ss.stringMap[args.Key].isRevoking = true
	ss.stringMapLock.Unlock()

	//Revoke leases
	timeNow := time.Now()

	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	timeout := time.After(time.Duration(leaseValidTime) * time.Second)

	for i, _ := range leasers {
		fmt.Println("Iter:", i)
		//Check if lease is valid and call revoke list
		// fmt.Println("Lease valid time:", leaseValidTime)
		// fmt.Println("Timebefore: ", ss.stringLeaser[args.Key][i].leaseTime)

		timeCheck := leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)
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
			ss.libClientLock.Lock()
			libClientHandle := ss.libClients[leasers[i].hostPort]
			ss.libClientLock.Unlock()
			fmt.Println("Calling the client hangle:", libClientHandle)
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				doneChan <- true
				leasers[i].leaseTime = time.Time{}
			}()
			// print(reply.Status)
			// print(err)
		}

	}
	if len(leasers) > 0 {
		replies := 0
		breakLoop := false
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leasers) {
					breakLoop = true
				}
			case <-timeout:
				fmt.Println("Didnt receive all revokes, but lease must have expired")
				for i, _ := range ss.stringMap[args.Key].leasers {
					leasers[i].leaseTime = time.Time{}
				}

				breakLoop = true
			}
		}
	}

	ss.stringMapLock.Lock()
	ss.stringMap[args.Key].isRevoking = false
	ss.stringMap[args.Key].leasers = leasers
	ss.stringMap[args.Key].value = args.Value
	ss.stringMapLock.Unlock()

	// ss.stringLock.Lock()
	// ss.stringStore[args.Key] = args.Value
	// ss.stringLock.Unlock()
	return nil
}
func (ss *storageServer) delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	reply.Status = storagerpc.OK

	ss.stringMapLock.Lock()
	if _, ok := ss.stringMap[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
		ss.stringMapLock.Unlock()
		return nil
	}

	leasers := ss.stringMap[args.Key].leasers
	ss.stringMap[args.Key].isRevoking = true
	ss.stringMapLock.Unlock()

	//Revoke leases
	timeNow := time.Now()

	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
	timeout := time.After(time.Duration(leaseValidTime) * time.Second)

	for i, _ := range leasers {
		fmt.Println("Iter:", i)
		//Check if lease is valid and call revoke list
		// fmt.Println("Lease valid time:", leaseValidTime)
		// fmt.Println("Timebefore: ", ss.stringLeaser[args.Key][i].leaseTime)

		timeCheck := leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)
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
			ss.libClientLock.Lock()
			libClientHandle := ss.libClients[leasers[i].hostPort]
			ss.libClientLock.Unlock()
			fmt.Println("Calling the client hangle:", libClientHandle)
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				doneChan <- true
				leasers[i].leaseTime = time.Time{}
			}()
			// print(reply.Status)
			// print(err)
		}

	}
	if len(leasers) > 0 {
		replies := 0
		breakLoop := false
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leasers) {
					breakLoop = true
				}
			case <-timeout:
				fmt.Println("Didnt receive all revokes, but lease must have expired")
				for i, _ := range ss.stringMap[args.Key].leasers {
					leasers[i].leaseTime = time.Time{}
				}

				breakLoop = true
			}
		}
	}

	ss.stringMapLock.Lock()
	delete(ss.stringMap, args.Key)
	ss.stringMapLock.Unlock()

	// ss.stringLock.Lock()
	// ss.stringStore[args.Key] = args.Value
	// ss.stringLock.Unlock()
	return nil
}

func (ss *storageServer) getList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	// ss.listLock.Lock()

	reply.Status = storagerpc.OK

	ss.listMapLock.Lock()
	defer ss.listMapLock.Unlock()
	if _, ok := ss.listMap[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
	if ss.listMap[args.Key].isRevoking {
		args.WantLease = false
	}

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
		ss.libClientLock.Unlock()

		for i, _ = range ss.listMap[args.Key].leasers {
			if ss.listMap[args.Key].leasers[i].hostPort == args.HostPort {
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
			ss.listMap[args.Key].leasers = append(ss.listMap[args.Key].leasers, newLeaserStruct)
		} else {
			timeNow := time.Now()
			leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
			if timeNow.Before(ss.listMap[args.Key].leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
				//lease is valid, dont grant lease! CHECK
				fmt.Println("Old lease is still valid")
				args.WantLease = false
			} else {
				ss.listMap[args.Key].leasers[i].leaseTime = time.Now()
			}

		}
	}
	//fmt.Println("Granting lease: ", args.WantLease)
	reply.Lease = storagerpc.Lease{Granted: args.WantLease, ValidSeconds: storagerpc.LeaseSeconds}
	list := ss.listMap[args.Key].value
	if len(list) > 100 {
		list = list[len(list)-100:]
	}
	reply.Value = list

	// ss.listLock.Unlock()
	return nil
}

func (ss *storageServer) appendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	reply.Status = storagerpc.OK

	ss.listMapLock.Lock()
	if _, ok := ss.listMap[args.Key]; !ok {
		ss.listMap[args.Key] = &listInfo{value: make([]string, 0), isRevoking: false, leasers: make([]leaserStruct, 0)}
		ss.listMap[args.Key].value = append(ss.listMap[args.Key].value, args.Value)
		ss.listMapLock.Unlock()
		return nil
	} else {
		// Check if the item already exists
		for _, val := range ss.listMap[args.Key].value {
			if val == args.Value {
				reply.Status = storagerpc.ItemExists
				ss.listMapLock.Unlock()
				return nil
			}
		}

	}

	leasers := ss.listMap[args.Key].leasers
	ss.listMap[args.Key].isRevoking = true
	ss.listMapLock.Unlock()

	timeNow := time.Now()
	// leaseHosts := ss.listLeaser[args.Key]
	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

	for i, _ := range leasers {
		//fmt.Println("Iter: ", i)
		//Check if lease is valid and call revoke list
		//fmt.Println(ss.listLeaser[args.Key][i].leaseTime, timeNow)
		if timeNow.After(leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
			// lease is invalid, do nothing with that server
			go func() {
				doneChan <- true
			}()
		} else {
			//print("Calling revoke lease")
			reply := &storagerpc.RevokeLeaseReply{}
			ss.libClientLock.Lock()
			libClientHandle := ss.libClients[leasers[i].hostPort]
			ss.libClientLock.Unlock()
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				leasers[i].leaseTime = time.Time{}
				doneChan <- true
			}()
		}

	}

	replies := 0
	timeout := time.After(time.Duration(leaseValidTime)*time.Second + 1)
	breakLoop := false
	if len(leasers) > 0 {
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leasers) {
					breakLoop = true
				}
			case <-timeout:
				breakLoop = true
				for i, _ := range leasers {
					leasers[i].leaseTime = time.Time{}
				}
			}
		}
	}
	ss.listMapLock.Lock()
	ss.listMap[args.Key].isRevoking = false
	ss.listMap[args.Key].leasers = leasers
	ss.listMap[args.Key].value = append(ss.listMap[args.Key].value, args.Value)
	ss.listMapLock.Unlock()

	return nil
}

func (ss *storageServer) removeFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	reply.Status = storagerpc.OK

	ss.listMapLock.Lock()
	if _, ok := ss.listMap[args.Key]; !ok {
		reply.Status = storagerpc.KeyNotFound
		// ss.listMap[args.Key] = &listInfo{value: make([]string, 0), isRevoking: false, leasers: make([]leaserStruct, 0)}
		// ss.listMap[args.Key].value = append(ss.listMap[args.Key].value, args.Value)
		ss.listMapLock.Unlock()
		return nil
	} else {
		// Check if the item is not in the list
		item_exists := false
		for _, val := range ss.listMap[args.Key].value {
			if val == args.Value {
				item_exists = true
				break
			}
		}
		if !item_exists {
			reply.Status = storagerpc.ItemNotFound
			ss.listMapLock.Unlock()
			return nil
		}

	}
	// fmt.Println("Removing an item")
	leasers := ss.listMap[args.Key].leasers
	ss.listMap[args.Key].isRevoking = true
	ss.listMapLock.Unlock()
	// fmt.Println("Removing an item")

	timeNow := time.Now()
	// leaseHosts := ss.listLeaser[args.Key]
	doneChan := make(chan bool)
	leaseValidTime := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds

	for i, _ := range leasers {
		//fmt.Println("Iter: ", i)
		//Check if lease is valid and call revoke list
		//fmt.Println(ss.listLeaser[args.Key][i].leaseTime, timeNow)
		if timeNow.After(leasers[i].leaseTime.Add(time.Duration(leaseValidTime) * time.Second)) {
			// lease is invalid, do nothing with that server
			go func() {
				doneChan <- true
			}()
		} else {
			//print("Calling revoke lease")
			reply := &storagerpc.RevokeLeaseReply{}
			ss.libClientLock.Lock()
			libClientHandle := ss.libClients[leasers[i].hostPort]
			ss.libClientLock.Unlock()
			go func() {
				libClientHandle.Call("LeaseCallbacks.RevokeLease", &storagerpc.RevokeLeaseArgs{Key: args.Key}, reply)
				leasers[i].leaseTime = time.Time{}
				doneChan <- true
			}()
		}

	}

	replies := 0
	timeout := time.After(time.Duration(leaseValidTime)*time.Second + 1)
	breakLoop := false
	if len(leasers) > 0 {
		for {
			if breakLoop {
				break
			}
			select {
			case <-doneChan:
				replies += 1
				if replies == len(leasers) {
					breakLoop = true
				}
			case <-timeout:
				breakLoop = true
				for i, _ := range leasers {
					leasers[i].leaseTime = time.Time{}
				}
			}
		}
	}
	// fmt.Println("Removing an item")

	ss.listMapLock.Lock()
	ss.listMap[args.Key].isRevoking = false
	ss.listMap[args.Key].leasers = leasers
	for i, val := range ss.listMap[args.Key].value {
		if val == args.Value {
			ss.listMap[args.Key].value = append(ss.listMap[args.Key].value[:i], ss.listMap[args.Key].value[i+1:]...)
			break
		}
	}
	ss.listMapLock.Unlock()
	// fmt.Println("Removing an item")

	return nil

}
