package libstore

import (
	"errors"

	//"log"
	"fmt"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type cacheString struct {
	value string
	leaseTime time.Time
	window []time.Time
	validSeconds int
}

type cacheList struct {
	value []string
	leaseTime time.Time
	window []time.Time
	validSeconds int

}

type libstore struct {
	// TODO: implement this!
	myPort        string
	virtualIDs    []uint32
	hostPorts		  map[uint32]string
	conns		  map[string]*rpc.Client

	listStore     map[string]*cacheList
	stringStore   map[string]*cacheString

	listLock 	  sync.Mutex
	stringLock	  sync.Mutex	
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	libst := new(libstore)
	libst.myPort = myHostPort
	print(masterServerHostPort)
	client, err := rpc.DialHTTP("tcp", masterServerHostPort)
	libst.conns = make(map[string]*rpc.Client)
	libst.conns[masterServerHostPort] = client
	libst.listStore = make(map[string]*cacheList)
	libst.stringStore = make(map[string]*cacheString)
	print("My lease mode is:",mode)
	if err != nil {
		return nil, err
	}
	reply := &storagerpc.GetServersReply{}
	for i := 0; i < 5; i++ {
		err := client.Call("StorageServer.GetServers", &storagerpc.GetServersArgs{}, reply)
		if err == nil && reply.Status == storagerpc.OK {
			break
		}
		time.Sleep(1 * time.Second)
	}
	libst.hostPorts = make(map[uint32]string)
	libst.virtualIDs = make([]uint32, 0)

	for _, node := range(reply.Servers) {
		for _, vID := range(node.VirtualIDs) {
			libst.hostPorts[vID] = node.HostPort
		}
		libst.virtualIDs = append(libst.virtualIDs, node.VirtualIDs...)
	}
	sort.Slice(libst.virtualIDs, func(i, j int) bool { return libst.virtualIDs[i] > libst.virtualIDs[j]})
	return libst, nil
}

func (ls *libstore) getVirtualID(key string) int {
	hash := StoreHash(key)
	//fmt.Println("Hash: ", hash)
	i := 0
	for i < len(ls.virtualIDs) && hash > ls.virtualIDs[i] {
		i++
	}
	return i % len(ls.virtualIDs)
}

func (ls *libstore) getClient(id int) (*rpc.Client, int) {
	hostPort := ls.hostPorts[ls.virtualIDs[id]]
	var res *rpc.Client
	for {
		if client, ok := ls.conns[hostPort]; ok {
			return client, id
		}
		client, err := rpc.DialHTTP("tcp", hostPort)
		if err == nil {
			ls.conns[hostPort] = client
			return client, id
		}
		id = (id + 1) % len(ls.virtualIDs)
	}
	return res, id
}

func (ls *libstore) Get(key string) (string, error) {
	ls.stringLock.Lock()
	defer ls.stringLock.Unlock()
	fmt.Println("------------------------------------------")
	replyArgs := &storagerpc.GetReply{}
	requestArgs := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}
	if _, ok := ls.stringStore[key]; !ok {
		fmt.Println("Creating new cache string")
		ls.stringStore[key] = &cacheString{window: make([]time.Time, 0), leaseTime: time.Time{}}
	}
	timeNow := time.Now()
	ls.stringStore[key].window = append(ls.stringStore[key].window, timeNow)
	if timeNow.Before(ls.stringStore[key].leaseTime.Add(time.Duration(ls.stringStore[key].validSeconds) * time.Second)) {
		fmt.Printf("Lease is still valid. Time now:%v , valid seconds: %v, leasetime :%v",timeNow,ls.stringStore[key].leaseTime,ls.stringStore[key].validSeconds)
		return ls.stringStore[key].value, nil
	}
	for len(ls.stringStore[key].window) > 0 && 
		timeNow.After(ls.stringStore[key].window[0].Add(time.Duration(storagerpc.QueryCacheSeconds) * time.Second)){
		ls.stringStore[key].window=ls.stringStore[key].window[1:]
	}
	if(len(ls.stringStore[key].window)>=storagerpc.QueryCacheThresh) {
		fmt.Println("Lease has expired, want new lease")
		//Request lease
		requestArgs.WantLease= true
	}

	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error" {
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.Get", requestArgs, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}

	if replyArgs.Status == storagerpc.OK && requestArgs.WantLease && replyArgs.Lease.Granted {
		fmt.Println("Lease granted, update value: ", replyArgs.Value)
		ls.stringStore[key].validSeconds = replyArgs.Lease.ValidSeconds
		ls.stringStore[key].leaseTime = time.Now()
		ls.stringStore[key].value = replyArgs.Value
		fmt.Println("CacheString updated to", ls.stringStore[key])
	}

	if replyArgs.Status != storagerpc.OK {
		return "", errors.New("Get error")
	}
	return replyArgs.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	replyArgs := &storagerpc.PutReply{}
	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error" {
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.Put", &storagerpc.PutArgs{Key: key, Value: value}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Put error")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	replyArgs := &storagerpc.DeleteReply{}
	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error"{
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.Delete", &storagerpc.DeleteArgs{Key: key}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Delete error")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	ls.stringLock.Lock()
	defer ls.stringLock.Unlock()
	fmt.Println("------------------------------------------")
	replyArgs := &storagerpc.GetListReply{}
	requestArgs := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}

	if _, ok := ls.listStore[key]; !ok {
		fmt.Println("Creating new cache string")
		ls.listStore[key] = &cacheList{window: make([]time.Time, 0), leaseTime: time.Time{}}
	}
	timeNow := time.Now()
	ls.listStore[key].window = append(ls.listStore[key].window, timeNow)
	if timeNow.Before(ls.listStore[key].leaseTime.Add(time.Duration(ls.listStore[key].validSeconds) * time.Second)) {
		fmt.Printf("Lease is still valid. Time now:%v , valid seconds: %v, leasetime :%v",timeNow,ls.listStore[key].leaseTime,ls.listStore[key].validSeconds)
		return ls.listStore[key].value, nil
	}
	for len(ls.listStore[key].window) > 0 && 
		timeNow.After(ls.listStore[key].window[0].Add(time.Duration(storagerpc.QueryCacheSeconds) * time.Second)){
		ls.listStore[key].window=ls.listStore[key].window[1:]
	}
	if(len(ls.listStore[key].window)>=storagerpc.QueryCacheThresh) {
		fmt.Printf("Time now:%v , leasetime: %v, valid second :%v",timeNow,ls.listStore[key].leaseTime,ls.listStore[key].validSeconds)

		fmt.Println("want new lease")
		//Request lease
		requestArgs.WantLease= true
	}

	id := ls.getVirtualID(key)
	err := errors.New("")

	for err != nil && err.Error() != "error"{
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.GetList", &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	fmt.Println(replyArgs.Status,requestArgs.WantLease,replyArgs.Lease.Granted)
	if replyArgs.Status == storagerpc.OK && requestArgs.WantLease && replyArgs.Lease.Granted {
		fmt.Println("Lease granted, update value: ", replyArgs.Value)
		ls.listStore[key].validSeconds = replyArgs.Lease.ValidSeconds
		ls.listStore[key].leaseTime = time.Now()
		ls.listStore[key].value = replyArgs.Value
		fmt.Println("CacheString updated to", ls.listStore[key])
	}
	if replyArgs.Status != storagerpc.OK {
		err = errors.New("Get list error")
	}
	return replyArgs.Value, err
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	replyArgs := &storagerpc.PutReply{}
	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error"{
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.RemoveFromList", &storagerpc.PutArgs{Key: key, Value: removeItem}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Remove from list error")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	replyArgs := &storagerpc.PutReply{}
	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error" {
		client, id := ls.getClient(id)
		err = client.Call("StorageServer.AppendToList", &storagerpc.PutArgs{Key: key, Value: newItem}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Append to list error")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
