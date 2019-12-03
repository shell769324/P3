package libstore

import (
	"errors"

	//"log"
	"fmt"
	"net/rpc"
	"sort"
	"sync"
	"time"

	"github.com/cmu440/tribbler/rpc/librpc"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type stringBox struct {
	key      string
	value    string
	retValue chan string
}

type safeStringMap struct {
	stringMap  map[string]string
	getChan    chan stringBox
	putChan    chan stringBox
	deleteChan chan stringBox
}

type listBox struct {
	key   string
	value chan []string
}

type safeListMap struct {
	listMap    map[string][]string
	getChan    chan listBox
	appendChan chan listBox
	removeChan chan listBox
}

type cacheString struct {
	value        string
	leaseTime    time.Time
	window       []time.Time
	validSeconds time.Duration
}

type cacheList struct {
	value        []string
	leaseTime    time.Time
	window       []time.Time
	validSeconds time.Duration
}

type libstore struct {
	// TODO: implement this!
	myPort     string
	virtualIDs []uint32
	hostPorts  map[uint32]string
	conns      map[string]*rpc.Client
	mode 	  LeaseMode

	// Concurrent safe maps
	listMap   safeListMap
	stringMap safeStringMap

	listStore     map[string]*cacheList
	stringStore   map[string]*cacheString
	maplistLock   sync.Mutex
	mapstringLock sync.Mutex

}

// func (ls *libstore) handleStringMap()  {
// 	for {
// 		select {
// 			case box := <- getChan:
// 				box.retValue <- ls.stringMap.stringMap[box.key]
// 			case box := <- putChan:
// 				ls.stringMap.stringMap[box.key] = box.value
// 				box.retValue <- ""
// 			case box := <- deleteChan:
// 				delete(ls.stringMap.stringMap, box.key)
// 				box.retValue <- ""
// 		}
// 	}
// }

// func (ls *libstore) handleListMap()  {
// 	for {
// 		select {
// 			case box := <- getChan:
// 				box.retValue <- ls.listMap.listMap[box.key]
// 			case box := <- appendChan:
// 				if _, ok := ls.listMap.listMap[box.key], !ok {
// 					ls.listMap.listMap[box.key]
// 				}
// 				ls.listMap.listMap[box.key]box.value
// 				box.retValue <- make([]string, 0)
// 			case box := <- removeChan:
// 				delete(ls.listMap.listMap, box.key)
// 				box.retValue <- make([]string, 0)
// 		}
// 	}
// }

// func (ls *libstore) stringMapLookUp(key string) string {
// 	box := stringBox{key : key, retValue : make(chan string)}
// 	ls.stringMap.getChan <- box
// 	val := <- retValue
// 	return val
// }

// func (ls *libstore) stringMapPut(key, val string) {
// 	box := stringBox{key : key, value : val, retValue : make(chan string)}
// 	ls.stringMap.putChan <- box
// 	<- retValue
// }

// func (ls *libstore) stringMapDelete(key, val string) {
// 	box := stringBox{key : key, retValue : make(chan string)}
// 	ls.stringMap.deleteChan <- box
// 	<- retValue
// }

// func (ls *libstore) handleListMap()  {
// }

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
	if myHostPort == "" {
		mode = Never
	}
	libst.mode = mode
	// print(masterServerHostPort)
	client, err := rpc.DialHTTP("tcp", masterServerHostPort)
	libst.conns = make(map[string]*rpc.Client)
	libst.conns[masterServerHostPort] = client
	libst.listStore = make(map[string]*cacheList)
	libst.stringStore = make(map[string]*cacheString)

	// libst.listMap = safeListMap{listMap:make(map[string][]string, getChan: make(chan listBox),
	// 						appendChan: make(chan listBox),removeChan: make(chan listBox))}

	// libst.stringMap = safeStringMap{stringMap:make(map[string]string, getChan: make(chan stringBox),
	// 						putChan: make(chan stringBox),deleteChan: make(chan stringBox))}

	// go libst.handleListMap()
	// go libst.handleStringMap()

	// print("My lease mode is:", mode)
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

	for _, node := range reply.Servers {
		for _, vID := range node.VirtualIDs {
			libst.hostPorts[vID] = node.HostPort
		}
		libst.virtualIDs = append(libst.virtualIDs, node.VirtualIDs...)
	}

	//fmt.Println("Virtual IDs: ", libst.virtualIDs)

	sort.Slice(libst.virtualIDs, func(i, j int) bool { return libst.virtualIDs[i] > libst.virtualIDs[j] })

	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libst))
	// rpc.HandleHTTP()

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
	// ls.stringLock.Lock()
	// defer ls.stringLock.Unlock()
	ls.mapstringLock.Lock()
	defer ls.mapstringLock.Unlock()

	// defer keyLock.Unlock()

	// fmt.Println("------------------------------------------")
	replyArgs := &storagerpc.GetReply{}
	// fmt.Println("Getting ", key)
	// fmt.Println("My hostport is libst: ", ls.myPort)
	requestArgs := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}
	if _, ok := ls.stringStore[key]; !ok {
		// fmt.Println("Creating new cache string")
		ls.stringStore[key] = &cacheString{window: make([]time.Time, 0), leaseTime: time.Time{}}
	}
	timeNow := time.Now()
	ls.stringStore[key].window = append(ls.stringStore[key].window, timeNow)
	if timeNow.Before(ls.stringStore[key].leaseTime.Add(ls.stringStore[key].validSeconds)) {
		// fmt.Printf("Lease is still valid. Time now:%v , valid seconds: %v, leasetime :%v", timeNow, ls.stringStore[key].leaseTime, ls.stringStore[key].validSeconds)
		return ls.stringStore[key].value, nil
	}
	for len(ls.stringStore[key].window) > 0 &&
		timeNow.After(ls.stringStore[key].window[0].Add(time.Duration(storagerpc.QueryCacheSeconds)*time.Second)) {
		ls.stringStore[key].window = ls.stringStore[key].window[1:]
	}
	if len(ls.stringStore[key].window) >= storagerpc.QueryCacheThresh {
		// fmt.Println("Lease has expired, want new lease")
		//Request lease
		requestArgs.WantLease = true
	}

	if ls.mode == Never {
		requestArgs.WantLease = false
	} else if ls.mode == Always {
		requestArgs.WantLease = true
	}

	id := ls.getVirtualID(key)
	err := errors.New("")
	var client *rpc.Client
	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
		err = client.Call("StorageServer.Get", requestArgs, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}

	if replyArgs.Status == storagerpc.OK && requestArgs.WantLease && replyArgs.Lease.Granted {
		// fmt.Println("Lease granted, update value: ", replyArgs.Value)
		validDur := time.Duration(replyArgs.Lease.ValidSeconds) * time.Second
		ls.stringStore[key].validSeconds = validDur
		ls.stringStore[key].leaseTime = time.Now()
		ls.stringStore[key].value = replyArgs.Value
		go func() {

			time.Sleep(validDur)
			// keyLock.Lock()
			// defer keyLock.Unlock()
			//fmt.Println("About to expire ", key)
			ls.mapstringLock.Lock()
			defer ls.mapstringLock.Unlock()

			ls.stringStore[key].leaseTime = time.Time{}
			ls.stringStore[key].value = ""
		}()
		// fmt.Println("CacheString updated to", ls.stringStore[key])
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
	var client *rpc.Client

	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
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
	var client *rpc.Client
	id := ls.getVirtualID(key)
	err := errors.New("")
	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
		err = client.Call("StorageServer.Delete", &storagerpc.DeleteArgs{Key: key}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Delete error")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// ls.stringLock.Lock()
	// defer ls.stringLock.Unlock()
	ls.maplistLock.Lock()
	defer ls.maplistLock.Unlock()

	// fmt.Println("------------------------------------------")
	replyArgs := &storagerpc.GetListReply{}
	requestArgs := &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}

	if _, ok := ls.listStore[key]; !ok {
		// fmt.Println("Creating new cache string")
		ls.listStore[key] = &cacheList{window: make([]time.Time, 0), leaseTime: time.Time{}}
	}
	timeNow := time.Now()
	ls.listStore[key].window = append(ls.listStore[key].window, timeNow)
	if timeNow.Before(ls.listStore[key].leaseTime.Add(ls.listStore[key].validSeconds)) {
		// fmt.Printf("Lease is still valid. Time now:%v , valid seconds: %v, leasetime :%v", timeNow, ls.listStore[key].leaseTime, ls.listStore[key].validSeconds)
		return ls.listStore[key].value, nil
	}
	ls.listStore[key].value = make([]string, 0)
	for len(ls.listStore[key].window) > 0 &&
		timeNow.After(ls.listStore[key].window[0].Add(time.Duration(storagerpc.QueryCacheSeconds)*time.Second)) {
		ls.listStore[key].window = ls.listStore[key].window[1:]
	}
	if len(ls.listStore[key].window) >= storagerpc.QueryCacheThresh {
		// fmt.Printf("Time now:%v , leasetime: %v, valid second :%v", timeNow, ls.listStore[key].leaseTime, ls.listStore[key].validSeconds)

		// fmt.Println("want new lease")
		//Request lease
		requestArgs.WantLease = true
	}
	if ls.mode == Never {
		requestArgs.WantLease = false
	} else if ls.mode == Always {
		requestArgs.WantLease = true
	}

	id := ls.getVirtualID(key)
	err := errors.New("")
	var client *rpc.Client
	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
		err = client.Call("StorageServer.GetList", &storagerpc.GetArgs{Key: key, WantLease: requestArgs.WantLease, HostPort: ls.myPort}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	// fmt.Println(replyArgs.Status, requestArgs.WantLease, replyArgs.Lease.Granted)
	if replyArgs.Status == storagerpc.OK && requestArgs.WantLease && replyArgs.Lease.Granted {
		// fmt.Println("Lease granted, update value: ", replyArgs.Value)
		validDur := time.Duration(replyArgs.Lease.ValidSeconds) * time.Second
		ls.listStore[key].validSeconds = validDur
		ls.listStore[key].leaseTime = time.Now()
		ls.listStore[key].value = replyArgs.Value
		go func() {
			time.Sleep(validDur)
			ls.maplistLock.Lock()
			defer ls.maplistLock.Unlock()
			ls.listStore[key].leaseTime = time.Time{}
			ls.listStore[key].value = make([]string, 0)
		}()
		// fmt.Println("CacheString updated to", ls.listStore[key])
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
	var client *rpc.Client
	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
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
	var client *rpc.Client
	for err != nil && err.Error() != "error" {
		client, id = ls.getClient(id)
		err = client.Call("StorageServer.AppendToList", &storagerpc.PutArgs{Key: key, Value: newItem}, replyArgs)
		id = (id + 1) % len(ls.virtualIDs)
	}
	if replyArgs.Status != storagerpc.OK {
		return errors.New("Append to list error")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	//print("Revoke Lease Called")
	reply.Status = storagerpc.KeyNotFound

	ls.mapstringLock.Lock()
	if _, ok := ls.stringStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		//fmt.Println("Revoke lease", args.Key)
		ls.stringStore[args.Key].leaseTime = time.Time{}
		ls.stringStore[args.Key].value = ""
	}
	ls.mapstringLock.Unlock()

	ls.maplistLock.Lock()
	if _, ok := ls.listStore[args.Key]; ok {
		reply.Status = storagerpc.OK
		ls.listStore[args.Key].value = make([]string, 0)
		ls.listStore[args.Key].leaseTime = time.Time{}
	}
	ls.maplistLock.Unlock()

	return nil
}
