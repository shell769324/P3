package libstore

import (
	"errors"
	"fmt"
	//"log"
	"net/rpc"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type libstore struct {
	// TODO: implement this!
	myPort        string
	masterStorage *rpc.Client
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
	if err != nil {
		return nil, err
	}
	libst.masterStorage = client
	// replyArgs := &storagerpc.GetReply{}

	// libst.masterStorage.Call("StorageServer.Get", &storagerpc.GetArgs{Key: "hello", WantLease: false, HostPort: libst.myPort}, replyArgs)
	return libst, nil
	// return nil, errors.New("not implemented")
}

func (ls *libstore) Get(key string) (string, error) {
	replyArgs := &storagerpc.GetReply{}
	fmt.Println("Calling get rpc")
	err := ls.masterStorage.Call("StorageServer.Get", &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}, replyArgs)
	fmt.Printf("Got value: %v", replyArgs.Value)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("Get error")
	}
	return replyArgs.Value, err
}

func (ls *libstore) Put(key, value string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.Put", &storagerpc.PutArgs{Key: key, Value: value}, replyArgs)
	//fmt.Printf("Status value:%v key not found: % v",replyArgs.Status,replyArgs.Status==storagerpc.KeyNotFound)
	fmt.Println(err)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("Put error")
	}
	fmt.Println(err)
	return err
}

func (ls *libstore) Delete(key string) error {
	replyArgs := &storagerpc.DeleteReply{}
	err := ls.masterStorage.Call("StorageServer.Delete", &storagerpc.DeleteArgs{Key: key}, replyArgs)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("Delete error")
	}
	return err
}

func (ls *libstore) GetList(key string) ([]string, error) {
	replyArgs := &storagerpc.GetListReply{}
	err := ls.masterStorage.Call("StorageServer.GetList", &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}, replyArgs)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("Get list error")
	}
	return replyArgs.Value, err
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.RemoveFromList", &storagerpc.PutArgs{Key: key, Value: removeItem}, replyArgs)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("RemoveFromList error")
	}
	return err
}

func (ls *libstore) AppendToList(key, newItem string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.AppendToList", &storagerpc.PutArgs{Key: key, Value: newItem}, replyArgs)
	fmt.Println(err)
	if err == nil && replyArgs.Status != storagerpc.OK {
		err = errors.New("AppendToList error")
	}
	return err
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
