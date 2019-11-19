package libstore

import (
	"errors"
	"fmt"
	"log"
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
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v", masterServerHostPort))
	if err != nil {
		log.Fatal("dialing:", err)
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
	ls.masterStorage.Call("StorageServer.Get", &storagerpc.GetArgs{Key: key, WantLease: false, HostPort: ls.myPort}, replyArgs)
	fmt.Printf("Got value: %v", replyArgs.Value)
	return replyArgs.Value, errors.New("Error")

}

func (ls *libstore) Put(key, value string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.Put", &storagerpc.PutArgs{Key: key, Value: value}, replyArgs)
	if err != nil {
		return errors.New("Error in put")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	return errors.New("not implemented")
}

func (ls *libstore) GetList(key string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.RemoveFromList", &storagerpc.PutArgs{Key: key, Value: removeItem}, replyArgs)
	return err

}

func (ls *libstore) AppendToList(key, newItem string) error {
	replyArgs := &storagerpc.PutReply{}
	err := ls.masterStorage.Call("StorageServer.AppendToList", &storagerpc.PutArgs{Key: key, Value: newItem}, replyArgs)
	if err != nil {
		return errors.New("Error in put")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
