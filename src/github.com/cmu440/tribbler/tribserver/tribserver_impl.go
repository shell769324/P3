package tribserver

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"encoding/json"

	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/util"
)

type tribServer struct {
	// TODO: implement this!
	listener net.Listener
	libStore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	tribserver := new(tribServer)
	listener, err := net.Listen("tcp", "localhost:"+myHostPort)
	tribserver.listener = listener
	tribserver.libStore, err = libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}

	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribserver))
	if err != nil {
		return nil, err
	}
	return tribserver, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	err := ts.libStore.Put(util.FormatUserKey(args.UserID), "")
	if err != nil {
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.libStore.Get(util.FormatUserKey(args.TargetUserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	ts.libStore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.libStore.Get(util.FormatUserKey(args.TargetUserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	ts.libStore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subscriptions, err := ts.libStore.GetList(util.FormatSubListKey(args.UserID))
	reply.Status = tribrpc.OK
	for _, sub := range subscriptions {
		otherSubscriptions, err := ts.libStore.GetList(util.FormatSubListKey(sub))
		for _, otherSub := range otherSubscriptions {
			if otherSub == args.UserID {
				reply.UserIDs = append(reply.UserIDs, sub)
				break
			}
		}
	}
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribble := tribrpc.Tribble{UserID: args.UserID, Contents: args.Contents, Posted: time.Now()}
	tribbleMar, _ := json.Marshal(tribble)
	tribKey := util.FormatPostKey(args.UserID, tribble.Posted.Unix())
	ts.libStore.AppendToList(util.FormatTribListKey(args.UserID), tribKey)
	ts.libStore.Put(tribKey, string(tribbleMar))
	reply.PostKey = tribKey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err == nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	err = ts.libStore.RemoveFromList(util.FormatTribListKey(args.UserID), args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	ts.libStore.Delete(args.PostKey)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	return errors.New("not implemented")
}
