package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
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
	if err == nil {
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.libStore.Get(util.FormatUserKey(args.TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	err = ts.libStore.AppendToList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	_, err = ts.libStore.Get(util.FormatUserKey(args.TargetUserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	err = ts.libStore.RemoveFromList(util.FormatSubListKey(args.UserID), args.TargetUserID)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	subscriptions, err := ts.libStore.GetList(util.FormatSubListKey(args.UserID))
	reply.Status = tribrpc.OK
	for _, sub := range subscriptions {
		otherSubscriptions, _ := ts.libStore.GetList(util.FormatSubListKey(sub))
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
	if err != nil {
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
	if err != nil {
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
	// Check if user exists first. TODO can do away without this
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribList, _ := ts.libStore.GetList(util.FormatTribListKey(args.UserID))
	tribbleList := make([]tribrpc.Tribble, 0)
	for _, tribID := range tribList {
		marshalledTribble, _ := ts.libStore.Get(tribID)
		var tribble tribrpc.Tribble
		if err := json.Unmarshal([]byte(marshalledTribble), &tribble); err != nil {
			panic(err)
		}
		tribbleList = append(tribbleList, tribble)
	}
	// fmt.Printf("Len of tribble list is : %v\n", len(tribbleList))
	start := 0
	if len(tribbleList) >= 100 {
		start = len(tribbleList) - 100
	}
	for i := len(tribbleList) - 1; i >= start; i-- {
		reply.Tribbles = append(reply.Tribbles, tribbleList[i])
	}
	reply.Status = tribrpc.OK

	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	// Check if user exists first. TODO can do away without this
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// Get Subscription List
	subList, _ := ts.libStore.GetList(util.FormatSubListKey(args.UserID))

	tribbles := make([][]tribrpc.Tribble, len(subList))

	for i := 0; i < len(subList); i++ {
		tribList, _ := ts.libStore.GetList(util.FormatTribListKey(subList[i]))
		itemsSize := 100
		if len(tribList) < 100 {
			itemsSize = len(tribList)
		}
		tribbles[i] = make([]tribrpc.Tribble, itemsSize)
		tribbleList := make([]tribrpc.Tribble, 0)
		for j := itemsSize - 1; j >= 0; j-- {
			marshalledTribble, _ := ts.libStore.Get(tribList[j])
			var tribble tribrpc.Tribble
			if err := json.Unmarshal([]byte(marshalledTribble), &tribble); err != nil {
				panic(err)
			}
			tribbleList = append(tribbleList, tribble)
			tribbles[i] = append(tribbles[i], tribble)
			// fmt.Printf("%v\n", tribble)
		}
		// tribbles[i] = append(tribbles[i], tribbleList[j])

		// for _, tribID := range tribList {
		// 	marshalledTribble, _ := ts.libStore.Get(tribID)
		// 	var tribble tribrpc.Tribble
		// 	if err := json.Unmarshal([]byte(marshalledTribble), &tribble); err != nil {
		// 		panic(err)
		// 	}
		// 	tribbleList = append(tribbleList, tribble)
		// }
		// start := 0
		// if len(tribbleList) >= 100 {
		// 	start = len(tribbleList) - 100
		// }
		// for j := len(tribbleList) - 1; j >= 0; j-- {
		// 	tribbles[i] = append(tribbles[i], tribbleList[j])
		// }
	}
	tribSortIndex := make([]int, len(subList))

	for {
		itemsRemaining := 0
		maxIndex := 0
		maxTimeStamp := time.Now()
		for i := 0; i < len(tribbles); i++ {
			fmt.Printf("Index: %v Length %v\n", i, len(tribbles[i]))
			if tribSortIndex[i] >= len(tribbles[i]) {
				continue
			}
			if i == 0 {
				maxTimeStamp = tribbles[i][tribSortIndex[i]].Posted
				maxIndex = 0
			} else {
				if maxTimeStamp.After(tribbles[i][tribSortIndex[i]].Posted) {
					maxTimeStamp = tribbles[i][tribSortIndex[i]].Posted
					maxIndex = i
				}
			}
			itemsRemaining += len(tribbles[i]) - 1 - tribSortIndex[i]
		}
		if itemsRemaining == 0 {
			break
		}
		fmt.Printf("Max Index: %v Sort Index: %v\n", maxIndex, tribSortIndex[maxIndex])
		reply.Tribbles = append(reply.Tribbles, tribbles[maxIndex][tribSortIndex[maxIndex]])
		tribSortIndex[maxIndex]++
		if len(reply.Tribbles) == 100 {
			break
		}
	}

	reply.Status = tribrpc.OK

	return nil

	return errors.New("not implemented")
}
