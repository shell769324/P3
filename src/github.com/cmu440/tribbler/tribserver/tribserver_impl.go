package tribserver

import (
	"encoding/json"
	//"errors"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
)

type tribServer struct {
	// TODO: implement this!
	listener net.Listener
	libStore libstore.Libstore
	mux      sync.Mutex
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	fmt.Println("Trib Server is called")
	tribserver := new(tribServer)
	listener, err := net.Listen("tcp", myHostPort)
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
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	return tribserver, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	userKey := util.FormatUserKey(args.UserID)
	_, err := ts.libStore.Get(userKey)
	if err == nil {
		reply.Status = tribrpc.Exists
	} else {
		reply.Status = tribrpc.OK
		ts.libStore.Put(userKey, "")
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
	//fmt.Println(args.UserID, "posts new Trib,", args.Contents)
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	ts.mux.Lock()
	tribble := tribrpc.Tribble{UserID: args.UserID, Contents: args.Contents, Posted: time.Now()}
	tribbleMar, _ := json.Marshal(tribble)

	tribKey := util.FormatPostKey(args.UserID, tribble.Posted.UnixNano())
	err = ts.libStore.AppendToList(util.FormatTribListKey(args.UserID), tribKey)
	for err != nil {
		tribKey := util.FormatPostKey(args.UserID, tribble.Posted.UnixNano())
		err = ts.libStore.AppendToList(util.FormatTribListKey(args.UserID), tribKey)
	}
	ts.mux.Unlock()
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

func recentFirst(tribList []string) []string {
	if len(tribList) <= 1 {
		return tribList
	}
	afterColon1 := strings.Split(tribList[0], ":")[1]
	unixTime1, _ := strconv.ParseInt(strings.Split(afterColon1, "_")[1], 16, 64)
	afterColon2 := strings.Split(tribList[1], ":")[1]
	unixTime2, _ := strconv.ParseInt(strings.Split(afterColon2, "_")[1], 16, 64)
	if unixTime1 < unixTime2 {
		for i := 0; i < (len(tribList) / 2); i++ {
			temp := tribList[i]
			tribList[i] = tribList[len(tribList)-1-i]
			tribList[len(tribList)-1-i] = temp
		}
	}
	if len(tribList) >= 100 {
		tribList = tribList[:100]
	}
	return tribList
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	//fmt.Println(args.UserID, "is calling get tribs")
	// Check if user exists first. TODO can do away without this
	_, err := ts.libStore.Get(util.FormatUserKey(args.UserID))
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	tribList, _ := ts.libStore.GetList(util.FormatTribListKey(args.UserID))
	tribList = recentFirst(tribList)
	for _, tribID := range tribList {
		marshalledTribble, getErr := ts.libStore.Get(tribID)
		// for getErr != nil {
		// 	marshalledTribble, getErr = ts.libStore.Get(tribID)
		// }
		if getErr != nil {
			continue
		}
		var tribble tribrpc.Tribble
		if err := json.Unmarshal([]byte(marshalledTribble), &tribble); err != nil {
			panic(err)
		}
		reply.Tribbles = append(reply.Tribbles, tribble)
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
	tribbleIDs := make([][]string, len(subList))
	for i := 0; i < len(subList); i++ {
		tribList, _ := ts.libStore.GetList(util.FormatTribListKey(subList[i]))
		tribbleIDs[i] = recentFirst(tribList)
	}
	tribSortIndex := make([]int, len(subList))
	for {
		itemsRemaining := 0
		maxIndex := 0
		maxTimeStamp := int64(0)
		for i := 0; i < len(tribbleIDs); i++ {
			// fmt.Printf("Index: %v Length %v\n", i, len(tribbles[i]))
			if tribSortIndex[i] >= len(tribbleIDs[i]) {
				continue
			}
			//fmt.Printf("i: %vTimestamp:%v My Timestamp: %v\n", i, maxTimeStamp, tribbles[i][tribSortIndex[i]].Posted)
			afterColon := strings.Split(tribbleIDs[i][tribSortIndex[i]], ":")[1]
			unixTime, _ := strconv.ParseInt(strings.Split(afterColon, "_")[1], 16, 64)
			if unixTime > maxTimeStamp {
				maxTimeStamp = unixTime
				maxIndex = i
			}
			itemsRemaining += len(tribbleIDs[i]) - tribSortIndex[i]
		}
		//fmt.Printf("Items Remaining: %v\n", itemsRemaining)
		if itemsRemaining == 0 {
			break
		}
		//fmt.Printf("Max Index: %v Sort Index: %v \n", maxIndex, tribSortIndex[maxIndex])
		marshalledTribble, getError := ts.libStore.Get(tribbleIDs[maxIndex][tribSortIndex[maxIndex]])
		if getError != nil {
			tribSortIndex[maxIndex]++
			continue
		}
		var tribble tribrpc.Tribble
		if err := json.Unmarshal([]byte(marshalledTribble), &tribble); err != nil {
			panic(err)
		}
		reply.Tribbles = append(reply.Tribbles, tribble)
		tribSortIndex[maxIndex]++
		if len(reply.Tribbles) == 100 {
			break
		}
	}

	reply.Status = tribrpc.OK

	return nil
}
