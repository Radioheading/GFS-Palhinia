package main

import (
	"bufio"
	"flag"
	"fmt"
	"gfs_stress"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"
)

/**********************************************************
 * ConsistencyWriteSuccess
**********************************************************/

type ConsistencyWriteSuccess struct {
	FilePath      string
	FileSize      int
	MaxWriteSize  int
	InitializerID string

	checkPoint []gfs_stress.ConsistencyWriteSuccess_CheckPoint
	md5s       [][]byte
	lock       sync.Mutex
}

func NewConsistencyWriteSuccess() (t *ConsistencyWriteSuccess) {
	t = &ConsistencyWriteSuccess{
		FilePath:      "/ConsistencyWriteSuccess.txt",
		FileSize:      10 << 20, //1000000000,
		MaxWriteSize:  1 << 20,  //128 << 20,
		InitializerID: chunkserverID[rand.Intn(len(chunkserverID))],
	}
	for n := 100; n > 0; n-- {
		x := rand.Intn(t.FileSize)
		y := rand.Intn(t.FileSize)
		if x > y {
			x, y = y, x
		}
		t.checkPoint = append(t.checkPoint, gfs_stress.ConsistencyWriteSuccess_CheckPoint{x, y})
	}
	return t
}

func (t *ConsistencyWriteSuccess) GetConfig(args struct{}, reply *gfs_stress.ConsistencyWriteSuccess_GetConfigReply) error {
	reply.FilePath = t.FilePath
	reply.FileSize = t.FileSize
	reply.MaxWriteSize = t.MaxWriteSize
	reply.CheckPoint = t.checkPoint
	reply.InitializerID = t.InitializerID
	return nil
}

func (t *ConsistencyWriteSuccess) ReportCheckPoint(args gfs_stress.ConsistencyWriteSuccess_ReportCheckPointArg, reply *struct{}) error {
	if len(args.MD5s) != len(t.checkPoint) {
		fail(args.ID, fmt.Sprintf("len(args.MD5s) %v != %v len(t.checkPoint)", len(args.MD5s), len(t.checkPoint)))
		return nil
	}

	t.lock.Lock()
	if t.md5s == nil {
		t.md5s = args.MD5s
	}
	t.lock.Unlock()
	lock.Lock()
	ack[args.ID] = true
	lock.Unlock()

	ok := reflect.DeepEqual(args.MD5s, t.md5s)
	if !ok {
		fail(args.ID, "different data read from different servers")
	}
	return nil
}

/**********************************************************
 * main
**********************************************************/

var (
	rpc_what_to_do string
	masterID       string
	chunkserverID  []string
	ack            map[string]bool
	lock           sync.RWMutex
	shutdown       chan struct{}
)

type RPC struct{}

func (*RPC) WhatToDo(args struct{}, reply *string) error {
	*reply = rpc_what_to_do
	return nil
}

func (*RPC) Acknowledge(args string, reply *struct{}) error {
	log.Printf("RPC.Acknowledge(%v)\n", args)
	lock.Lock()
	ack[args] = true
	lock.Unlock()
	return nil
}

func (*RPC) ReportFailure(args gfs_stress.RPCStringMessage, reply *struct{}) error {
	fail(args.ID, args.Message)
	return nil
}

func fail(id, msg string) {
	log.Fatalf("Fail on Node %s: %s\n", id, msg)
}

func rpcHandler(l net.Listener, rpcs *rpc.Server) {
	for {
		select {
		case <-shutdown:
			return
		default:
		}
		conn, err := l.Accept()
		if err == nil {
			go func() {
				rpcs.ServeConn(conn)
				conn.Close()
			}()
		} else {
			log.Fatal(err)
		}
	}
}

func readServers(path string) {
	ack = make(map[string]bool)
	f, err := os.Open(path)
	if err != nil {
		log.Fatal("cannot open server list file")
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		s, err := r.ReadString('\n')
		if err != nil {
			break
		}
		s = strings.TrimSpace(s)
		ack[s] = false
		if masterID == "" {
			masterID = s
		} else {
			chunkserverID = append(chunkserverID, s)
		}
	}
	if masterID == "" || len(chunkserverID) < 3 {
		log.Fatalln("the server list should contain a master and at least 3 chunkservers")
	}
	log.Printf("got %d servers", len(chunkserverID)+1)
}

func newMessage(msg string) {
	log.Printf("newMessage(%s)\n", msg)
	rpc_what_to_do = msg
	lock.Lock()
	for k := range ack {
		ack[k] = false
	}
	lock.Unlock()
}

func _ensureAck(includeMaster bool) {
	for {
		ok := true
		for k, v := range ack {
			if !v && (k != masterID || includeMaster) {
				ok = false
				break
			}
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func ensureAck() {
	_ensureAck(false)
}

func main() {
	// Start up
	serversFile := flag.String("server-list", "servers.txt", "path to the server list file. the first line is the master and the rest are chunkservers.")
	listen := flag.String("listen", "", "listen address")
	flag.Parse()
	if *listen == "" {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	readServers(*serversFile)

	l, e := net.Listen("tcp", *listen)
	if e != nil {
		log.Fatal("RPC listen error:", e)
	}
	log.Println("RPC server listening on ", *listen)
	shutdown = make(chan struct{})
	rpcs := rpc.NewServer()
	rpcs.RegisterName("RPC", &RPC{})
	go rpcHandler(l, rpcs)

	// Start up: Wait until all online
	newMessage("wait")
	_ensureAck(true)

	// Test: ConsistencyWriteSuccess
	log.Println("========== Test: ConsistencyWriteSuccess")
	cws := NewConsistencyWriteSuccess()
	rpcs.Register(cws)
	newMessage("ConsistencyWriteSuccess:GetConfig")
	ensureAck()
	newMessage("ConsistencyWriteSuccess:Run")
	ensureAck()

	// Finish: Shutdown all
	newMessage("Shutdown")
	ensureAck()
}
