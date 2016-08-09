package main

import (
	"bufio"
	"flag"
	"fmt"
	"gfs"
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
	Count         int
	InitializerID string

	checkPoint []gfs_stress.ConsistencyWriteSuccess_CheckPoint
	md5s       [][]byte
	lock       sync.Mutex
	initSpeed  gfs_stress.NetSpeed
	writeSpeed gfs_stress.NetSpeed
	readSpeed  gfs_stress.NetSpeed
}

func NewConsistencyWriteSuccess() (t *ConsistencyWriteSuccess) {
	t = &ConsistencyWriteSuccess{
		FilePath:      "/ConsistencyWriteSuccess.txt",
		FileSize:      10 << 10, //1000000000,
		MaxWriteSize:  3 << 10,  //128 << 20,
		Count:         3,
		InitializerID: chunkserverID[rand.Intn(len(chunkserverID))],
		initSpeed:     gfs_stress.NewNetSpeed(),
		writeSpeed:    gfs_stress.NewNetSpeed(),
		readSpeed:     gfs_stress.NewNetSpeed(),
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
	reply.Count = t.Count
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
	ack(args.ID)

	ok := reflect.DeepEqual(args.MD5s, t.md5s)
	if !ok {
		fail(args.ID, "different data read from different servers")
	}
	t.initSpeed.Merge(args.InitSpeed)
	t.readSpeed.Merge(args.ReadSpeed)
	t.writeSpeed.Merge(args.WriteSpeed)
	return nil
}

func (t *ConsistencyWriteSuccess) success() {
	log.Println("++++++ Pass! Statistics:")
	log.Println("    Create File Speed:", t.initSpeed.String())
	log.Println("          Write Speed:", t.writeSpeed.String())
	log.Println("           Read Speed:", t.readSpeed.String())
}

/**********************************************************
 * ConsistencyAppendSuccess
**********************************************************/

type ConsistencyAppendSuccess struct {
	FilePath      string
	MaxSize       int
	Count         int
	InitializerID string

	lock        sync.Mutex
	maxOffset   gfs.Offset
	checkchunk  map[string][]int
	set         map[gfs_stress.ConsistencyAppendSuccess_CheckPoint]bool
	appendSpeed gfs_stress.NetSpeed
	readSpeed   gfs_stress.NetSpeed
}

func NewConsistencyAppendSuccess() (t *ConsistencyAppendSuccess) {
	t = &ConsistencyAppendSuccess{
		FilePath:      "/ConsistencyAppendSuccess.txt",
		MaxSize:       10 << 10, //1000000000,
		Count:         10,
		InitializerID: chunkserverID[rand.Intn(len(chunkserverID))],
		appendSpeed:   gfs_stress.NewNetSpeed(),
		readSpeed:     gfs_stress.NewNetSpeed(),
	}
	return t
}

func (t *ConsistencyAppendSuccess) GetConfig(args struct{}, reply *gfs_stress.ConsistencyAppendSuccess_GetConfigReply) error {
	reply.FilePath = t.FilePath
	reply.MaxSize = t.MaxSize
	reply.Count = t.Count
	reply.InitializerID = t.InitializerID
	return nil
}

func (t *ConsistencyAppendSuccess) generateCheckChunk() {
	chunks := int((t.maxOffset + gfs.MaxChunkSize - 1) / gfs.MaxChunkSize)
	avg := chunks / len(chunkserverID)
	rest := chunks - avg*len(chunkserverID)
	t.checkchunk = make(map[string][]int)
	t.set = make(map[gfs_stress.ConsistencyAppendSuccess_CheckPoint]bool)
	x := 0
	add := func(id string) {
		t.checkchunk[id] = append(t.checkchunk[id], x)
		x++
	}
	for i, id := range chunkserverID {
		for j := 0; j < avg; j++ {
			add(id)
		}
		if i < rest {
			add(id)
		}
	}
}

func (t *ConsistencyAppendSuccess) ReportOffset(args gfs_stress.ConsistencyAppendSuccess_ReportOffsetArg, reply *struct{}) error {
	ack(args.ID)
	t.lock.Lock()
	if args.Offset > t.maxOffset {
		t.maxOffset = args.Offset
	}
	t.lock.Unlock()
	return nil
}

func (t *ConsistencyAppendSuccess) GetCheckChunk(args string, reply *[]int) error {
	ack(args)
	*reply = t.checkchunk[args]
	return nil
}

func (t *ConsistencyAppendSuccess) ReportCheck(args gfs_stress.ConsistencyAppendSuccess_ReportCheckArg, reply *struct{}) error {
	ack(args.ID)
	t.lock.Lock()
	for _, v := range args.Found {
		t.set[v] = true
	}
	t.lock.Unlock()
	t.appendSpeed.Merge(args.AppendSpeed)
	t.readSpeed.Merge(args.ReadSpeed)
	return nil
}

func (t *ConsistencyAppendSuccess) check() {
	tot := t.Count * len(chunkserverID)
	found := len(t.set)
	if found != tot {
		fail("", fmt.Sprintf("should have %d records, but %d found.", tot, found))
	}
	log.Println("++++++ Pass! Statistics:")
	log.Println("         Append Speed:", t.appendSpeed.String())
	log.Println("           Read Speed:", t.readSpeed.String())
}

/**********************************************************
 * main
**********************************************************/

var (
	rpc_what_to_do string
	masterID       string
	chunkserverID  []string
	_ack           map[string]bool
	lock           sync.RWMutex
	shutdown       chan struct{}
)

func ack(id string) {
	lock.Lock()
	_ack[id] = true
	lock.Unlock()
}

type RPC struct{}

func (*RPC) WhatToDo(args struct{}, reply *string) error {
	*reply = rpc_what_to_do
	return nil
}

func (*RPC) Acknowledge(args string, reply *struct{}) error {
	log.Printf("RPC.Acknowledge(%v)\n", args)
	ack(args)
	return nil
}

func (*RPC) ReportFailure(args gfs_stress.RPCStringMessage, reply *struct{}) error {
	log.Printf("RPC.ReportFailure(%v)\n", args)
	fail(args.ID, args.Message)
	return nil
}

func fail(id, msg string) {
	if id == "" {
		log.Fatalf("!!!!!!!!!! Fail: %s\n", msg)
	} else {
		log.Fatalf("!!!!!!!!!! Fail on Node %s: %s\n", id, msg)
	}
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
	_ack = make(map[string]bool)
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
		_ack[s] = false
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
	for k := range _ack {
		_ack[k] = false
	}
	lock.Unlock()
}

func _ensureAck(includeMaster bool) {
	for {
		ok := true
		for k, v := range _ack {
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
	gfs_stress.WritePID()
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
	cws.success()

	// Test: ConsistencyAppendSuccess
	log.Println("========== Test: ConsistencyAppendSuccess")
	cas := NewConsistencyAppendSuccess()
	rpcs.Register(cas)
	newMessage("ConsistencyAppendSuccess:GetConfig")
	ensureAck()
	newMessage("ConsistencyAppendSuccess:Append")
	ensureAck()
	cas.generateCheckChunk()
	newMessage("ConsistencyAppendSuccess:GetCheckChunk")
	ensureAck()
	newMessage("ConsistencyAppendSuccess:Check")
	ensureAck()
	cas.check()

	// Finish: Shutdown all
	log.Println("========== Shutdown")
	newMessage("Shutdown")
	ensureAck()
	log.Println("========== Well Done! You've passed all tests!")
}
