package gfs_stress

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"

	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
)

type Config struct {
	ID     string
	Role   string
	Listen string
	Master string
	Center string
}

var (
	m    *master.Master
	cs   *chunkserver.ChunkServer
	c    *client.Client
	conf Config
	root string
)

type RPCStringMessage struct {
	ID      string
	Message string
}

func WritePID() {
	PIDFile := "/tmp/gfs.pid"
	f, err := os.OpenFile(PIDFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalln("cannot open ", PIDFile, ":", err)
	}
	if _, err = f.WriteString(fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		log.Fatalln("cannot write pid", err)
	}
}

func ReadAndChecksum(path string, start, end int) ([]byte, error) {
	h := md5.New()
	buf := make([]byte, 0, 128<<20)
	offset := start
	for {
		n, err := c.Read(gfs.Path(path), gfs.Offset(offset), buf[:cap(buf)])
		buf = buf[:n]
		if err != nil && err != io.EOF {
			return nil, err
		}
		h.Write(buf)
		if err == io.EOF {
			break
		}
	}
	return h.Sum(nil), nil
}

func call(srv, rpcname string, args interface{}, reply interface{}) error {
	c, errx := rpc.Dial("tcp", srv)
	if errx != nil {
		return errx
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err != nil {
		return err
	}

	return nil
}

func sendAck() {
	log.Println("sendAck")
	call(conf.Center, "RPC.Acknowledge", conf.ID, nil)
}

func waitMessage(expect string) {
	var msg string
	log.Printf("waitMessage(%s)\n", expect)
	for {
		call(conf.Center, "RPC.WhatToDo", struct{}{}, &msg)
		if msg == expect {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func asMaster() {
	m = master.NewAndServe(gfs.ServerAddress(conf.Listen), root)
}

func asChunkserver() {
	cs = chunkserver.NewAndServe(gfs.ServerAddress(conf.Listen), gfs.ServerAddress(conf.Master), root)
	c = client.NewClient(gfs.ServerAddress(conf.Master))

	runConsistencyWriteSuccess()
	runConsistencyAppendSuccess()
}

func Run(cfg Config) {
	conf = cfg

	// create temporary directory
	var err error
	root, err = ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	// run
	waitMessage("wait")
	sendAck()
	switch conf.Role {
	case "master":
		asMaster()
	case "chunkserver":
		asChunkserver()
	}

	// shutdown
	waitMessage("Shutdown")
	sendAck()
	os.RemoveAll(root)
}
