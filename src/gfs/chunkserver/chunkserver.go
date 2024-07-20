package chunkserver

import (
	"fmt"
	"io"
	"net"
	"net/rpc"
	"os"
	"path"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
	"gfs/util"
)

// ChunkServer struct
type ChunkServer struct {
	address    gfs.ServerAddress // chunkserver address
	master     gfs.ServerAddress // master address
	serverRoot string            // path to data storage
	l          net.Listener
	shutdown   chan struct{}
	dead       bool // set to true if server is shutdown

	dl                     *downloadBuffer                // expiring download buffer
	pendingLeaseExtensions *util.ArraySet                 // pending lease extension
	chunk                  map[gfs.ChunkHandle]*chunkInfo // chunk information
	chunkProtector         sync.RWMutex                   // protect chunk map
}

type Mutation struct {
	mtype   gfs.MutationType
	version gfs.ChunkVersion
	data    []byte
	offset  gfs.Offset
}

type chunkInfo struct {
	sync.RWMutex
	length        gfs.Offset
	version       gfs.ChunkVersion               // version number of the chunk in disk
	newestVersion gfs.ChunkVersion               // allocated newest version number
	mutations     map[gfs.ChunkVersion]*Mutation // mutation buffer
}

// NewAndServe starts a chunkserver and return the pointer to it.
func NewAndServe(addr, masterAddr gfs.ServerAddress, serverRoot string) *ChunkServer {
	cs := &ChunkServer{
		address:                addr,
		shutdown:               make(chan struct{}),
		master:                 masterAddr,
		serverRoot:             serverRoot,
		dl:                     newDownloadBuffer(gfs.DownloadBufferExpire, gfs.DownloadBufferTick),
		pendingLeaseExtensions: new(util.ArraySet),
		chunk:                  make(map[gfs.ChunkHandle]*chunkInfo),
	}
	rpcs := rpc.NewServer()
	rpcs.Register(cs)
	l, e := net.Listen("tcp", string(cs.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	cs.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			conn, err := cs.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// if chunk server is dead, ignores connection error
				if !cs.dead {
					log.Fatal(err)
				}
			}
		}
	}()

	// Heartbeat
	go func() {
		for {
			select {
			case <-cs.shutdown:
				return
			default:
			}
			pe := cs.pendingLeaseExtensions.GetAllAndClear()
			le := make([]gfs.ChunkHandle, len(pe))
			for i, v := range pe {
				le[i] = v.(gfs.ChunkHandle)
			}
			args := &gfs.HeartbeatArg{
				Address:         addr,
				LeaseExtensions: le,
			}
			if err := util.Call(cs.master, "Master.RPCHeartbeat", args, nil); err != nil {
				log.Fatal("heartbeat rpc error ", err)
				log.Exit(1)
			}

			time.Sleep(gfs.HeartbeatInterval)
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

// ApplyMutationOnChunk applies mutation to the chunk
func (cs *ChunkServer) ApplyMutationOnChunk(handle gfs.ChunkHandle, m *Mutation) error {
	my_path := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.OpenFile(my_path, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	defer f.Close()
	_, err = f.WriteAt(m.data, int64(m.offset))
	if err != nil {
		return err
	}

	return nil
}

// Shutdown shuts the chunkserver down
func (cs *ChunkServer) Shutdown() {
	if !cs.dead {
		log.Warningf("ChunkServer %v shuts down", cs.address)
		cs.dead = true
		close(cs.shutdown)
		cs.l.Close()
	}
}

// RPCPushDataAndForward is called by client.
// It saves client pushed data to memory buffer and forward to all other replicas.
// Returns a DataID which represents the index in the memory buffer.
func (cs *ChunkServer) RPCPushDataAndForward(args gfs.PushDataAndForwardArg, reply *gfs.PushDataAndForwardReply) error {
	reply.DataID = cs.dl.New(args.Handle)

	var chainOrder []gfs.ServerAddress
	for _, addr := range args.ForwardTo {
		if addr != cs.address {
			chainOrder = append(chainOrder, addr)
		}
	}

	return cs.RPCForwardData(gfs.ForwardDataArg{DataID: reply.DataID, Data: args.Data, ChainOrder: chainOrder}, &gfs.ForwardDataReply{})
}

// RPCForwardData is called by another replica who sends data to the current memory buffer.
// TODO: This should be replaced by a chain forwarding.
func (cs *ChunkServer) RPCForwardData(args gfs.ForwardDataArg, reply *gfs.ForwardDataReply) error {
	if _, ok := cs.dl.Get(args.DataID); ok {
		return fmt.Errorf("DataID %v already found", args.DataID)
	}

	cs.dl.Set(args.DataID, args.Data)

	if len(args.ChainOrder) > 0 { // continue to forward
		addr := args.ChainOrder[0]
		args.ChainOrder = args.ChainOrder[1:]
		if err := util.Call(addr, "ChunkServer.RPCForwardData", args, reply); err != nil {
			return err
		}
	}

	return nil
}

// RPCCreateChunk is called by master to create a new chunk given the chunk handle.
func (cs *ChunkServer) RPCCreateChunk(args gfs.CreateChunkArg, reply *gfs.CreateChunkReply) error {
	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()
	if _, ok := cs.chunk[args.Handle]; ok {
		return fmt.Errorf("Chunk %v already exists", args.Handle)
	}

	cs.chunk[args.Handle] = &chunkInfo{length: 0, version: 0, newestVersion: 0, mutations: make(map[gfs.ChunkVersion]*Mutation)}
	log.Infof("Chunk %v created", args.Handle)
	return nil
}

// ReadChunk reads the chunk data from disk
func (cs *ChunkServer) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, length gfs.Offset) ([]byte, error) {
	cs.chunkProtector.RLock()
	if _, ok := cs.chunk[handle]; !ok {
		cs.chunkProtector.RUnlock()
		return nil, fmt.Errorf("Chunk %v not found", handle)
	}

	cs.chunkProtector.RUnlock()
	path := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.Open(path)

	if err != nil {
		return nil, err
	}

	defer f.Close()
	data := make([]byte, length)
	_, err = f.ReadAt(data, int64(offset))

	if err != nil {
		return nil, err
	}

	return data, nil
}

// WriteChunk writes the chunk data to disk
func (cs *ChunkServer) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	cs.chunkProtector.RLock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cs.chunk[handle]; !ok {
		cs.chunkProtector.RUnlock()
		return fmt.Errorf("Chunk %v not found", handle)
	}

	cs.chunkProtector.RUnlock()
	chunk.Lock()
	defer chunk.Unlock()
	err := cs.ApplyMutationOnChunk(handle, &Mutation{mtype: gfs.MutationWrite, version: chunk.version, data: data, offset: offset})

	if err != nil {
		return err
	}

	chunk.length = util.Max(chunk.length, offset+gfs.Offset(len(data)))
	chunk.version = chunk.newestVersion

	return nil
}

// RPCReadChunk is called by client, read chunk data and return
func (cs *ChunkServer) RPCReadChunk(args gfs.ReadChunkArg, reply *gfs.ReadChunkReply) error {
	data, err := cs.ReadChunk(args.Handle, args.Offset, gfs.Offset(args.Length))

	reply.Data = data

	if err == io.EOF {
		reply.ErrorCode = gfs.ReadEOF
		return nil
	}

	return err
}

// RPCWriteChunk is called by client
// applies chunk write to itself (primary) and asks secondaries to do the same.
func (cs *ChunkServer) RPCWriteChunk(args gfs.WriteChunkArg, reply *gfs.WriteChunkReply) error {
	op_data, ok := cs.dl.Get(args.DataID)

	if !ok {
		return fmt.Errorf("DataID %v not found", args.DataID)
	}

	err := cs.WriteChunk(args.DataID.Handle, args.Offset, op_data)

	if err != nil {
		return err
	}

	// ask mutation to be done on all secondaries
	err = util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", gfs.ApplyMutationArg{DataID: args.DataID, Mtype: gfs.MutationWrite, Offset: args.Offset})
	if err != nil {
		return err
	}

	cs.pendingLeaseExtensions.Add(args.DataID.Handle)
	return nil
}

// RPCAppendChunk is called by client to apply atomic record append.
// The length of data should be within max append size.
// If the chunk size after appending the data will excceed the limit,
// pad current chunk and ask the client to retry on the next chunk.
func (cs *ChunkServer) RPCAppendChunk(args gfs.AppendChunkArg, reply *gfs.AppendChunkReply) error {
	op_data, ok := cs.dl.Get(args.DataID)
	if !ok {
		return fmt.Errorf("DataID %v not found", args.DataID)
	}

	if gfs.Offset(len(op_data)) > gfs.MaxAppendSize {
		return fmt.Errorf("Append size %v out of bound", len(op_data))
	}

	cs.chunkProtector.RLock()

	var chunk *chunkInfo

	if chunk, ok = cs.chunk[args.DataID.Handle]; !ok {
		cs.chunkProtector.RUnlock()
		return fmt.Errorf("Chunk %v not found", args.DataID.Handle)
	}

	cs.chunkProtector.RUnlock()
	chunk.Lock()
	defer chunk.Unlock()

	m := gfs.ApplyMutationArg{DataID: args.DataID, Offset: chunk.length}
	reply.Offset = chunk.length

	if chunk.length+gfs.Offset(len(op_data)) > gfs.MaxChunkSize { // need to pad the current chunk
		m.Mtype = gfs.MutationPad
		reply.ErrorCode = gfs.AppendExceedChunkSize
		op_data = make([]byte, gfs.MaxChunkSize-chunk.length)
	} else {
		m.Mtype = gfs.MutationAppend
		reply.ErrorCode = gfs.Success
	}

	err := cs.WriteChunk(args.DataID.Handle, chunk.length, op_data)

	if err != nil {
		return err
	}

	if m.Mtype == gfs.MutationPad {
		chunk.length = gfs.MaxChunkSize
	} else {
		chunk.length += gfs.Offset(len(op_data))
	}

	return util.CallAll(args.Secondaries, "ChunkServer.RPCApplyMutation", m)
}

// RPCApplyWriteChunk is called by primary to apply mutations
// first we decide data to fill by judging whether its MutationPad
// then we apply the mutation to the chunk
func (cs *ChunkServer) RPCApplyMutation(args gfs.ApplyMutationArg, reply *gfs.ApplyMutationReply) error {
	var opdata []byte
	var ok bool

	if args.Mtype == gfs.MutationWrite || args.Mtype == gfs.MutationAppend {
		opdata, ok = cs.dl.Get(args.DataID)

		if !ok {
			return fmt.Errorf("DataID %v not found", args.DataID)
		}
	} else {
		opdata = make([]byte, gfs.MaxChunkSize-args.Offset)
	}

	log.Infof("Applying mutation %v to chunk %v", args.Mtype, args.Version)
	log.Infof("Data length: %v, offset: %v", len(opdata), args.Offset)

	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()

	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		return fmt.Errorf("Chunk %v not found", args.Version)
	}

	if args.Offset+gfs.Offset(len(opdata)) > gfs.Offset(len(opdata)) {
		return fmt.Errorf("Offset %v out of bound", args.Offset)
	}

	err := cs.ApplyMutationOnChunk(args.DataID.Handle, &Mutation{mtype: args.Mtype, version: args.Version, data: opdata, offset: args.Offset})

	if err != nil {
		return err
	}

	cs.chunk[args.DataID.Handle].newestVersion = args.Version
	cs.chunk[args.DataID.Handle].length = util.Max(cs.chunk[args.DataID.Handle].length, args.Offset+gfs.Offset(len(opdata)))
	return nil
}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	cs.chunkProtector.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkProtector.RUnlock()

	if !ok {
		return fmt.Errorf("Chunk Number: %v not found", args.Handle)
	}

	chunk.RLock()
	defer chunk.RUnlock()
	data, err := cs.ReadChunk(args.Handle, 0, chunk.length)

	if err != nil {
		return err
	}

	err = util.Call(args.Address, "ChunkServer.RPCApplyCopy", gfs.ApplyCopyArg{Handle: args.Handle, Data: data, Version: chunk.newestVersion}, &gfs.ApplyCopyReply{})

	return err
}

// RPCSendCCopy is called by another replica
// rewrite the local version to given copy data
func (cs *ChunkServer) RPCApplyCopy(args gfs.ApplyCopyArg, reply *gfs.ApplyCopyReply) error {
	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()

	if _, ok := cs.chunk[args.Handle]; !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}

	if args.Version <= cs.chunk[args.Handle].version {
		return fmt.Errorf("version %v is outdated", args.Version)
	}

	err := cs.ApplyMutationOnChunk(args.Handle, &Mutation{mtype: gfs.MutationCopy, version: args.Version, data: args.Data, offset: 0})
	if err != nil {
		return err
	}

	chunk := cs.chunk[args.Handle]
	chunk.Lock()
	defer chunk.Unlock()

	chunk.version = args.Version
	chunk.newestVersion = args.Version
	chunk.length = gfs.Offset(len(args.Data))

	return nil
}

// AdjustVersion is called by master to check whether the chunkserver
// holds the latest version of the chunk.
func (cs *ChunkServer) RPCAdjustVersion(args gfs.AdjustChunkVersionArg, reply *gfs.AdjustChunkVersionReply) error {
	log.Info("Adjusting version for ", args.Handle)
	cs.chunkProtector.RLock()
	defer cs.chunkProtector.RUnlock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cs.chunk[args.Handle]; !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}

	chunk.Lock()
	defer chunk.Unlock()

	chunk.version++

	if chunk.version < args.Version {
		log.Warningf("Chunk %v version is outdated, current version %v, master version %v", args.Handle, chunk.version, args.Version)
		reply.Stale = true
	} else {
		reply.Stale = false
	}

	return nil
}
