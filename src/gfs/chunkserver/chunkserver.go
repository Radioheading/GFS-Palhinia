package chunkserver

import (
	"encoding/gob"
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
	// GFS devises lazy garbage collection mechanism, so we need to persist the chunk information
	garbageList []gfs.ChunkHandle
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
	broken        bool                           // whether the chunk is broken
	invalidated   bool                           // whether the chunk is invalidated
}

type persistChunkInfo struct {
	Handle        gfs.ChunkHandle
	Length        gfs.Offset
	Version       gfs.ChunkVersion
	NewestVersion gfs.ChunkVersion
}

func (cs *ChunkServer) persistChunkServer() {
	cs.chunkProtector.RLock()
	defer cs.chunkProtector.RUnlock()

	var persisted []persistChunkInfo
	log.Info("\033[32m persisting chunkserver \033[0m")

	for k, v := range cs.chunk {
		v.RLock()
		persisted = append(persisted, persistChunkInfo{
			Handle:        k,
			Length:        v.length,
			Version:       v.version,
			NewestVersion: v.newestVersion,
		})
		v.RUnlock()
	}
	filename := path.Join(cs.serverRoot, "/chunkserver")
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Warning("open file error: ", err)
		return
	}

	defer file.Close()

	encoder := gob.NewEncoder(file)
	log.Println("persisted: ", len(persisted))
	err = encoder.Encode(persisted)

	if err != nil {
		log.Fatal("encode error: ", err)
	}
}

func (cs *ChunkServer) restoreChunkServer() {
	filename := path.Join(cs.serverRoot, "/chunkserver")
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)

	if err != nil {
		log.Warning("open file error: ", err)
		return
	}

	defer file.Close()

	var persisted []persistChunkInfo
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&persisted)

	if err != nil {
		log.Fatal("decode error: ", err)
	}

	log.Println("antipersisted: ", len(persisted))

	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()

	for _, v := range persisted {
		cs.chunk[v.Handle] = &chunkInfo{
			length:        v.Length,
			version:       v.Version,
			newestVersion: v.NewestVersion,
			mutations:     make(map[gfs.ChunkVersion]*Mutation),
			invalidated:   false,
		}
	}

	log.Info("ChunkServer restored")
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

	cs.restoreChunkServer()

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
		persistTick := time.Tick(gfs.MasterPersistTick)
		garbageTick := time.Tick(gfs.GarbageCollectionTick)
		heartbeatTick := time.Tick(gfs.HeartbeatInterval)
		quickStart := make(chan struct{}, 1)
		quickStart <- struct{}{}
		for {
			select {
			case <-cs.shutdown:
				return
			case <-persistTick:
				cs.persistChunkServer()
			case <-garbageTick:
				// garbage collection
				for _, handle := range cs.garbageList {
					if cs.removeChunk(handle) != nil {
						log.Warning("remove chunk failed: ", handle)
					}
				}
				cs.garbageList = make([]gfs.ChunkHandle, 0)
			case <-quickStart:
				{
					log.Info("quick start")
					cs.heartbeatRoutine()
				}
			case <-heartbeatTick:
				cs.heartbeatRoutine()
			}
		}
	}()

	log.Infof("ChunkServer is now running. addr = %v, root path = %v, master addr = %v", addr, serverRoot, masterAddr)

	return cs
}

func (cs *ChunkServer) heartbeatRoutine() {
	pe := cs.pendingLeaseExtensions.GetAllAndClear()
	le := make([]gfs.ChunkHandle, len(pe))
	for i, v := range pe {
		le[i] = v.(gfs.ChunkHandle)
	}
	args := &gfs.HeartbeatArg{
		Address:         cs.address,
		LeaseExtensions: le,
	}
	reply := &gfs.HeartbeatReply{}
	if err := util.Call(cs.master, "Master.RPCHeartbeat", args, reply); err != nil {
		log.Warning("heartbeat rpc error ", err)
		// log.Exit(1)
	}

	cs.garbageList = append(cs.garbageList, reply.Garbages...)
}

// ApplyMutationOnChunk applies mutation to the chunk
func (cs *ChunkServer) ApplyMutationOnChunk(handle gfs.ChunkHandle, m *Mutation) error {
	my_path := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.OpenFile(my_path, os.O_WRONLY|os.O_CREATE, 0777)

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
		cs.persistChunkServer()
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
	cs.chunk[args.Handle] = &chunkInfo{length: 0, version: 0, newestVersion: 0, mutations: make(map[gfs.ChunkVersion]*Mutation), invalidated: false}
	return nil
}

// ReadChunk reads the chunk data from disk
func (cs *ChunkServer) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, length gfs.Offset) ([]byte, error) {
	cs.chunkProtector.RLock()
	if _, ok := cs.chunk[handle]; !ok {
		cs.chunkProtector.RUnlock()
		return nil, fmt.Errorf("chunk %v not found", handle)
	}

	cs.chunkProtector.RUnlock()
	path := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	f, err := os.Open(path)

	if err != nil { // if open failed, this is equal to EOF
		return nil, io.EOF
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
func (cs *ChunkServer) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (*chunkInfo, error) {
	cs.chunkProtector.RLock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cs.chunk[handle]; !ok {
		cs.chunkProtector.RUnlock()
		return nil, fmt.Errorf("chunk %v not found", handle)
	}

	cs.chunkProtector.RUnlock()
	chunk.Lock()
	// defer chunk.Unlock()
	err := cs.ApplyMutationOnChunk(handle, &Mutation{mtype: gfs.MutationWrite, version: chunk.version, data: data, offset: offset})

	if err != nil {
		chunk.broken = true
		chunk.Unlock()
		return nil, err
	}

	chunk.length = util.Max(chunk.length, offset+gfs.Offset(len(data)))
	chunk.version = chunk.newestVersion

	return chunk, nil
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

	cs.chunkProtector.Lock()
	var chunkInfo *chunkInfo
	chunkInfo, ok = cs.chunk[args.DataID.Handle]
	if !ok || chunkInfo.broken {
		cs.chunkProtector.Unlock()
		return fmt.Errorf("chunk %v not found", args.DataID.Handle)
	}

	if chunkInfo.invalidated {
		cs.chunkProtector.Unlock()
		reply.ErrorCode = gfs.LeaseHasExpired
		return nil
	}
	cs.chunkProtector.Unlock()

	chunk, err := cs.WriteChunk(args.DataID.Handle, args.Offset, op_data)

	if err != nil || chunk == nil {
		return err
	}

	defer chunk.Unlock()

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

	cs.chunkProtector.Lock()
	var myChunkInfo *chunkInfo
	myChunkInfo, ok = cs.chunk[args.DataID.Handle]
	if !ok || myChunkInfo.broken {
		cs.chunkProtector.Unlock()
		return fmt.Errorf("chunk %v not found", args.DataID.Handle)
	}

	if myChunkInfo.invalidated {
		reply.ErrorCode = gfs.LeaseHasExpired
		cs.chunkProtector.Unlock()
		return nil
	}
	cs.chunkProtector.Unlock()

	if gfs.Offset(len(op_data)) > gfs.MaxAppendSize {
		return fmt.Errorf("append size %v out of bound", len(op_data))
	}

	cs.chunkProtector.RLock()

	var chunk *chunkInfo

	if chunk, ok = cs.chunk[args.DataID.Handle]; !ok {
		cs.chunkProtector.RUnlock()
		return fmt.Errorf("chunk %v not found", args.DataID.Handle)
	}

	cs.chunkProtector.RUnlock()
	chunk.Lock()

	m := gfs.ApplyMutationArg{DataID: args.DataID, Offset: chunk.length}
	reply.Offset = chunk.length

	if chunk.length+gfs.Offset(len(op_data)) > gfs.MaxChunkSize { // need to pad the current chunk
		log.Warning("RPCAppend: exceed chunk size on chunkserver: ", cs.address)
		m.Mtype = gfs.MutationPad
		reply.ErrorCode = gfs.AppendExceedChunkSize
		op_data = make([]byte, gfs.MaxChunkSize-chunk.length)
	} else {
		m.Mtype = gfs.MutationAppend
		reply.ErrorCode = gfs.Success
	}
	chunk.Unlock()

	chunk, err := cs.WriteChunk(args.DataID.Handle, chunk.length, op_data)

	if err != nil || chunk == nil {
		return err
	}

	defer chunk.Unlock()

	if m.Mtype == gfs.MutationPad {
		chunk.length = gfs.MaxChunkSize
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

	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()

	if _, ok := cs.chunk[args.DataID.Handle]; !ok {
		return fmt.Errorf("chunk %v not found", args.Version)
	}

	err := cs.ApplyMutationOnChunk(args.DataID.Handle, &Mutation{mtype: args.Mtype, version: args.Version, data: opdata, offset: args.Offset})

	if err != nil {
		return err
	}
	cs.chunk[args.DataID.Handle].length = util.Max(cs.chunk[args.DataID.Handle].length, args.Offset+gfs.Offset(len(opdata)))
	return nil
}

// RPCSendCopy is called by master, send the whole copy to given address
func (cs *ChunkServer) RPCSendCopy(args gfs.SendCopyArg, reply *gfs.SendCopyReply) error {
	cs.chunkProtector.RLock()
	chunk, ok := cs.chunk[args.Handle]
	cs.chunkProtector.RUnlock()

	if !ok {
		return fmt.Errorf("chunk Number: %v not found", args.Handle)
	}

	chunk.RLock()
	defer chunk.RUnlock()
	data, err := cs.ReadChunk(args.Handle, 0, chunk.length)

	if err != nil {
		log.Warning("Read chunk failed: ", args.Handle)
		return nil
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
	cs.chunkProtector.RLock()
	defer cs.chunkProtector.RUnlock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cs.chunk[args.Handle]; !ok || chunk.broken {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}

	chunk.Lock()
	defer chunk.Unlock()

	chunk.version++
	chunk.newestVersion++

	if chunk.version < args.Version {
		log.Warningf("Chunk %v version is outdated, current version %v, master version %v", args.Handle, chunk.version, args.Version)
		reply.Stale = true
		chunk.broken = true
	} else {
		reply.Stale = false
	}

	return nil
}

// removeChunk removes the chunk from the chunkserver
func (cs *ChunkServer) removeChunk(handle gfs.ChunkHandle) error {
	cs.chunkProtector.Lock()
	defer cs.chunkProtector.Unlock()
	delete(cs.chunk, handle)

	filename := path.Join(cs.serverRoot, fmt.Sprintf("%v", handle))
	return os.Remove(filename)
}

// RPCGetServerStatus is called by master to check the status of the chunkserver for master information
// restore after each restart
func (cs *ChunkServer) RPCGetServerStatus(args gfs.GetServerStatusArg, reply *gfs.GetServerStatusReply) error {
	cs.chunkProtector.RLock()
	defer cs.chunkProtector.RUnlock()

	var chunks []gfs.ChunkHandle
	var versions []gfs.ChunkVersion
	for k, v := range cs.chunk {
		chunks = append(chunks, k)
		versions = append(versions, v.version)
	}

	reply.Chunks = chunks
	reply.Versions = versions

	return nil
}

// RPCInvalidateLease is called by master to invalidate the lease of a chunk during snapshots
func (cs *ChunkServer) RPCInvalidateLease(args gfs.InvalidateLeaseArg, reply *gfs.InvalidateLeaseReply) error {
	cs.chunkProtector.RLock()
	defer cs.chunkProtector.RUnlock()

	if _, ok := cs.chunk[args.Handle]; !ok {
		return fmt.Errorf("chunk %v not found", args.Handle)
	}

	cs.chunk[args.Handle].invalidated = true
	return nil
}

// RPCCopyOnWrite is called by master to copy all chunks from a single handle to a new one
func (cs *ChunkServer) RPCCopyOnWrite(args gfs.CopyOnWriteArg, reply *gfs.CopyOnWriteReply) error {
	cs.chunkProtector.RLock()
	chunk, ok := cs.chunk[args.SrcHandle]
	if !ok || chunk.broken {
		cs.chunkProtector.RUnlock()
		return fmt.Errorf("chunk %v not found", args.SrcHandle)
	}
	cs.chunkProtector.RUnlock()

	cs.chunk[args.DestHandle] = &chunkInfo{
		length:        chunk.length,
		version:       chunk.version,
		newestVersion: chunk.newestVersion,
		mutations:     make(map[gfs.ChunkVersion]*Mutation),
		invalidated:   false}

	var chunkData []byte
	var err error
	if chunkData, err = cs.ReadChunk(args.SrcHandle, 0, chunk.length); err != nil {
		log.Fatal(err)
		return err
	}
	if _, err = cs.WriteChunk(args.DestHandle, 0, chunkData); err != nil {
		cs.chunk[args.DestHandle].Unlock()
		log.Fatal(err)
		return err
	}

	cs.chunk[args.DestHandle].Unlock()
	return nil
}
