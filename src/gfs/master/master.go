package master

import (
	"encoding/gob"
	"fmt"
	"gfs/util"
	"net"
	"net/rpc"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"gfs"
)

// Master Server struct
type Master struct {
	address    gfs.ServerAddress // master server address
	serverRoot string            // path to metadata storage
	l          net.Listener
	shutdown   chan struct{}

	nm  *namespaceManager
	cm  *chunkManager
	csm *chunkServerManager
}

type PersistMaster struct {
	NamespaceInfo []persistNsTree
	ChunkInfo     persistChunkManager
}

// NewAndServe starts a master and returns the pointer to it.
func NewAndServe(address gfs.ServerAddress, serverRoot string) *Master {
	m := &Master{
		address:    address,
		serverRoot: serverRoot,
		shutdown:   make(chan struct{}),
	}

	m.nm = newNamespaceManager()
	m.cm = newChunkManager()
	m.csm = newChunkServerManager()
	m.masterRestoreMeta()

	rpcs := rpc.NewServer()
	rpcs.Register(m)
	l, e := net.Listen("tcp", string(m.address))
	if e != nil {
		log.Fatal("listen error:", e)
		log.Exit(1)
	}
	m.l = l

	// RPC Handler
	go func() {
		for {
			select {
			case <-m.shutdown:
				{
					print("close rpc channel\n")
					return
				}
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				// log.Fatal("accept error:", err)
				// log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.Tick(gfs.BackgroundInterval)
		persistTicker := time.Tick(gfs.MasterPersistTick)
		for {
			var err error

			select {
			case <-m.shutdown:
				return
			case <-persistTicker:
				err = m.masterPersistMeta()
			case <-ticker:
				err = m.BackgroundActivity()
			default:
			}

			if err != nil {
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

func (m *Master) masterPersistMeta() error {
	filename := m.serverRoot + "/master.meta"

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}

	defer file.Close()

	namespacePersist := m.nm.Persist()
	chunkPersist := m.cm.Persist()

	var persistMaster = &PersistMaster{
		NamespaceInfo: namespacePersist,
		ChunkInfo:     chunkPersist,
	}

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(persistMaster)

	if err != nil {
		return err
	}

	return nil
}

func (m *Master) masterRestoreMeta() error {
	filename := m.serverRoot + "/master.meta"

	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		return err
	}

	defer file.Close()

	var persistMaster PersistMaster
	decoder := gob.NewDecoder(file)

	err = decoder.Decode(&persistMaster)

	if err != nil {
		return err
	}

	m.nm.antiPersist(persistMaster.NamespaceInfo)
	m.cm.antiPersist(persistMaster.ChunkInfo)

	return nil
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	log.Warn("Master is shutting down")
	err := m.masterPersistMeta()

	if err != nil {
		log.Error("Persist master meta error: ", err)
	}

	print("close master channel\n")
	close(m.shutdown)
	m.l.Close()
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	log.Info("Master background activity...")
	log.Info("RemoveServers...")
	if err := m.RemoveServers(); err != nil {
		return err
	}
	log.Info("ReReplicateChunks...")
	if err := m.ReReplicateChunks(); err != nil {
		return err
	}

	return nil
}

// HireChunkServer is called by chunkserver to register itself to the master.
func (m *Master) HireChunkServer(args gfs.HireChunkServerArg, reply *gfs.HireChunkServerReply) error {
	err1 := m.cm.HireChunkServer(args)
	err2 := m.csm.AddChunk([]gfs.ServerAddress{args.Address}, args.Handle)

	if err1 != nil || err2 != nil {
		return fmt.Errorf("HireChunkServer error: %v %v", err1, err2)
	}

	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	var garbages = make([]gfs.ChunkHandle, 0)
	reboot := m.csm.Heartbeat(args.Address, garbages)
	reply.Garbages = garbages

	// for _, handle := range args.LeaseExtensions {
	// 	m.cm.ExtendLease(handle, args.Address)
	// }

	// note: if we reboots the master, we need to acknowledge each chunkserver
	if !reboot {
		return nil
	}

	log.Warning("Master reboots, chunkserver ", args.Address, " is acknowledged")

	var serverStatusReply gfs.GetServerStatusReply

	err := util.Call(args.Address, "ChunkServer.RPCGetServerStatus", gfs.GetServerStatusArg{}, &serverStatusReply)

	if err != nil {
		return err
	}

	for i, handle := range serverStatusReply.Chunks {
		m.cm.RLock()
		chunk, ok := m.cm.chunk[handle]
		nowVersion := chunk.version
		m.cm.RUnlock()

		log.Info("previous version on addr ", args.Address, " is: ", nowVersion)
		log.Info("now version on addr ", args.Address, " is: ", serverStatusReply.Versions[i])

		if !ok || nowVersion != serverStatusReply.Versions[i] {
			continue
		}
		// up-to-date chunk information
		chunk.Lock()
		defer chunk.Unlock()
		log.Info("up-to-date chunk information: ", handle, " ", args.Address)
		if (m.HireChunkServer(gfs.HireChunkServerArg{
			Address: args.Address,
			Handle:  handle,
		}, &gfs.HireChunkServerReply{}) != nil) {
			log.Warn("HireChunkServer error on address ", args.Address, " handle ", handle)
		}
	}

	return nil
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	lease, err, staleServers := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}

	m.csm.FillGarbage(staleServers, args.Handle)
	reply.Primary = lease.primary
	reply.Secondaries = lease.secondaries
	reply.Expire = lease.expire
	return nil
}

// RPCGetReplicas is called by client to find all chunkservers that hold the chunk.
func (m *Master) RPCGetReplicas(args gfs.GetReplicasArg, reply *gfs.GetReplicasReply) error {
	// todo: further check
	log.Infof("RPCGetReplicas %v", args.Handle)
	locations, err := m.cm.GetReplicas(args.Handle)

	if err != nil {
		return err
	}

	for _, location := range locations.GetAll() {
		if addr, ok := location.(gfs.ServerAddress); ok {
			reply.Locations = append(reply.Locations, addr)
		} else {
			return fmt.Errorf("invalid location %v", location)
		}
	}

	return err
}

// RPCCreateFile is called by client to create a new file
func (m *Master) RPCCreateFile(args gfs.CreateFileArg, replay *gfs.CreateFileReply) error {
	return m.nm.Create(args.Path)
}

// RPCMkdir is called by client to make a new directory
func (m *Master) RPCMkdir(args gfs.MkdirArg, replay *gfs.MkdirReply) error {
	return m.nm.Mkdir(args.Path)
}

// RPCGetFileInfo is called by client to get file information
// todo: GetFileInfo
func (m *Master) RPCGetFileInfo(args gfs.GetFileInfoArg, reply *gfs.GetFileInfoReply) error {
	return m.nm.GetFileInfo(args.Path, reply)
}

// RPCGetChunkHandle returns the chunk handle of (path, index).
// If the requested index is bigger than the number of chunks of this path by exactly one, create one.
func (m *Master) RPCGetChunkHandle(args gfs.GetChunkHandleArg, reply *gfs.GetChunkHandleReply) error {
	raw_path, filename := args.Path.ParseLeafname()
	paths := raw_path.GetPaths()

	log.Info("RPCGetChunkHandle: ", args.Path, " ", args.Index, ", filename: ", filename)

	new_node, err := m.nm.lockParents(paths, true)

	if err != nil {
		m.nm.unlockParents(paths, true)
		return err
	}

	defer m.nm.unlockParents(paths, true)

	log.Info("RPCGetChunkHandle: ", filename, " ", new_node)

	chunk_file, ok := new_node.children[filename]

	if !ok {
		log.Info("LYD")
		return fmt.Errorf("file %v does not exist", filename)
	}

	chunk_file.Lock() // remember to lock the file itself
	defer chunk_file.Unlock()

	if args.Index < 0 || args.Index > gfs.ChunkIndex(chunk_file.chunks) {
		return fmt.Errorf("index %d out of bound", args.Index)
	}

	if args.Index == gfs.ChunkIndex(chunk_file.chunks) {
		chunk_file.chunks++
		chunk_file.length += gfs.MaxChunkSize

		servers, err := m.csm.ChooseServers(gfs.DefaultNumReplicas)

		log.Info("done choose servers: ", servers)

		if err != nil {
			log.Info("ChooseServers error: ", err)
			return err
		}

		var valid_addr []gfs.ServerAddress

		reply.Handle, valid_addr, err = m.cm.CreateChunk(args.Path, servers)

		log.Info("CreateChunk: ", reply.Handle, " ", valid_addr)

		if err != nil {
			return err
		}

		// var waitGroup sync.WaitGroup
		// waitGroup.Add(len(servers))
		// var wg_lock sync.RWMutex
		// var errList []error

		// for _, server := range servers {
		// 	go func(addr gfs.ServerAddress) {
		// 		err := util.Call(server, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: reply.Handle}, &gfs.CreateChunkReply{})
		// 		if err != nil {
		// 			wg_lock.Lock()
		// 			errList = append(errList, err)
		// 			wg_lock.Unlock()
		// 		}

		// 		sv := make([]gfs.ServerAddress, 0)
		// 		sv = append(sv, server)

		// 		m.csm.AddChunk(sv, reply.Handle)
		// 	}(server)
		// }

		// waitGroup.Wait()

		m.csm.AddChunk(valid_addr, reply.Handle)
		return nil
	} else {
		log.Info("already exists, get handle: ", args.Index)
		reply.Handle, err = m.cm.GetChunk(args.Path, args.Index)
		return err
	}
}

// RPCList is called by client to list all files and directories under a path
func (m *Master) RPCList(args gfs.ListArg, reply *gfs.ListReply) error {
	ret, err := m.nm.List(args.Path)

	if err != nil {
		return err
	}

	reply.Files = ret

	return nil
}

// RemoveServers is called by master to remove obsolete servers from the system
func (m *Master) RemoveServers() error {
	obsoleteServers := m.csm.GetObsoleteServers()

	for _, server := range obsoleteServers {
		log.Info("$$RemoveServers: ", server)
		handles, err := m.csm.RemoveServer(server)

		if err != nil {
			return err
		}

		err = m.cm.RemoveObsoleteAddresses(handles, server)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReReplicateChunks is called by master to re-replicate chunks that are under-replicated
func (m *Master) ReReplicateChunks() error {
	handles := m.cm.GetUnderReplicatedChunks()

	m.cm.RLock()

	for _, handle := range handles {
		ck := m.cm.chunk[handle]
		log.Info("handle: ", handle, ", num of replicas: ", ck.location.Size(), ", expire time", ck.expire.Sub(time.Now()))
		if ck.expire.Before(time.Now()) {
			log.Info("ReReplicateChunks: ", handle)
			ck.Lock()

			from, to, err := m.csm.ChooseReReplication(handle)

			log.Info("$$ReReplicateChunks from ", from, " to ", to)

			if err != nil {
				ck.Unlock()
				log.Warning("ChooseReReplication error: ", err)
				continue
			}

			err = util.Call(to, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &gfs.CreateChunkReply{})
			if err != nil {
				return err
			}

			log.Warning("ReReplicateChunks: ", handle, " from ", from, " to ", to)

			err = util.Call(from, "ChunkServer.RPCSendCopy", gfs.SendCopyArg{Handle: handle, Address: to}, &gfs.SendCopyReply{})
			if err != nil {
				log.Info("SendCopy error: ", err)
				return err
			}

			err = m.cm.HireChunkServer(gfs.HireChunkServerArg{
				Address: to,
				Handle:  handle,
			})
			if err != nil {
				log.Warning("HireChunkServer error: ", err)
				return err
			}

			m.csm.AddChunk([]gfs.ServerAddress{to}, handle)

			ck.Unlock()
		}
	}

	m.cm.RUnlock()

	return nil
}
