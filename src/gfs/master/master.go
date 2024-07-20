package master

import (
	"fmt"
	"net"
	"net/rpc"
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
				return
			default:
			}
			conn, err := m.l.Accept()
			if err == nil {
				go func() {
					rpcs.ServeConn(conn)
					conn.Close()
				}()
			} else {
				log.Fatal("accept error:", err)
				log.Exit(1)
			}
		}
	}()

	// Background Task
	go func() {
		ticker := time.Tick(gfs.BackgroundInterval)
		for {
			select {
			case <-m.shutdown:
				return
			default:
			}
			<-ticker

			err := m.BackgroundActivity()
			if err != nil {
				log.Fatal("Background error ", err)
			}
		}
	}()

	log.Infof("Master is running now. addr = %v, root path = %v", address, serverRoot)

	return m
}

// Shutdown shuts down master
func (m *Master) Shutdown() {
	close(m.shutdown)
}

// BackgroundActivity does all the background activities:
// dead chunkserver handling, garbage collection, stale replica detection, etc
func (m *Master) BackgroundActivity() error {
	return nil
}

// RPCHeartbeat is called by chunkserver to let the master know that a chunkserver is alive.
// Lease extension request is included.
func (m *Master) RPCHeartbeat(args gfs.HeartbeatArg, reply *gfs.HeartbeatReply) error {
	m.csm.Heartbeat(args.Address)
	return nil
	// todo: lease extension
}

// RPCGetPrimaryAndSecondaries returns lease holder and secondaries of a chunk.
// If no one holds the lease currently, grant one.
func (m *Master) RPCGetPrimaryAndSecondaries(args gfs.GetPrimaryAndSecondariesArg, reply *gfs.GetPrimaryAndSecondariesReply) error {
	lease, err := m.cm.GetLeaseHolder(args.Handle)
	if err != nil {
		return err
	}

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

	chunk_file, ok := new_node.children[filename]

	if !ok {
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

		if err != nil {
			return err
		}

		var valid_addr []gfs.ServerAddress

		reply.Handle, valid_addr, err = m.cm.CreateChunk(args.Path, servers)

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
