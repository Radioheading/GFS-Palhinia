package master

import (
	"fmt"
	"gfs"
	"gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	replicasWaitlist []gfs.ChunkHandle
	numChunkHandle   gfs.ChunkHandle
}

// what data should we persist?
// for chunkInfo, we need to store version, path
// for fileInfo, we need to store all of its handles
// actually, we don't need to store location, as we will
// ping each address when master restarts

type persistChunkManager struct {
	Versions    []gfs.ChunkVersion
	Handles     []gfs.ChunkHandle
	Paths       []gfs.Path
	FileHandles [][]gfs.ChunkHandle
	NumHandles  gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location       util.ArraySet     // set of replica locations
	primary        gfs.ServerAddress // primary chunkserver
	expire         time.Time         // lease expire time
	path           gfs.Path
	version        gfs.ChunkVersion
	referenceCount int
}

type fileInfo struct {
	handles []gfs.ChunkHandle
}

type lease struct {
	primary     gfs.ServerAddress
	expire      time.Time
	secondaries []gfs.ServerAddress
}

func newChunkManager() *chunkManager {
	cm := &chunkManager{
		chunk: make(map[gfs.ChunkHandle]*chunkInfo),
		file:  make(map[gfs.Path]*fileInfo),
	}
	return cm
}

// Persist persists the chunk manager to disk
func (cm *chunkManager) Persist() persistChunkManager {
	cm.RLock()
	defer cm.RUnlock()

	var ret persistChunkManager

	for handle, info := range cm.chunk {
		info.RLock()
		ret.Versions = append(ret.Versions, info.version)
		info.RUnlock()
		ret.Handles = append(ret.Handles, handle)
	}

	for path, info := range cm.file {
		ret.Paths = append(ret.Paths, path)
		ret.FileHandles = append(ret.FileHandles, info.handles)
	}

	ret.NumHandles = cm.numChunkHandle

	return ret
}

func (cm *chunkManager) antiPersist(pcm persistChunkManager) {
	cm.Lock()
	defer cm.Unlock()

	for i, handle := range pcm.Handles {
		info := &chunkInfo{
			location: util.ArraySet{},
			primary:  "",
			expire:   time.Now(),
			path:     "",
			version:  pcm.Versions[i],
		}

		cm.chunk[handle] = info
	}

	for i, path := range pcm.Paths {
		info := &fileInfo{
			handles: pcm.FileHandles[i],
		}

		cm.file[path] = info
	}

	cm.numChunkHandle = pcm.NumHandles
}

// RegisterReplica adds a replica for a chunk
func (cm *chunkManager) RegisterReplica(handle gfs.ChunkHandle, addr gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.chunk[handle]

	if !ok {
		return fmt.Errorf("chunk %d not found", handle)
	}

	value.Lock()
	defer value.Unlock()

	value.location.Add(addr)

	return nil
}

// GetReplicas returns the replicas of a chunk
func (cm *chunkManager) GetReplicas(handle gfs.ChunkHandle) (*util.ArraySet, error) {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.chunk[handle]

	if !ok {
		return nil, fmt.Errorf("chunk %d not found", handle)
	}

	return &value.location, nil
}

// GetChunk returns the chunk handle for (path, index).
func (cm *chunkManager) GetChunk(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.file[path]

	if !ok {
		return 0, fmt.Errorf("file %s not found", path)
	}

	if index < 0 || index >= gfs.ChunkIndex(len(value.handles)) {
		return 0, fmt.Errorf("index %d out of bound", index)
	}

	return value.handles[index], nil
}

// GetLeaseHolder returns the chunkserver that hold the lease of a chunk
// (i.e. primary) and expire time of the lease. If no one has a lease,
// grants one to a replica it chooses.
// note: we detect a chunkserver having obsolete version or fail to connect,
// we add it to waitlist for garbage collection.
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error, []gfs.ServerAddress) {
	log.Info("get lease holder for chunk ", handle)

	cm.RLock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cm.chunk[handle]; !ok {
		cm.RUnlock()
		return nil, fmt.Errorf("chunk %d not found", handle), nil
	}

	cm.RUnlock()

	chunk.Lock()
	defer chunk.Unlock()
	var staleServers []gfs.ServerAddress

	if chunk.expire.After(time.Now()) {
		var ret lease
		ret.primary = chunk.primary
		ret.expire = chunk.expire

		for _, addr := range chunk.location.GetAll() {
			if addr != chunk.primary {
				if serverAddr, ok := addr.(gfs.ServerAddress); ok {
					ret.secondaries = append(ret.secondaries, serverAddr)
				} else {
					fmt.Println("Failed to convert addr to gfs.ServerAddress")
				}
			}
		}

		return &ret, nil, staleServers

	} else { // grant a new lease
		log.Info("obsolete lease for chunk ", handle, " version ", chunk.version, "; grant a new lease")
		var ret lease
		chunk.version++

		var serverAddr []gfs.ServerAddress
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(chunk.location.GetAll()))
		var addrLock sync.Mutex

		for _, inter_addr := range chunk.location.GetAll() {
			var addr gfs.ServerAddress
			if serverAddr, ok := inter_addr.(gfs.ServerAddress); ok {
				addr = serverAddr
			} else {
				fmt.Println("Failed to convert addr to gfs.ServerAddress")
			}

			go func(a gfs.ServerAddress) {
				log.Info("adjust chunk version for ", handle, " at ", a)
				var r gfs.AdjustChunkVersionReply

				err := util.Call(a, "ChunkServer.RPCAdjustVersion", gfs.AdjustChunkVersionArg{Handle: handle, Version: chunk.version}, &r)

				if err == nil && !r.Stale {
					addrLock.Lock()
					serverAddr = append(serverAddr, a)
					addrLock.Unlock()
				} else {
					log.Info("stale server ", a, " for chunk ", handle)
					staleServers = append(staleServers, a)
				}

				waitGroup.Done()
			}(addr)
		}

		waitGroup.Wait()

		chunk.location = util.ArraySet{}

		for _, addr := range serverAddr {
			chunk.location.Add(addr)
		}

		if len(serverAddr) < gfs.MinimumNumReplicas {
			cm.Lock()
			cm.replicasWaitlist = append(cm.replicasWaitlist, handle)
			cm.Unlock()
		}

		chunk.primary = chunk.location.RandomPick().(gfs.ServerAddress)
		chunk.expire = time.Now().Add(gfs.LeaseExpire)

		ret.primary = chunk.primary
		ret.expire = chunk.expire

		cm.DeployCopyOnWrite(chunk, handle)

		for _, addr := range chunk.location.GetAll() {
			if addr != chunk.primary {
				if serverAddr, ok := addr.(gfs.ServerAddress); ok {
					ret.secondaries = append(ret.secondaries, serverAddr)
				} else {
					fmt.Println("Failed to convert addr to gfs.ServerAddress")
				}
			}
		}

		return &ret, nil, staleServers
	}
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.chunk[handle]

	if !ok {
		return fmt.Errorf("chunk %d not found", handle)
	}

	value.Lock()
	defer value.Unlock()

	if value.primary != primary || value.expire.After(time.Now()) {
		return fmt.Errorf("primary mismatch")
	}

	value.expire = time.Now().Add(gfs.LeaseExpire)

	return nil
}

// CreateChunk creates a new chunk for path.
// here, if some of the addresses are not available, we would append it to the waitlist.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, []gfs.ServerAddress, error) {
	log.Info("enter create chunk for ", path)
	cm.Lock()
	defer cm.Unlock()

	log.Info("create chunk for ", path)

	file_info, ok := cm.file[path]

	if !ok {
		file_info = new(fileInfo)
		file_info.handles = make([]gfs.ChunkHandle, 0)
		cm.file[path] = file_info
	}

	handle := cm.numChunkHandle
	cm.numChunkHandle++

	file_info.handles = append(file_info.handles, handle)

	cm.chunk[handle] = &chunkInfo{path: path}

	errlist := make([]error, 0)
	validAddrs := make([]gfs.ServerAddress, 0)

	for _, addr := range addrs {
		err := util.Call(addr, "ChunkServer.RPCCreateChunk", gfs.CreateChunkArg{Handle: handle}, &gfs.CreateChunkReply{})
		if err != nil {
			errlist = append(errlist, err)
		} else {
			cm.chunk[handle].location.Add(addr)
			validAddrs = append(validAddrs, addr)
		}
	}

	if len(errlist) > 0 {
		cm.replicasWaitlist = append(cm.replicasWaitlist, handle)
	}

	return handle, validAddrs, nil
}

// HireChunkServer hires a chunkserver to store a chunk.
// we guarantee the chunkInfo's writeLock is held by the caller.
func (cm *chunkManager) HireChunkServer(args gfs.HireChunkServerArg) error {
	chunkInfo, ok := cm.chunk[args.Handle]

	if !ok {
		return fmt.Errorf("chunk %d not found", args.Handle)
	}

	chunkInfo.location.Add(args.Address)
	return nil
}

// GetObsoleteChunks returns the chunks that are obsolete.
func (cm *chunkManager) RemoveObsoleteAddresses(handles []gfs.ChunkHandle, address gfs.ServerAddress) error {
	cm.Lock()
	defer cm.Unlock()

	for _, handle := range handles {
		if chunkInfo, ok := cm.chunk[handle]; ok {
			chunkInfo.Lock()
			chunkInfo.location.Delete(address)
			chunkInfo.expire = time.Now()
			if chunkInfo.location.Size() < gfs.MinimumNumReplicas {
				cm.replicasWaitlist = append(cm.replicasWaitlist, handle)
			}

			// if chunkInfo.location.Size() == 0 {
			// 	return fmt.Errorf("chunk %d has no replica", handle)
			// }

			chunkInfo.Unlock()
		}
	}

	return nil
}

// GetUnderReplicatedChunks returns the chunks that are under-replicated.
func (cm *chunkManager) GetUnderReplicatedChunks() []gfs.ChunkHandle {
	cm.Lock()
	defer cm.Unlock()

	var ret []gfs.ChunkHandle

	var need_map = make(map[gfs.ChunkHandle]bool)

	for _, handle := range cm.replicasWaitlist {
		if _, ok := need_map[handle]; !ok {
			if chunkInfo, ok := cm.chunk[handle]; ok {
				chunkInfo.RLock()
				if chunkInfo.location.Size() < gfs.MinimumNumReplicas {
					ret = append(ret, handle)
					need_map[handle] = true
					log.Info("&add needed chunk ", handle)
				}
				chunkInfo.RUnlock()
			}
		}
	}

	cm.replicasWaitlist = make([]gfs.ChunkHandle, 0)

	return ret
}

// AddReferenceCount adds reference count for a primary chunkserver
// so as to enable snapshot.
func (cm *chunkManager) GetChunkInfo(handle gfs.ChunkHandle) (*chunkInfo, error) {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.chunk[handle]

	if !ok {
		return nil, fmt.Errorf("chunk %d not found", handle)
	}

	return value, nil
}

func (cm *chunkManager) AddReferenceCount(handle gfs.ChunkHandle) error {
	cm.RLock()
	defer cm.RUnlock()

	value, ok := cm.chunk[handle]

	if !ok {
		return fmt.Errorf("chunk %d not found", handle)
	}

	value.referenceCount++

	return nil
}

// DeployCopyOnWrite deploys copy-on-write for a chunk.
func (cm *chunkManager) DeployCopyOnWrite(myChunkInfo *chunkInfo, oldHandle gfs.ChunkHandle) error {
	if myChunkInfo.referenceCount == 0 {
		return nil
	}

	cm.Lock()
	cm.numChunkHandle++
	nextHandle := cm.numChunkHandle
	var arrayNewLocation []gfs.ServerAddress
	for _, addr := range myChunkInfo.location.GetAll() {
		arrayNewLocation = append(arrayNewLocation, addr.(gfs.ServerAddress))
	}
	cm.chunk[nextHandle] = &chunkInfo{
		location: util.ArraySet{},
		primary:  myChunkInfo.primary,
		expire:   time.Now(),
		version:  myChunkInfo.version,
	}
	for _, addr := range arrayNewLocation {
		cm.chunk[nextHandle].location.Add(addr)
	}

	cm.Unlock()
	err := util.CallAll(arrayNewLocation, "ChunkServer.RPCCopyOnWrite", gfs.CopyOnWriteArg{SrcHandle: oldHandle, DestHandle: nextHandle})
	if err != nil {
		return err
	}
	myChunkInfo.referenceCount--
	return nil
}
