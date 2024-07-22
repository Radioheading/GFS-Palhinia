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

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
	version  gfs.ChunkVersion
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
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) (*lease, error) {
	log.Info("get lease holder for chunk ", handle)

	cm.RLock()

	var chunk *chunkInfo
	var ok bool

	if chunk, ok = cm.chunk[handle]; !ok {
		cm.RUnlock()
		return nil, fmt.Errorf("chunk %d not found", handle)
	}

	chunk.Lock()
	defer chunk.Unlock()

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

		return &ret, nil

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

		for _, addr := range chunk.location.GetAll() {
			if addr != chunk.primary {
				if serverAddr, ok := addr.(gfs.ServerAddress); ok {
					ret.secondaries = append(ret.secondaries, serverAddr)
				} else {
					fmt.Println("Failed to convert addr to gfs.ServerAddress")
				}
			}
		}

		return &ret, nil
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
	cm.Lock()
	defer cm.Unlock()

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
