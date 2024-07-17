package master

import (
	"fmt"
	"gfs"
	"gfs/util"
	"sync"
	"time"
)

// chunkManager manges chunks
type chunkManager struct {
	sync.RWMutex

	chunk map[gfs.ChunkHandle]*chunkInfo
	file  map[gfs.Path]*fileInfo

	numChunkHandle gfs.ChunkHandle
}

type chunkInfo struct {
	sync.RWMutex
	location util.ArraySet     // set of replica locations
	primary  gfs.ServerAddress // primary chunkserver
	expire   time.Time         // lease expire time
	path     gfs.Path
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

	value.RLock()
	defer value.RUnlock()

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
func (cm *chunkManager) GetLeaseHolder(handle gfs.ChunkHandle) ([]gfs.ServerAddress, *lease, error) {
	cm.RLock()
	defer cm.RUnlock()
	
}

// ExtendLease extends the lease of chunk if the lease holder is primary.
func (cm *chunkManager) ExtendLease(handle gfs.ChunkHandle, primary gfs.ServerAddress) error {
	return nil
}

// CreateChunk creates a new chunk for path.
func (cm *chunkManager) CreateChunk(path gfs.Path, addrs []gfs.ServerAddress) (gfs.ChunkHandle, error) {
	return 0, nil
}
