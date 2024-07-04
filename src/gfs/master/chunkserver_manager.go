package master

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"
	// log "github.com/sirupsen/logrus"
)

// chunkServerManager manages chunkservers
type chunkServerManager struct {
	sync.RWMutex
	servers map[gfs.ServerAddress]*chunkServerInfo
}

func newChunkServerManager() *chunkServerManager {
	csm := &chunkServerManager{
		servers: make(map[gfs.ServerAddress]*chunkServerInfo),
	}
	return csm
}

type chunkServerInfo struct {
	lastHeartbeat time.Time
	chunks        map[gfs.ChunkHandle]bool // set of chunks that the chunkserver has
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress) {
	csm.Lock()
	defer csm.Unlock()
	if _, ok := csm.servers[addr]; !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks:        make(map[gfs.ChunkHandle]bool),
		}
	} else {
		csm.servers[addr].lastHeartbeat = time.Now()
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	return nil
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	return "", "", nil
}

// ChooseServers returns servers to store new chunk.
// It is called when a new chunk is create
func (csm *chunkServerManager) ChooseServers(num int) ([]gfs.ServerAddress, error) {
	csm.RLock()
	defer csm.RUnlock()
	var addrs []gfs.ServerAddress
	for addr := range csm.servers {
		addrs = append(addrs, addr)
	}
	list, err := util.Sample(len(addrs), num)
	if err != nil {
		return nil, err
	}
	var res []gfs.ServerAddress
	for i := range list {
		res = append(res, addrs[i])
	}
	return res, nil

}

// DetectDeadServers detects disconnected chunkservers according to last heartbeat time
func (csm *chunkServerManager) DetectDeadServers() []gfs.ServerAddress {
	return nil
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	return nil, nil
}
