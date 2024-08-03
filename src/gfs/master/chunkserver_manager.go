package master

///

import (
	// "fm发“
	"fmt"
	"gfs"
	"gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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
	// GFS devises lazy garbage collection mechanism, so we need to persist the chunk information
	garbageList []gfs.ChunkHandle
}

// fill ruubish
func (csm *chunkServerManager) FillGarbage(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		if server, ok := csm.servers[addr]; ok {
			server.garbageList = append(server.garbageList, handle)
		}
	}
	return nil
}

// Hearbeat marks the chunkserver alive for now.
func (csm *chunkServerManager) Heartbeat(addr gfs.ServerAddress, garbages []gfs.ChunkHandle) bool {
	csm.Lock()
	defer csm.Unlock()
	if server, ok := csm.servers[addr]; !ok {
		csm.servers[addr] = &chunkServerInfo{
			lastHeartbeat: time.Now(),
			chunks:        make(map[gfs.ChunkHandle]bool),
		}

		return true
	} else {
		server.lastHeartbeat = time.Now()
		garbages = append(server.garbageList, garbages...)
		csm.servers[addr].garbageList = make([]gfs.ChunkHandle, 0)
		return false
	}
}

// AddChunk creates a chunk on given chunkservers
func (csm *chunkServerManager) AddChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		if server, ok := csm.servers[addr]; ok {
			log.Info("Add chunk for addr: ", addr)

			var checker bool

			if checker, ok = server.chunks[handle]; !ok || !checker {
				server.chunks[handle] = true
			}
			if ok && checker {
				log.Warning("Chunk ", handle, " already exists on ", addr)
			}
		}
	}
	return nil
}

// RemoveChunk removes a chunk on given chunkservers
func (csm *chunkServerManager) RemoveChunk(addrs []gfs.ServerAddress, handle gfs.ChunkHandle) error {
	csm.Lock()
	defer csm.Unlock()
	for _, addr := range addrs {
		if server, ok := csm.servers[addr]; ok {
			if checker, ok := server.chunks[handle]; ok && checker {
				server.chunks[handle] = false
			}
			log.Warning("Chunk ", handle, " already exists on ", addr)
		}
	}
	return nil
}

// ChooseReReplication chooses servers to perform re-replication
// called when the replicas number of a chunk is less than gfs.MinimumNumReplicas
// returns two server address, the master will call 'from' to send a copy to 'to'
func (csm *chunkServerManager) ChooseReReplication(handle gfs.ChunkHandle) (from, to gfs.ServerAddress, err error) {
	csm.RLock()
	defer csm.RUnlock()

	from = ""
	to = ""
	err = nil

	done1 := false
	done2 := false

	for addr, server := range csm.servers {
		if checker, ok := server.chunks[handle]; ok && checker {
			from = addr
			done1 = true
			if done2 {
				return
			}
		} else {
			to = addr
			done2 = true
			if done1 {
				return
			}
		}
		if from != "" && to != "" {
			return
		}
	}
	err = fmt.Errorf("ChooseReReplication: no enough chunkservers")
	return
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
	csm.RLock()
	defer csm.RUnlock()

	deads := make([]gfs.ServerAddress, 0)
	for addr, server := range csm.servers {
		if server.lastHeartbeat.Add(gfs.ServerTimeout).Before(time.Now()) {
			deads = append(deads, addr)
		}
	}

	return deads
}

// RemoveServers removes metedata of a disconnected chunkserver.
// It returns the chunks that server holds
func (csm *chunkServerManager) RemoveServer(addr gfs.ServerAddress) (handles []gfs.ChunkHandle, err error) {
	csm.Lock()
	defer csm.Unlock()

	if server, ok := csm.servers[addr]; ok {
		for handle, checker := range server.chunks {
			if checker {
				handles = append(handles, handle)
			}
		}
	}

	err = fmt.Errorf("RemoveServer: no such server")
	return
}
