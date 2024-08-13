package client

import (
	"gfs"
	"gfs/util"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type LeaseBuffer struct {
	sync.RWMutex
	master gfs.ServerAddress
	buffer map[gfs.ChunkHandle]*gfs.Lease
	tick   time.Duration
}

func newLeaseBuffer(master gfs.ServerAddress, tick time.Duration) *LeaseBuffer {
	buf := &LeaseBuffer{
		master: master,
		buffer: make(map[gfs.ChunkHandle]*gfs.Lease),
		tick:   tick,
	}

	// lease buffer will expire after a period of time, so we need to periodically check the lease buffer
	// and delete the lease if necessary
	go func() {
		for {
			time.Sleep(tick)
			buf.Lock()
			for handle, lease := range buf.buffer {
				if lease.Expire.Before(time.Now()) {
					// use color blue to show log
					log.Printf("\033[34mClient\033[0m: Lease of chunk %v expired", handle)
					delete(buf.buffer, handle)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *LeaseBuffer) GetLease(handle gfs.ChunkHandle) (*gfs.Lease, error) {
	log.Info("%%praying")
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]
	if ok {
		return lease, nil
	}

	// if lease not found in buffer, we need to fetch it from master
	var getPrimaryAndSecondariesReply gfs.GetPrimaryAndSecondariesReply
	var err error

	log.Info("get lease buffer failed, try RPCGetPrimaryAndSecondaries: ", handle)

	if err = util.Call(buf.master, "Master.RPCGetPrimaryAndSecondaries", gfs.GetPrimaryAndSecondariesArg{Handle: handle}, &getPrimaryAndSecondariesReply); err != nil {
		return nil, err
	}

	log.Info("RPCGetPrimaryAndSecondaries: ", handle, " ", getPrimaryAndSecondariesReply.Primary, " ", getPrimaryAndSecondariesReply.Expire, " ", getPrimaryAndSecondariesReply.Secondaries)

	lease = &gfs.Lease{Primary: getPrimaryAndSecondariesReply.Primary, Expire: getPrimaryAndSecondariesReply.Expire, Secondaries: getPrimaryAndSecondariesReply.Secondaries}
	buf.buffer[handle] = lease
	return lease, nil
}
