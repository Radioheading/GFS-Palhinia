package client

import (
	"gfs"
	"sync"
	"time"
)

type LeaseBuffer struct {
	sync.RWMutex
	master gfs.ServerAddress
	buffer map[gfs.ChunkHandle]gfs.Lease
	tick  time.Duration
}

func newLeaseBuffer(master gfs.ServerAddress, tick time.Duration) *LeaseBuffer {
	buf := &LeaseBuffer{
		master: master,
		buffer: make(map[gfs.ChunkHandle]gfs.Lease),
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
					delete(buf.buffer, handle)
				}
			}
			buf.Unlock()
		}
	}()

	return buf
}

func (buf *LeaseBuffer) GetLease(handle gfs.ChunkHandle) (gfs.Lease, error) {
	buf.Lock()
	defer buf.Unlock()
	lease, ok := buf.buffer[handle]
	if ok {
		return lease, nil
	}

	// if lease not found in buffer, we need to fetch it from master
	var 
}