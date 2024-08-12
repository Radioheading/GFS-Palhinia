package client

import (
	"fmt"
	"gfs"
	"gfs/util"
	"io"
	"time"

	log "github.com/sirupsen/logrus"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
	buffer *LeaseBuffer
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master: master,
		buffer: newLeaseBuffer(master, gfs.LeaseBufferTick),
	}
}

// Create creates a new file on the specific path on GFS.
func (c *Client) Create(path gfs.Path) error {
	return util.Call(c.master, "Master.RPCCreateFile", gfs.CreateFileArg{Path: path}, &gfs.CreateFileReply{})
}

// Mkdir creates a new directory on GFS.
func (c *Client) Mkdir(path gfs.Path) error {
	return util.Call(c.master, "Master.RPCMkdir", gfs.MkdirArg{Path: path}, &gfs.MkdirReply{})
}

// List lists everything in specific directory on GFS.
func (c *Client) List(path gfs.Path) ([]gfs.PathInfo, error) {
	reply := &gfs.ListReply{}
	err := util.Call(c.master, "Master.RPCList", gfs.ListArg{Path: path}, reply)
	return reply.Files, err
}

// Read reads the file at specific offset.
// It reads up to len(data) bytes form the File.
// It return the number of bytes, and an error if any.

func (c *Client) Read(path gfs.Path, offset gfs.Offset, data []byte) (n int, err error) {
	start := int(offset) / gfs.MaxChunkSize
	end := (int(offset) + len(data) - 1) / gfs.MaxChunkSize
	cur := start
	cur_read := 0
	log.Info("start: ", start, ", end: ", end)
	for cur <= end {
		var handle gfs.ChunkHandle
		handle, err = c.GetChunkHandle(path, gfs.ChunkIndex(cur))

		if err != nil {
			return 0, err
		}
		// read range [lhs, rhs]
		var lhs int
		var rhs int

		if cur == start {
			lhs = int(offset)
		} else {
			lhs = cur * gfs.MaxChunkSize
		}

		if cur == end {
			rhs = int(offset) + len(data)
		} else {
			rhs = (cur + 1) * gfs.MaxChunkSize
		}

		var data_read int
		var new_err error

		for { // read until we reach success
			data_read, new_err = c.ReadChunk(handle, gfs.Offset(lhs%gfs.MaxChunkSize), data[cur_read:cur_read+rhs-lhs])

			if new_err == nil {
				break
			}

			if e, ok := new_err.(gfs.Error); ok && e.Code == gfs.ReadEOF {
				err = io.EOF
				break
			}

			time.Sleep(gfs.RetryInterval)
			log.Info("retry read ", new_err)
		}

		cur++
		cur_read += data_read

		if err != nil {
			break
		}
	}

	if err != nil {
		return cur_read, io.EOF
	}

	return cur_read, err
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	start := int(offset) / gfs.MaxChunkSize
	end := (int(offset) + len(data) - 1) / gfs.MaxChunkSize
	cur := start
	cur_write := 0
	for cur <= end {
		var handle gfs.ChunkHandle
		var err error
		handle, err = c.GetChunkHandle(path, gfs.ChunkIndex(cur))

		if err != nil {
			return err
		}
		// read range [lhs, rhs]

		var lhs int
		var rhs int

		if cur == start {
			lhs = int(offset)
		} else {
			lhs = cur * gfs.MaxChunkSize
		}

		if cur == end {
			rhs = int(offset) + len(data)
		} else {
			rhs = (cur + 1) * gfs.MaxChunkSize
		}

		var new_err error

		for { // write until we reach success
			new_err = c.WriteChunk(handle, gfs.Offset(lhs%gfs.MaxChunkSize), data[cur_write:cur_write+rhs-lhs])
			// log.Info("&data written on chunk: ", handle, " is: ", string(data[cur_write:cur_write+rhs-lhs]))

			if new_err == nil {
				break
			}

			if e, ok := new_err.(gfs.Error); ok && e.Code == gfs.LeaseHasExpired {
				break
			}

			time.Sleep(gfs.RetryInterval)
			log.Info("retry write ", err)
		}

		if err != nil {
			return err
		}

		cur++
		cur_write += rhs - lhs
	}

	return nil
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	log.Info("append data: ", string(data))
	if len(data) > gfs.MaxAppendSize {
		return 0, fmt.Errorf("append: out of maxAppendSize")
	}

	var fileInfoReply gfs.GetFileInfoReply

	err = util.Call(c.master, "Master.RPCGetFileInfo", gfs.GetFileInfoArg{Path: path}, &fileInfoReply)

	if err != nil {
		log.Info("Error in getting chunk information of this file")
		return
	}

	var startIndex gfs.ChunkIndex
	var handle gfs.ChunkHandle
	var internalOffset gfs.Offset

	if fileInfoReply.Chunks > 0 {
		startIndex = gfs.ChunkIndex(fileInfoReply.Chunks - 1)
	} else {
		startIndex = 0
	}

	for {
		handle, err = c.GetChunkHandle(path, startIndex)

		if err != nil {
			return 0, err
		}

		for {
			internalOffset, err = c.AppendChunk(handle, data)

			if err == nil {
				break
			}

			if e, ok := err.(gfs.Error); ok && e.Code == gfs.AppendExceedChunkSize || e.Code == gfs.LeaseHasExpired {
				break
			}

			log.Warning("chunk ", handle, " failed at appending, retry again: ", err)
			time.Sleep(gfs.RetryInterval)
		}

		if err == nil {
			break // successful write in the chunk
		} else if e, ok := err.(gfs.Error); ok && e.Code != gfs.AppendExceedChunkSize {
			break
		}

		// retry at next chunk(we'll create one via getChunkHandle)
		startIndex++
	}

	if err != nil {
		return
	}

	offset = gfs.Offset(startIndex*gfs.MaxChunkSize + gfs.ChunkIndex(internalOffset))

	return offset, err
}

// GetChunkHandle returns the chunk handle of (path, index).
// If the chunk doesn't exist, master will create one.
func (c *Client) GetChunkHandle(path gfs.Path, index gfs.ChunkIndex) (gfs.ChunkHandle, error) {
	reply := &gfs.GetChunkHandleReply{}
	err := util.Call(c.master, "Master.RPCGetChunkHandle", gfs.GetChunkHandleArg{Path: path, Index: index}, reply)
	return reply.Handle, err
}

func (c *Client) GetChunkReplicas(handle gfs.ChunkHandle) ([]gfs.ServerAddress, error) {
	reply := &gfs.GetReplicasReply{}
	err := util.Call(c.master, "Master.RPCGetReplicas", gfs.GetReplicasArg{Handle: handle}, reply)
	return reply.Locations, err
}

// ReadChunk reads data from the chunk at specific offset.
// len(data)+offset  should be within chunk size.
func (c *Client) ReadChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) (int, error) {
	location, err := c.GetChunkReplicas(handle)
	if err != nil {
		return 0, err
	}

	if len(data)+int(offset) > gfs.MaxChunkSize {
		return 0, fmt.Errorf("ReadChunk: read exceeds chunk size")
	}

	if len(location) == 0 {
		return 0, fmt.Errorf("ReadChunk: no replica available")
	}

	index := 0

	r := &gfs.ReadChunkReply{Data: data}
	err = util.Call(location[index], "ChunkServer.RPCReadChunk", gfs.ReadChunkArg{Handle: handle, Offset: offset, Length: int(len(data))}, &r)

	if r.ErrorCode == gfs.ReadEOF {
		return len(r.Data), gfs.Error{Code: gfs.ReadEOF, Err: "ReadChunk: read EOF"}
	}

	if err != nil {
		return 0, err
	}

	return len(r.Data), nil

}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	if len(data)+int(offset) > gfs.MaxChunkSize { // len(data) + offset should be within chunk size
		return fmt.Errorf("WriteChunk: write exceeds chunk size")
	}

	// log.Info("write chunk; handle: ", handle, " offset: ", offset, " data: ", data)

	leaseBuf, err := c.buffer.GetLease(handle)

	if err != nil {
		return err
	}

	chain := append(leaseBuf.Secondaries, leaseBuf.Primary)

	// PushDataAndForward, we send the data to memory buffer and send it to all secondaries
	pushDataReply := &gfs.PushDataAndForwardReply{}

	err = util.Call(chain[0], "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: chain}, pushDataReply)
	if err != nil {
		return err
	}

	// write to primary
	r := &gfs.WriteChunkReply{}
	args := gfs.WriteChunkArg{DataID: pushDataReply.DataID, Offset: offset, Secondaries: leaseBuf.Secondaries}

	log.Info("going to write to primary: ", leaseBuf.Primary)

	err = util.Call(leaseBuf.Primary, "ChunkServer.RPCWriteChunk", args, r)

	if err != nil {
		return err
	}

	if r.ErrorCode == gfs.WriteExceedChunkSize {
		return gfs.Error{Code: r.ErrorCode, Err: "ExceedChunkSize!"}
	}

	if r.ErrorCode == gfs.LeaseHasExpired {
		return gfs.Error{Code: r.ErrorCode, Err: "Lease Expiration in WriteChunk"}
	}

	log.Info("done writing!!!")

	return nil
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	if len(data) >= gfs.MaxAppendSize {
		return 0, fmt.Errorf("AppendChunk: Out of Size")
	}

	leaseBuf, err := c.buffer.GetLease(handle)

	if err != nil {
		return 0, err
	}

	chain := append(leaseBuf.Secondaries, leaseBuf.Primary)

	// PushDataAndForward, we send the data to memory buffer and send it to all secondaries
	pushDataReply := &gfs.PushDataAndForwardReply{}

	err = util.Call(chain[0], "ChunkServer.RPCPushDataAndForward", gfs.PushDataAndForwardArg{Handle: handle, Data: data, ForwardTo: chain}, pushDataReply)
	if err != nil {
		return 0, err
	}

	log.Info("Begin to append to chunk: ", handle)

	// append to chunks
	args := gfs.AppendChunkArg{DataID: pushDataReply.DataID, Secondaries: leaseBuf.Secondaries}
	r := &gfs.AppendChunkReply{}

	err = util.Call(leaseBuf.Primary, "ChunkServer.RPCAppendChunk", args, r)

	if err != nil {
		return r.Offset, gfs.Error{Code: r.ErrorCode, Err: "Append Failed!"}
	}

	if r.ErrorCode == gfs.AppendExceedChunkSize {
		return r.Offset, gfs.Error{Code: r.ErrorCode, Err: "AppendExceedChunkSize!"}
	}

	if r.ErrorCode == gfs.LeaseHasExpired {
		return r.Offset, gfs.Error{Code: r.ErrorCode, Err: "Append Failed Because Lease Has Expired"}
	}

	return r.Offset, nil

}
