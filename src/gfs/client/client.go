package client

import (
	"fmt"
	"gfs"
	"gfs/util"
)

// Client struct is the GFS client-side driver
type Client struct {
	master gfs.ServerAddress
}

// NewClient returns a new gfs client.
func NewClient(master gfs.ServerAddress) *Client {
	return &Client{
		master: master,
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
	return 0, nil;
}

// Write writes data to the file at specific offset.
func (c *Client) Write(path gfs.Path, offset gfs.Offset, data []byte) error {
	return nil;
}

// Append appends data to the file. Offset of the beginning of appended data is returned.
func (c *Client) Append(path gfs.Path, data []byte) (offset gfs.Offset, err error) {
	return offset, nil;
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
		return 0, err;
	}

	if len(data) + int(offset) > gfs.MaxChunkSize {
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
		return 0, err;
	}
	
	return len(r.Data), nil;

}

// WriteChunk writes data to the chunk at specific offset.
// len(data)+offset should be within chunk size.
func (c *Client) WriteChunk(handle gfs.ChunkHandle, offset gfs.Offset, data []byte) error {
	if len(data) + int(offset) > gfs.MaxChunkSize { // len(data) + offset should be within chunk size
		return fmt.Errorf("WriteChunk: write exceeds chunk size")
	}

	
}

// AppendChunk appends data to a chunk.
// Chunk offset of the start of data will be returned if success.
// len(data) should be within max append size.
func (c *Client) AppendChunk(handle gfs.ChunkHandle, data []byte) (offset gfs.Offset, err error) {
	return offset, nil;
}
