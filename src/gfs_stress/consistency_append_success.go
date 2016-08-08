package gfs_stress

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"fmt"
	"gfs"
	"io"
	"log"
	"math/rand"
	"reflect"
)

type ConsistencyAppendSuccess struct {
	ConsistencyAppendSuccess_GetConfigReply
	maxOffset   gfs.Offset
	checkChunk  []int
	checkpoints []ConsistencyAppendSuccess_CheckPoint
}

type ConsistencyAppendSuccess_CheckPoint struct {
	ID    string
	Count int
}

type ConsistencyAppendSuccess_ReportOffsetArg struct {
	ID     string
	Offset gfs.Offset
}

type ConsistencyAppendSuccess_ReportCheckArg struct {
	ID    string
	Found []ConsistencyAppendSuccess_CheckPoint
}

type ConsistencyAppendSuccess_Data struct {
	ID       string
	Count    int
	Data     []byte
	Checksum []byte
}

type ConsistencyAppendSuccess_GetConfigReply struct {
	FilePath      string
	MaxSize       int
	Count         int
	InitializerID string
}

func (t *ConsistencyAppendSuccess) initRemoteFile() error {
	log.Println("initRemoteFile")
	if err := c.Create(gfs.Path(t.FilePath)); err != nil {
		return err
	}
	return nil
}

func (t *ConsistencyAppendSuccess) append() error {
	log.Println("append")
	var buf bytes.Buffer
	h := md5.New()
	data := ConsistencyAppendSuccess_Data{
		ID:   conf.ID,
		Data: make([]byte, 0, t.MaxSize),
	}
	for i := 0; i < t.Count; i++ {
		log.Println("i=", i)

		data.Count = i
		size := rand.Intn(t.MaxSize)
		data.Data = data.Data[:size]
		for i := 0; i < size; i++ {
			data.Data[i] = byte(rand.Int())
		}
		data.Checksum = h.Sum(data.Data)

		buf.Reset()
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data); err != nil {
			return err
		}
		l := buf.Len()
		data := []byte{0x20, 0x16, 0x08, 0x08,
			byte((l >> 24) & 0xFF), byte((l >> 16) & 0xFF), byte((l >> 8) & 0xFF), byte(l & 0xFF)}
		data = append(data, buf.Bytes()...)

		offset, err := c.Append(gfs.Path(t.FilePath), data)
		if err != nil {
			return err
		}
		t.maxOffset = offset
	}
	return nil
}

func (t *ConsistencyAppendSuccess) check() error {
	log.Println("check")
	r := make([]byte, 0, gfs.MaxChunkSize)
	h := md5.New()
	for _, idx := range t.checkChunk {
		offset := idx * gfs.MaxChunkSize
		n, err := c.Read(gfs.Path(t.FilePath), gfs.Offset(offset), r[:cap(r)])
		r = r[:n]
		if err != nil && err != io.EOF {
			return err
		}

		p := 0
		for p+7 < n {
			if !(r[p] == 0x20 && r[p+1] == 0x16 && r[p+2] == 0x08 && r[p+3] == 0x08) {
				p++
				continue
			}
			len := (int(r[p+4]) << 24) | (int(r[p+5]) << 16) | (int(r[p+6]) << 8) | int(r[p+7])
			if p+7+len >= n {
				p++
				continue
			}
			data := r[p+8 : p+8+len]
			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			var d ConsistencyAppendSuccess_Data
			if err := dec.Decode(&d); err != nil {
				return err
			}
			checksum := h.Sum(d.Data)
			if !reflect.DeepEqual(checksum, d.Checksum) {
				return fmt.Errorf("checksum differs")
			}
			t.checkpoints = append(t.checkpoints, ConsistencyAppendSuccess_CheckPoint{d.ID, d.Count})
			p += len + 8
		}
	}
	return nil
}

func (t *ConsistencyAppendSuccess) run() error {
	waitMessage("ConsistencyAppendSuccess:GetConfig")
	var r1 ConsistencyAppendSuccess_GetConfigReply
	call(conf.Center, "ConsistencyAppendSuccess.GetConfig", struct{}{}, &r1)
	t.ConsistencyAppendSuccess_GetConfigReply = r1
	if r1.InitializerID == conf.ID {
		if err := t.initRemoteFile(); err != nil {
			return err
		}
	}
	sendAck()

	waitMessage("ConsistencyAppendSuccess:Append")
	if err := t.append(); err != nil {
		return err
	}
	arg1 := ConsistencyAppendSuccess_ReportOffsetArg{conf.ID, t.maxOffset}
	call(conf.Center, "ConsistencyAppendSuccess.ReportOffset", arg1, nil)

	waitMessage("ConsistencyAppendSuccess:GetCheckChunk")
	call(conf.Center, "ConsistencyAppendSuccess.GetCheckChunk", conf.ID, &t.checkChunk)

	waitMessage("ConsistencyAppendSuccess:Check")
	if err := t.check(); err != nil {
		return err
	}
	arg2 := ConsistencyAppendSuccess_ReportCheckArg{conf.ID, t.checkpoints}
	call(conf.Center, "ConsistencyAppendSuccess.ReportCheck", arg2, nil)
	return nil
}

func runConsistencyAppendSuccess() {
	log.Println("========== Test: ConsistencyAppendSuccess")
	t := new(ConsistencyAppendSuccess)
	err := t.run()
	if err != nil {
		log.Println("Error:", err)
		call(conf.Center, "RPC.ReportFailure", RPCStringMessage{conf.ID, err.Error()}, nil)
	}
}
