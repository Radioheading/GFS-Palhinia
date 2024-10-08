package gfs_test

import (
	"gfs"
	"gfs/chunkserver"
	"gfs/client"
	"gfs/master"
	"gfs/util"
	"math/rand"
	"reflect"

	"fmt"
	"io"
	"io/ioutil"

	log "github.com/sirupsen/logrus"

	//"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	m     *master.Master
	cs    []*chunkserver.ChunkServer
	c     *client.Client
	csAdd []gfs.ServerAddress
	root  string // root of tmp file path
)

const (
	mAdd  = ":7777"
	csNum = 16
	N     = 2
)

func errorAll(ch chan error, n int, t *testing.T) {
	for i := 0; i < n; i++ {
		if err := <-ch; err != nil {
			t.Error(err)
		}
	}
}

const ChunkSize = 1 << 24

func ConcurrentWriteFilesWithClients(numClients int) {
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			p := gfs.Path(fmt.Sprintf("/file_%d.txt", x))
			m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})

			data := make([]byte, ChunkSize)
			for j := 0; j < ChunkSize; j++ {
				data[j] = byte(x)
			}
			c.Write(p, gfs.Offset(0), data)

		}(i)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("aggregate time: %s\n", elapsed)
	fmt.Printf("average speed: %.2f MB/s\n", float64(numClients*ChunkSize)/1e6/elapsed.Seconds())

	file, err := os.OpenFile("write_speed_results.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	result := fmt.Sprintf("Clients: %d, Average Write Speed: %.2f MB/s\n", numClients, float64(numClients*ChunkSize)/1e6/elapsed.Seconds())
	if _, err := file.WriteString(result); err != nil {
		fmt.Println("Error writing to file:", err)
	}
}

func ConcurrentReadFilesWithClients(numClients int) {
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			p := gfs.Path(fmt.Sprintf("/file_%d.txt", x))

			data := make([]byte, ChunkSize)
			c.Read(p, gfs.Offset(0), data)

		}(i)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("aggregate time: %s\n", elapsed)
	fmt.Printf("average speed: %.2f MB/s\n", float64(numClients*ChunkSize)/1e6/elapsed.Seconds())

	file, err := os.OpenFile("read_speed_results.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	result := fmt.Sprintf("Clients: %d, Average Read Speed: %.2f MB/s\n", numClients, float64(numClients*ChunkSize)/1e6/elapsed.Seconds())
	if _, err := file.WriteString(result); err != nil {
		fmt.Println("Error writing to file:", err)
	}
}

func ConcurrentAppendFilesWithClients(numClients int) {
	var wg sync.WaitGroup

	startTime := time.Now()
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(1000)
	p := gfs.Path(fmt.Sprintf("/file_%d.txt", randomNumber))
	m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})

	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()

			for k := 0; k < 1024; k++ {
				data := make([]byte, ChunkSize>>10)
				for j := 0; j < ChunkSize>>10; j++ {
					data[j] = byte(x)
				}
				c.Append(p, data)
			}
		}(i)
	}

	wg.Wait()

	elapsed := time.Since(startTime)
	fmt.Printf("aggregate time: %s\n", elapsed)
	fmt.Printf("average speed: %.2f MB/s\n", float64(numClients*ChunkSize)/1e6/elapsed.Seconds())

	file, err := os.OpenFile("append_speed_results.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	result := fmt.Sprintf("Clients: %d, Average Append Speed: %.2f MB/s\n", numClients, float64(numClients*ChunkSize)/1e6/elapsed.Seconds())
	if _, err := file.WriteString(result); err != nil {
		fmt.Println("Error appending to file:", err)
	}
}

func TestConcurrentWriteFiles(t *testing.T) {
	clientCounts := []int{2, 4, 6, 8, 10, 12, 14}
	for _, count := range clientCounts {
		ConcurrentWriteFilesWithClients(count)
	}
}

func TestConcurrentAppendFiles(t *testing.T) {
	clientCounts := []int{2, 4, 6, 8, 10, 12, 14, 16}
	for _, count := range clientCounts {
		ConcurrentAppendFilesWithClients(count)
	}
}

func TestConcurrentReadFiles(t *testing.T) {
	clientCounts := []int{2, 4, 6, 8, 10, 12, 14}
	for _, count := range clientCounts {
		ConcurrentWriteFilesWithClients(count)
		ConcurrentReadFilesWithClients(count)
	}
}

func TestSnapShot(t *testing.T) {
	p := gfs.Path("/snapshot.txt")
	msg := []byte("Don't Lose Me.")
	msg2 := []byte("Don't Read Me.")

	ch := make(chan error, N+10)
	ch <- c.Create(p)

	// append chunk 1st
	_, err := c.Append(p, msg)
	ch <- err

	// write chunk 2nd
	var r gfs.GetChunkHandleReply
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 1}, &r)
	if err != nil {
		t.Error(err)
	}
	for i := 0; i < N; i++ {
		go func(x int) {
			ch <- c.WriteChunk(r.Handle, gfs.Offset(x*2), []byte(fmt.Sprintf("%2d", x)))
		}(i)
	}

	time.Sleep(500 * time.Millisecond)

	// test whether the information have been correctly written by using ReadChunk
	buf := make([]byte, 2)
	for i := 0; i < N; i++ {
		_, err := c.ReadChunk(r.Handle, gfs.Offset(i*2), buf)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(buf, []byte(fmt.Sprintf("%2d", i))) {
			t.Error("expected (", fmt.Sprintf("%2d", i), ") != buf (", buf, ")")
		}
	}

	log.Info("\033[32mWriting Test Passed\033[0m")

	c1 := client.NewClient(mAdd)
	go func() {
		err := c1.MakeSnapshot(p)
		ch <- err
	}()
	go func() {
		time.Sleep(500 * time.Millisecond)
		err := c.Write(p, gfs.Offset(0), msg)
		if e, ok := err.(gfs.Error); ok && e.Code == gfs.LeaseHasExpired {
			log.Info("\033[32mLease Expire Test Passed\033[0m")
			ch <- nil
		} else {
			log.Println(e.Code)
			log.Info("\033[31mLease Expire Test Failed\033[0m")
			ch <- fmt.Errorf("lease expire not work")
		}
		log.Info("\033[32mbegin test invalidated write\033[0m")
		err = c.Write(p, gfs.Offset(0), msg2)
		// red
		log.Info("\033[31mWrite Error Info: ", err, "\033[0m")
		ch <- err
	}()

	time.Sleep(2000 * time.Millisecond)
	log.Info("\033[33mCOW test passed!\033[0m")

	buf = make([]byte, len(msg2))
	_, err = c1.ReadChunk(3, gfs.Offset(0), buf)
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(msg, buf) {
		t.Error("expected (", msg, ") != buf (", buf, ")")
	}

	errorAll(ch, N+4, t)
}

func TestCreateFile(t *testing.T) {
	err := m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, &gfs.CreateFileReply{})
	if err != nil {
		t.Error(err)
	}
	err = m.RPCCreateFile(gfs.CreateFileArg{"/test1.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}
}

func TestMkdirDeleteList(t *testing.T) {
	ch := make(chan error, 9)
	ch <- m.RPCMkdir(gfs.MkdirArg{"/dir1"}, &gfs.MkdirReply{})
	ch <- m.RPCMkdir(gfs.MkdirArg{"/dir2"}, &gfs.MkdirReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{"/file1.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{"/file2.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{"/dir1/file3.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir1/file4.txt"}, &gfs.CreateFileReply{})
	ch <- m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir2/file5.txt"}, &gfs.CreateFileReply{})

	err := m.RPCCreateFile(gfs.CreateFileArg{Path: "/dir2/file5.txt"}, &gfs.CreateFileReply{})
	if err == nil {
		t.Error("the same file has been created twice")
	}

	err = m.RPCMkdir(gfs.MkdirArg{"/dir1"}, &gfs.MkdirReply{})
	if err == nil {
		t.Error("the same dirctory has been created twice")
	}

	todelete := make(map[string]bool)
	todelete["dir1"] = true
	todelete["dir2"] = true
	todelete["file1.txt"] = true
	todelete["file2.txt"] = true

	var l gfs.ListReply
	ch <- m.RPCList(gfs.ListArg{"/"}, &l)
	for _, v := range l.Files {
		delete(todelete, v.Name)
	}
	if len(todelete) != 0 {
		t.Error("error in list root path, get", l.Files)
	}

	todelete["file3.txt"] = true
	todelete["file4.txt"] = true
	ch <- m.RPCList(gfs.ListArg{"/dir1"}, &l)
	for _, v := range l.Files {
		delete(todelete, v.Name)
	}
	if len(todelete) != 0 {
		t.Error("error in list root path, get", l.Files)
	}

	errorAll(ch, 9, t)
}

func TestRPCGetChunkHandle(t *testing.T) {
	// var r gfs.CreateFileReply
	var r1, r2 gfs.GetChunkHandleReply
	path := gfs.Path("/test1.txt")
	// err := m.RPCCreateFile(gfs.CreateFileArg{Path: path}, &r)
	// if err != nil {
	// 	t.Error(err)
	// }
	err := m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r1)
	log.Info("get chunk handle: ", r1)
	if err != nil {
		t.Error(err)
	}
	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 0}, &r2)
	if err != nil {
		t.Error(err)
	}
	if r1.Handle != r2.Handle {
		t.Errorf("got different handle: %v and %v", r1.Handle, r2.Handle)
	}

	err = m.RPCGetChunkHandle(gfs.GetChunkHandleArg{path, 2}, &r2)
	if err == nil {
		t.Error("discontinuous chunk should not be created")
	}
}

func TestWriteChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	ch := make(chan error, N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)

	for i := 0; i < N; i++ {
		go func(x int) {
			ch <- c.WriteChunk(r1.Handle, gfs.Offset(x*2), []byte(fmt.Sprintf("%2d", x)))
		}(i)
	}

	errorAll(ch, N+2, t)
}

func TestReadChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestWriteChunk.txt")
	// err := m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	// if err != nil {
	// 	t.Error(err)
	// }
	ch := make(chan error, N+1)
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	for i := 0; i < N; i++ {
		go func(x int) {
			buf := make([]byte, 2)
			n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*2), buf)
			ch <- err
			expected := []byte(fmt.Sprintf("%2d", x))
			if n != 2 {
				t.Error("should read exactly 2 bytes but", n, "instead")
			} else if !reflect.DeepEqual(expected, buf) {
				t.Error("expected (", expected, ") != buf (", buf, ")")
			}
		}(i)
	}
	errorAll(ch, N+1, t)
}

// check if the content of replicas are the same, returns the number of replicas
func checkReplicas(handle gfs.ChunkHandle, length int, t *testing.T) int {
	var data [][]byte

	// get replicas location from master
	var l gfs.GetReplicasReply
	err := m.RPCGetReplicas(gfs.GetReplicasArg{handle}, &l)
	if err != nil {
		t.Error(err)
	}

	for _, v := range l.Locations {
		log.Info("replica: ", v)
	}

	// read
	args := gfs.ReadChunkArg{handle, 0, length}
	for _, addr := range l.Locations {
		var r gfs.ReadChunkReply
		err := util.Call(addr, "ChunkServer.RPCReadChunk", args, &r)
		if err == nil {
			data = append(data, r.Data)
			//fmt.Println("find in ", addr)
		}
	}

	// check equality
	for i := 1; i < len(data); i++ {
		if !reflect.DeepEqual(data[0], data[i]) {
			t.Error("replicas are different. ", data[0], "vs", data[i])
		}
	}

	return len(data)
}

func TestReplicaEquality(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	var data [][]byte
	p := gfs.Path("/TestWriteChunk.txt")
	m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	println(r1.Handle)
	n := checkReplicas(r1.Handle, N*2, t)
	if n != gfs.DefaultNumReplicas {
		t.Error("expect", gfs.DefaultNumReplicas, "replicas, got only", len(data))
	}

	println("###### TestReplicaEquality passed\n")
}

func TestAppendChunk(t *testing.T) {
	var r1 gfs.GetChunkHandleReply
	p := gfs.Path("/TestAppendChunk.txt")
	ch := make(chan error, 2*N+2)
	ch <- m.RPCCreateFile(gfs.CreateFileArg{p}, &gfs.CreateFileReply{})
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	println("###### Got Handle ", r1.Handle)
	expected := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%3d", i))
	}

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			offset, err := c.AppendChunk(r1.Handle, expected[x])
			ch <- err
			data := make([]byte, 3)
			for i := 0; i < 3; i++ {
				data[i] = expected[x][i]
			}
			println("append ", data[0], data[1], data[2], " at ", offset)
			wg.Done()
		}(i)
	}
	wg.Wait()

	println("###### Append Finished")

	checkReplicas(r1.Handle, N*3, t)

	for x := 0; x < N; x++ {
		buf := make([]byte, 3)
		n, err := c.ReadChunk(r1.Handle, gfs.Offset(x*3), buf)
		println("read ", buf[0], buf[1], buf[2], " at ", x*3)
		ch <- err
		if n != 3 {
			t.Error("should read exactly 2 bytes but", n, "instead")
		}
		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		if key == -1 {
			t.Error("incorrect data", buf)
		} else {
			delete(expected, key)
		}
	}

	if len(expected) != 0 {
		t.Error("incorrect data")
	}

	errorAll(ch, 2*N+2, t)
}

/*
 *  TEST SUITE 2 - Client API
 */

// if the append would cause the chunk to exceed the maximum size
// this chunk should be pad and the data should be appended to the next chunk
func TestPadOver(t *testing.T) {
	p := gfs.Path("/appendover.txt")

	ch := make(chan error, 6)
	ch <- c.Create(p)

	bound := gfs.MaxAppendSize - 1
	buf := make([]byte, bound)
	for i := 0; i < bound; i++ {
		buf[i] = byte(i%26 + 'a')
	}

	for i := 0; i < 4; i++ {
		_, err := c.Append(p, buf)
		ch <- err
	}

	buf = buf[:5]
	// an append cause pad, and client should retry to next chunk
	offset, err := c.Append(p, buf)
	ch <- err
	if offset != gfs.MaxChunkSize { // i.e. 0 at next chunk
		t.Error("data should be appended to the beginning of next chunk")
	}

	errorAll(ch, 6, t)
}

// big data that invokes several chunks
func TestWriteReadBigData(t *testing.T) {
	p := gfs.Path("/bigData.txt")

	ch := make(chan error, 4)
	ch <- c.Create(p)

	size := gfs.MaxChunkSize * 3
	expected := make([]byte, size)
	for i := 0; i < size; i++ {
		expected[i] = byte(i%26 + 'a')
	}

	// write large data
	ch <- c.Write(p, gfs.MaxChunkSize/2, expected)

	// read
	buf := make([]byte, size)
	n, err := c.Read(p, gfs.MaxChunkSize/2, buf)
	ch <- err

	if n != size {
		t.Error("size: ", size, " read: ", n)
		t.Error("read counter is wrong")
	}
	if !reflect.DeepEqual(expected, buf) {
		t.Error("read wrong data")
	}

	// test read at EOF
	n, err = c.Read(p, gfs.MaxChunkSize/2+gfs.Offset(size), buf)
	if err == nil {
		t.Error("an error should be returned if read at EOF")
	}

	// test append offset
	var offset gfs.Offset
	buf = buf[:gfs.MaxAppendSize-1]
	offset, err = c.Append(p, buf)
	if offset != gfs.MaxChunkSize/2+gfs.Offset(size) {
		t.Error("append in wrong offset")
	}
	ch <- err

	errorAll(ch, 4, t)
}

type Counter struct {
	sync.Mutex
	ct int
}

func (ct *Counter) Next() int {
	ct.Lock()
	defer ct.Unlock()
	ct.ct++
	return ct.ct - 1
}

// a concurrent producer-consumer number collector for testing race contiditon
func TestComprehensiveOperation(t *testing.T) {
	return
	createTick := 100 * time.Millisecond

	done := make(chan struct{})

	var line []chan gfs.Path
	for i := 0; i < 3; i++ {
		line = append(line, make(chan gfs.Path, N*200))
	}

	// Hard !!
	go func() {
		return
		i := 1
		cs[i-1].Shutdown()
		time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)
		for {
			select {
			case <-done:
				return
			default:
			}
			j := (i - 1 + csNum) % csNum
			jj := strconv.Itoa(j)
			cs[i].Shutdown()
			cs[j] = chunkserver.NewAndServe(csAdd[j], mAdd, path.Join(root, "cs"+jj))
			i = (i + 1) % csNum
			time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)
		}
	}()

	// create
	var wg0 sync.WaitGroup
	var createCt Counter
	wg0.Add(2)
	for i := 0; i < 2; i++ {
		go func(x int) {
			ticker := time.Tick(createTick)
		loop:
			for {
				select {
				case <-done:
					break loop
				case p := <-line[1]: // get back from append
					line[0] <- p
				case <-ticker: // create new file
					x := createCt.Next()
					p := gfs.Path(fmt.Sprintf("/haha%v.txt", x))

					//fmt.Println("create ", p)
					err := c.Create(p)
					if err != nil {
						t.Error(err)
					} else {
						line[0] <- p
					}
				}
			}
			wg0.Done()
		}(i)
	}

	go func() {
		wg0.Wait()
		close(line[0])
	}()

	// append 0, 1, 2, ..., sendCounter
	var wg1 sync.WaitGroup
	var sendCt Counter
	wg1.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			for p := range line[0] {
				x := sendCt.Next()

				//fmt.Println("append ", p, "  ", tmp)
				_, err := c.Append(p, []byte(fmt.Sprintf("%10d,", x)))
				if err != nil {
					t.Error(err)
				}

				line[1] <- p
				line[2] <- p
			}
			wg1.Done()
		}(i)
	}

	go func() {
		wg1.Wait()
		close(line[2])
	}()

	// read and collect numbers
	var wg2 sync.WaitGroup
	var lock2 sync.RWMutex
	receiveData := make(map[int]int)
	fileOffset := make(map[gfs.Path]int)
	wg2.Add(N)
	for i := 0; i < N; i++ {
		go func(x int) {
			for p := range line[2] {
				lock2.RLock()
				pos := fileOffset[p]
				lock2.RUnlock()
				buf := make([]byte, 10000) // large enough
				n, err := c.Read(p, gfs.Offset(pos), buf)
				if err != nil && err != io.EOF {
					t.Error(err)
				}
				if n == 0 {
					continue
				}
				buf = buf[:n-1]
				//fmt.Println("read ", p, " at ", pos, " : ", string(buf))

				lock2.Lock()
				for _, v := range strings.Split(string(buf), ",") {
					i, err := strconv.Atoi(strings.TrimSpace(v))
					if err != nil {
						t.Error(err)
					}
					receiveData[i]++
				}
				if pos+n > fileOffset[p] {
					fileOffset[p] = pos + n
				}
				lock2.Unlock()
				time.Sleep(N * time.Millisecond)
			}
			wg2.Done()
		}(i)
	}

	// wait to test race contition
	fmt.Println("###### Continue life for the elder to pass a long time test...")
	for i := 0; i < 6; i++ {
		fmt.Print(" +1s ")
		time.Sleep(time.Second)
	}
	fmt.Println("")
	close(done)
	wg2.Wait()

	// check correctness
	total := sendCt.Next() - 1
	for i := 0; i < total; i++ {
		if receiveData[i] < 1 {
			t.Errorf("error occured in sending data %v", i)
		}
	}
	fmt.Printf("##### You send %v numbers in total\n", total)
}

/*
 *
 */

/*
 *  TEST SUITE 3 - Fault Tolerance
 */

// Shutdown two chunk servers during appending
func TestShutdownInAppend(t *testing.T) {
	p := gfs.Path("/shutdown.txt")
	ch := make(chan error, N+3)
	ch <- c.Create(p)

	expected := make(map[int][]byte)
	todelete := make(map[int][]byte)
	for i := 0; i < N; i++ {
		expected[i] = []byte(fmt.Sprintf("%2d", i))
		todelete[i] = []byte(fmt.Sprintf("%2d", i))
	}

	// get two replica locations
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	var l gfs.GetReplicasReply
	ch <- m.RPCGetReplicas(gfs.GetReplicasArg{r1.Handle}, &l)

	for i := 0; i < N; i++ {
		go func(x int) {
			_, err := c.Append(p, expected[x])
			ch <- err
		}(i)
	}

	time.Sleep(N * time.Millisecond)
	// choose two servers to shutdown during appending
	for i, v := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			log.Warning("##########shutdown ", i)
			v.Shutdown()
		}
	}

	errorAll(ch, N+3, t)

	// check correctness, append at least once
	// TODO : stricter - check replicas
	for x := 0; x < gfs.MaxChunkSize/2 && len(todelete) > 0; x++ {
		buf := make([]byte, 2)
		n, err := c.Read(p, gfs.Offset(x*2), buf)
		if err != nil {
			t.Error("read error ", err)
		}
		if n != 2 {
			t.Error("should read exactly 2 bytes but", n, "instead")
		}

		key := -1
		for k, v := range expected {
			if reflect.DeepEqual(buf, v) {
				key = k
				break
			}
		}
		if key == -1 {
			t.Error("incorrect data", buf)
		} else {
			// log.Info("read key: ", key)
			delete(todelete, key)
		}
	}
	if len(todelete) != 0 {
		t.Errorf("missing data %v", todelete)
	}

	// restart
	for i, _ := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			ii := strconv.Itoa(i)
			cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
		}
	}
}

// Shutdown all servers in turns. You should perform re-replication well
func TestReReplication(t *testing.T) {
	p := gfs.Path("/re-replication.txt")

	ch := make(chan error, 2)
	ch <- c.Create(p)

	c.Append(p, []byte("Dangerous"))

	fmt.Println("###### Mr. Disaster is coming...")
	time.Sleep(gfs.LeaseExpire)

	cs[1].Shutdown()
	cs[2].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[1] = chunkserver.NewAndServe(csAdd[1], mAdd, path.Join(root, "cs1"))
	cs[2] = chunkserver.NewAndServe(csAdd[2], mAdd, path.Join(root, "cs2"))

	cs[3].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[4].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[3] = chunkserver.NewAndServe(csAdd[3], mAdd, path.Join(root, "cs3"))
	cs[4] = chunkserver.NewAndServe(csAdd[4], mAdd, path.Join(root, "cs4"))
	time.Sleep(gfs.ServerTimeout)

	cs[0].Shutdown()
	time.Sleep(gfs.ServerTimeout * 2)

	cs[0] = chunkserver.NewAndServe(csAdd[0], mAdd, path.Join(root, "cs0"))
	time.Sleep(gfs.ServerTimeout)

	// check equality and number of replicas
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	n := checkReplicas(r1.Handle, N*2, t)

	if n < gfs.MinimumNumReplicas {
		t.Errorf("Cannot perform replicas promptly, only get %v replicas", n)
	} else {
		fmt.Printf("###### Well done, you save %v replicas during disaster\n", n)
	}

	errorAll(ch, 2, t)
}

// some file operations to check if the server is still working
func checkWork(p gfs.Path, msg []byte, t *testing.T) {
	ch := make(chan error, 6)
	log.Info("checkWork path: ", p)

	// append again to confirm all information about this chunk has been reloaded properly
	offset, err := c.Append(p, msg)
	ch <- err

	// read and check data
	buf := make([]byte, len(msg)*2)
	// read at defined region
	_, err = c.Read(p, offset-gfs.Offset(len(msg)), buf)
	ch <- err

	msg = append(msg, msg...)
	if !reflect.DeepEqual(buf, msg) {
		t.Errorf("[check 1]read wrong data \"%v\", expect \"%v\"", string(buf), string(msg))
	}

	// // other file operation
	ch <- c.Mkdir(gfs.Path("/" + string(msg)))
	newfile := gfs.Path("/" + string(msg) + "/" + string(msg) + ".txt")
	ch <- c.Create(newfile)
	ch <- c.Write(newfile, 4, msg)

	// read and check data again
	buf = make([]byte, len(msg))
	_, err = c.Read(p, 0, buf)
	ch <- err
	if !reflect.DeepEqual(buf, msg) {
		t.Errorf("[check 2]read wrong data \"%v\", expect \"%v\"", string(buf), string(msg))
	}

	errorAll(ch, 6, t)
}

// Shutdown all chunk servers. You must store the meta data of chunkserver persistently
func TestPersistentChunkServer(t *testing.T) {
	p := gfs.Path("/persistent-chunkserver.txt")
	msg := []byte("Don't Lose Me. ")

	ch := make(chan error, 4)
	ch <- c.Create(p)

	_, err := c.Append(p, msg)
	ch <- err

	// shut all down
	fmt.Println("###### SHUT All DOWN")
	for _, v := range cs {
		v.Shutdown()
	}
	time.Sleep(2*gfs.ServerTimeout + gfs.LeaseExpire)

	// restart
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
	}

	fmt.Println("###### Waiting for Chunk Servers to report their chunks to master...")
	time.Sleep(2*gfs.ServerTimeout + gfs.LeaseExpire)

	// check recovery
	checkWork(p, msg, t)

	errorAll(ch, 2, t)
}

// Shutdown master. You must store the meta data of master persistently
func TestPersistentMaster(t *testing.T) {
	p := gfs.Path("/persistent/master.txt")
	msg := []byte("Don't Lose Yourself. ")

	ch := make(chan error, 5)
	ch <- c.Mkdir("/persistent")
	ch <- c.Create(p)

	_, err := c.Append(p, msg)
	ch <- err

	// shut master down
	fmt.Println("###### Shutdown Master")
	m.Shutdown()
	time.Sleep(2 * gfs.ServerTimeout)

	// restart
	m = master.NewAndServe(mAdd, path.Join(root, "m"))
	time.Sleep(2 * gfs.ServerTimeout)

	errorAll(ch, 3, t)
	// check recovery
	checkWork(p, msg, t)

}

// delete all files in two chunkservers ...
func TestDiskError(t *testing.T) {
	p := gfs.Path("/virus.die")
	msg := []byte("fucking disk!")

	ch := make(chan error, 5)
	ch <- c.Create(p)

	_, err := c.Append(p, msg)
	ch <- err

	// get replica locations
	var r1 gfs.GetChunkHandleReply
	ch <- m.RPCGetChunkHandle(gfs.GetChunkHandleArg{p, 0}, &r1)
	var l gfs.GetReplicasReply
	ch <- m.RPCGetReplicas(gfs.GetReplicasArg{r1.Handle}, &l)

	fmt.Println("###### Destory two chunkserver's diskes")
	// destory two server's disk
	log.Info("Kill server: ", l.Locations[:2])
	for i := range cs {
		if csAdd[i] == l.Locations[0] || csAdd[i] == l.Locations[1] {
			ii := strconv.Itoa(i)
			os.RemoveAll(path.Join(root, "cs"+ii))
		}
	}
	fmt.Println("###### Waiting for recovery")
	time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)
	fmt.Println("bushi, gemener")
	_, err = c.Append(p, msg)
	ch <- err

	time.Sleep(gfs.ServerTimeout + gfs.LeaseExpire)

	// TODO: check re-replication
	// check recovery
	checkWork(p, msg, t)

	errorAll(ch, 5, t)
}

/*
 *  TEST SUITE 4 - Challenge
 */

// todo : simulate an extremely adverse condition

func TestMain(tm *testing.M) {
	// create temporary directory
	var err error
	root, err = ioutil.TempDir("", "gfs-")
	if err != nil {
		log.Fatal("cannot create temporary directory: ", err)
	}

	//log.SetLevel(log.FatalLevel)

	// run master
	os.Mkdir(path.Join(root, "m"), 0755)
	m = master.NewAndServe(mAdd, path.Join(root, "m"))

	// run chunkservers
	csAdd = make([]gfs.ServerAddress, csNum)
	cs = make([]*chunkserver.ChunkServer, csNum)
	for i := 0; i < csNum; i++ {
		ii := strconv.Itoa(i)
		os.Mkdir(path.Join(root, "cs"+ii), 0755)
		csAdd[i] = gfs.ServerAddress(fmt.Sprintf(":%v", 10000+i))
		cs[i] = chunkserver.NewAndServe(csAdd[i], mAdd, path.Join(root, "cs"+ii))
	}

	// init client
	c = client.NewClient(mAdd)
	time.Sleep(300 * time.Millisecond)

	// run tests
	ret := tm.Run()

	// shutdown
	for _, v := range cs {
		v.Shutdown()
	}
	m.Shutdown()
	os.RemoveAll(root)

	os.Exit(ret)
}
