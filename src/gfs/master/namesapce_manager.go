package master

import (
	"fmt"
	"gfs"
	"sync"

	log "github.com/sirupsen/logrus"
)

type namespaceManager struct {
	root *nsTree
}

type nsTree struct {
	sync.RWMutex

	// if it is a directory
	isDir    bool
	children map[string]*nsTree

	// if it is a file
	length int64
	chunks int64
}

type persistNsTree struct {
	Id  int
	Fid int

	Name   string
	IsDir  bool
	Length int64
	Chunks int64
}

func newNamespaceManager() *namespaceManager {
	nm := &namespaceManager{
		root: &nsTree{isDir: true,
			children: make(map[string]*nsTree)},
	}
	return nm
}

// here we deploy the APIs from lmzheng
// func dfs for persistence of namespace tree, as is show in the struct persistNsTree,
// we need to store its node id: DFS order, father id, name, isdir, length, chunks.
func dfs(u *nsTree, name string, fid int, cur_id *int, array *[]persistNsTree) {
	*cur_id++

	*array = append(*array, persistNsTree{
		Id:     *cur_id,
		Fid:    fid,
		Name:   name,
		IsDir:  u.isDir,
		Length: u.length,
		Chunks: u.chunks,
	})

	for name, v := range u.children {
		v.Lock()
		dfs(v, name, *cur_id, cur_id, array)
		v.Unlock()
	}
}

// func persist for namespace tree, we need to lock the root node and call dfs
func (nm *namespaceManager) Persist() []persistNsTree {
	var id int
	var array []persistNsTree
	nm.root.Lock()
	dfs(nm.root, "nm-lock", 0, &id, &array)
	nm.root.Unlock()
	return array
}

func (nm *namespaceManager) antiPersist(array []persistNsTree) {
	nm.root.Lock()

	cur_restore := make(map[int]*nsTree)

	for _, v := range array {
		var u *nsTree
		if v.Fid == 0 { // this is root
			u = nm.root
		} else {
			u = &nsTree{isDir: v.IsDir, children: make(map[string]*nsTree), length: v.Length, chunks: v.Chunks}
		}
		cur_restore[v.Id] = u
		// find father
		if v.Fid != 0 && cur_restore[v.Fid] != nil {
			father := cur_restore[v.Fid]
			father.children[v.Name] = u
		}
	}
	nm.root.Unlock()
}

// acquire read lock along the parents (e.g. /d1/d2/.../dn/leaf):
//
// acquire read-locks on the directory names /d1, /d1/d2, ..., /d1/d2/.../dn
//
// If RLockLeaf = True, then acquire read-locks on /d1/d2/.../dn/leaf
// thus, need true for read-only circumstances
func (nm *namespaceManager) lockParents(paths []string, RLockLeaf bool) (*nsTree, error) {
	log.Info("lockParents: ", paths)
	cur := nm.root
	for i := 0; i < len(paths)-1; i++ {
		if cur.children[paths[i]] == nil {
			return nil, fmt.Errorf("path %v does not exist", paths[i])
		} else {
			log.Info("lockParents: ", paths[i])
			cur.RLock()
			cur = cur.children[paths[i]]
		}
	}

	if !cur.isDir {
		return nil, fmt.Errorf("path %v is not a directory", paths[len(paths)-1])
	}

	if RLockLeaf {
		cur.RLock()
	}

	return cur, nil
}

// func unloockParents for releasing read lock along the parents
func (nm *namespaceManager) unlockParents(paths []string, RLockLeaf bool) {
	cur := nm.root
	for i := 0; i < len(paths)-1; i++ {
		cur.RUnlock()
		cur = cur.children[paths[i]]
	}

	if RLockLeaf {
		cur.RUnlock()
	}
}

// Create creates an empty file on path p. All parents should exist.
func (nm *namespaceManager) Create(p gfs.Path) error {
	raw_path, filename := p.ParseLeafname()
	paths := raw_path.GetPaths()
	new_node, err := nm.lockParents(paths, false)
	new_node.Lock()
	defer new_node.Unlock()
	if err != nil {
		return err
	}

	if new_node.children[filename] != nil {
		return fmt.Errorf("file %v already exists", filename)
	}

	log.Info("create file: ", filename)
	log.Info("file path: ", raw_path)

	new_node.children[filename] = &nsTree{isDir: false, length: 0, chunks: 0, children: make(map[string]*nsTree)}

	return nil
}

// Mkdir creates a directory on path p. All parents should exist.
func (nm *namespaceManager) Mkdir(p gfs.Path) error {
	raw_path, filename := p.ParseLeafname()
	paths := raw_path.GetPaths()
	new_node, err := nm.lockParents(paths, false)
	new_node.Lock()
	defer new_node.Unlock()
	if err != nil {
		return err
	}

	if new_node.children[filename] != nil {
		return fmt.Errorf("directory %v already exists", filename)
	}

	new_node.children[filename] = &nsTree{isDir: true, length: 0, chunks: 0, children: make(map[string]*nsTree)}

	return nil
}

// GetFileInfo returns the information of a file, including length, chunks, isDir
func (nm *namespaceManager) GetFileInfo(p gfs.Path, reply *gfs.GetFileInfoReply) error {
	raw_path, filename := p.ParseLeafname()
	paths := raw_path.GetPaths()
	new_node, err := nm.lockParents(paths, false)
	defer nm.unlockParents(paths, false)
	if err != nil {
		return err
	}

	if new_node.children[filename] == nil {
		return fmt.Errorf("file %v does not exist", filename)
	}

	reply.IsDir = new_node.children[filename].isDir
	reply.Length = new_node.children[filename].length
	reply.Chunks = new_node.children[filename].chunks

	return nil
}

// List lists everything in specific directory on GFS.
func (nm *namespaceManager) List(p gfs.Path) ([]gfs.PathInfo, error) {
	paths := p.GetPaths()
	new_node, err := nm.lockParents(paths, true)
	defer nm.unlockParents(paths, true)
	if err != nil {
		return nil, err
	}

	ret := make([]gfs.PathInfo, 0)
	for name, v := range new_node.children {
		ret = append(ret, gfs.PathInfo{Name: name, IsDir: v.isDir, Length: v.length, Chunks: v.chunks})
	}

	return ret, nil
}
