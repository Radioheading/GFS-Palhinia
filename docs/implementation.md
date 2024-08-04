# Development Document

## Persistence

To avoid fatal loss when encountering shutdown, persistence is needed to periodically store information on disk. In the implementations of GFS, we need to persist the whole namespace tree, the metadata table of master, and the necessary information of the chunkservers.

In a nutshell, the persistence process consists of two steps: collecting all the information and use `gob.NewEncoder` to serialize it in a file.

#### Namespace Tree Persistence

As for the namespace tree, we only need to DFS the tree to record the information in an array. Since we need to recover the tree structure, the definition of a persistent namespace tree node is:

```go
type persistNsTree struct {
	Id  int // dfs order of the current node
	Fid int // dfs order of its father
    // metadata of the file
	Name   string 
	IsDir  bool
	Length int64
	Chunks int64
}
```

In this manner we would be able to restore the namespace tree in the decoding process.

#### Master's Metadata Persistence

Recall that we use `chunkManager` to store the chunk information and the corresponding handles. For its persistence, only path, version of each chunk should be stored, as the server locations of each chunk would be updated upon the reboot of master.

#### Chunk Servers' Persistence

just store the metadata of the whole file: including what are its handles, what are their lengths, versions respectively.

## Garbage Collection

GFS original paper introduces a lazy GC strategy: we remove chunks and their corresponding files on a given routine, so we need to store what chunks for what handles to remove.

The full process of GC can be described as follows:

1. during `getLeaseHolder()`, we check all the servers which are supposed to hold the given handle, and return with those who don't have or have obsolete version
2. we maintain such stale servers in the `chunkServerManager`, and during the heartbeat of a `chunkServer`, it gets such information from the `chunkServerManger`. Thus, during the GC routine, we can delete these chunks.