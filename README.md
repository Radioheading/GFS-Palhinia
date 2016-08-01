# PPCA 2016: Google File System

1或2人一组，完成一份简化版的 [Google File System](http://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) 实现，需要通过灰盒测试和压力测试。请在 Github 或者 Bitbucket 上建立私有 git 仓库，并使用 git 进行版本管理。

## 功能

你需要完成 Master, Chunkserver 和 Client 三者的编写。以下基本要求一定要达到的：

* 文件系统操作：`create`, `mkdir`
* 文件操作：`read`, `write`, `append`
* Chunkserver 掉线、上线不会影响正常使用
* Master 和 Chunkserver 的 Metadata 持久化（重启时能载入）
* 当存活的 Replica 数目过少时进行 Re-replication

以下是可选要求：

* Chunkserver 文件校验

## 代码框架

你可以依靠本仓库下的代码框架。你可以做任何修改，只要能通过灰盒测试以及压力测试即可。

## 灰盒测试

灰盒测试运行在本地，用来检验实现是否基本正确。很抱歉测试的粒度非常粗糙。测试的运行方法：

```bash
export GOPATH=/path/to/your/git/repository
go test -v
```

目前灰盒测试还未完成编写，接下来会追加测试内容。

## 压力测试

压力测试会分布式地运行在所有机子上，通过大量的业务操作来检查实现中可能存在的问题，同时也兼有性能测试的功能。

目前压力测试还未编写。


