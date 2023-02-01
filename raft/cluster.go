package raft

import (
	"net/http"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

type Cluster struct {
	proposeC    chan<- []byte
	confChangeC chan<- raftpb.ConfChange
	commitC     <-chan *Commit
	errorC      <-chan error
	rnode       *raftNode
	snapshotter *snap.Snapshotter
	snapInit    sync.Once
	handler     http.Handler
	handlerInit sync.Once
}

func StartCluster(localID uint64, peers map[uint64]string, basedir string, generateSnapshot func() ([]byte, error)) *Cluster {
	proposeC := make(chan []byte, 64)
	confChangeC := make(chan raftpb.ConfChange, 16)
	commitC, errorC, rnode := newRaftNode(localID, peers, basedir, generateSnapshot, proposeC, confChangeC)

	return &Cluster{rnode: rnode, proposeC: proposeC, confChangeC: confChangeC, commitC: commitC, errorC: errorC}
}

func (c *Cluster) PullCommit() (*Commit, bool) {
	commit, ok := <-c.commitC
	return commit, ok
}

func (c *Cluster) LoadSnapshot() (*raftpb.Snapshot, error) {
	c.snapInit.Do(func() {
		c.snapshotter = <-c.rnode.snapshotterReady
	})
	return c.snapshotter.Load()
}

func (c *Cluster) Propose(proposal []byte) {
	c.proposeC <- proposal
}

func (c *Cluster) ProposeConfChange(change raftpb.ConfChange) {
	c.confChangeC <- change
}

func (c *Cluster) Stop() {
	c.rnode.Stop()
}

func (c *Cluster) Error() error {
	return <-c.errorC
}

func (c *Cluster) TransportHandler() http.Handler {
	c.handlerInit.Do(func() {
		c.handler = (<-c.rnode.transportReady).Handler()
	})
	return c.handler
}

// func (c *Cluster) Nodes() map[uint64]string {
// }
