package service

import (
	"fmt"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"go.uber.org/multierr"
)

func (svc *HttpService) applyRaftCommits() {
	for commit, ok := svc.cluster.PullCommit(); ok; commit, ok = svc.cluster.PullCommit() {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := svc.cluster.LoadSnapshot()
			if err != nil {
				log.Fatalf("failed to load snapshot: %v", err)
			}
			if snapshot == nil {
				continue
			}
			log.Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := svc.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Fatalf("failed to recover from snapshot: %v", err)
			}
			continue
		}

		for _, data := range commit.Data {
			dataNodes, replicas, err := unserialize(data)
			if err != nil {
				log.Fatalf("broken commit (%v): %v", err, string(data))
			}
			if err = svc.applyNodeChanges(dataNodes); err != nil {
				log.Fatalf("failed to apply data-node changes: %v", err)
			}
			if err = svc.sharder.ApplyReplicaChanges(replicas); err != nil {
				log.Fatalf("failed to apply data-node changes: %v", err)
			}
		}
		close(commit.ApplyDoneC)
	}
	if err := svc.cluster.Error(); err != nil {
		log.Fatalf("raft cluster quit with err: %v", err)
	}
}

func (svc *HttpService) Stop() {
	svc.cluster.Stop()
}
func (svc *HttpService) proposeDataNodeChanges(node *DataNodeInfo) error {
	data, err := serialize([]*DataNodeInfo{node}, nil)
	if err != nil {
		return err
	}
	svc.cluster.Propose(data)
	return nil
}

func (svc *HttpService) recoverFromSnapshot(snapshot []byte) error {
	dataNodes, replicas, err := unserialize(snapshot)
	if err != nil {
		return err
	}

	err = multierr.Append(err, svc.applyNodeChanges(dataNodes))
	err = multierr.Append(err, svc.sharder.ApplyReplicaChanges(replicas))
	return err
}

func (svc *HttpService) applyNodeChanges(changes []*DataNodeInfo) error {
	var err error
	for _, nodeInfo := range changes {
		id, url, user, pwd := nodeInfo.ID, nodeInfo.Url, nodeInfo.Username, nodeInfo.Password
		switch {
		case len(svc.dataNodes) == id: // append
			nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
			svc.cfg.DataNodes = append(svc.cfg.DataNodes, nodeCfg)
			svc.dataNodes = append(svc.dataNodes, backend.NewBackend(id, nodeCfg, svc.cfg.Proxy))
			svc.dmgr.SetDataNodes(svc.dataNodes)
			svc.proxy.SetDataNodes(svc.dataNodes)
		case len(svc.dataNodes) > id && nodeInfo.Url != "": // replace
			nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
			origin := svc.dataNodes[id].HttpBackend
			svc.dataNodes[id].HttpBackend = backend.NewHttpBackend(nodeCfg, svc.cfg.Proxy)
			svc.cfg.DataNodes[id] = nodeCfg
			origin.Close()
		case len(svc.dataNodes)-1 == id && nodeInfo.Url == "": // remove last
			origin := svc.dataNodes[id].HttpBackend
			svc.dataNodes = svc.dataNodes[:id]
			svc.cfg.DataNodes = svc.cfg.DataNodes[:id]
			svc.dmgr.SetDataNodes(svc.dataNodes)
			svc.proxy.SetDataNodes(svc.dataNodes)
			origin.Close()
		default: // error
			log.Warnf("illegal data-node change ignored: %v, current nodes: %v", nodeInfo, svc.cfg.DataNodes)
			err = multierr.Append(err, fmt.Errorf("can not apply change: %v", nodeInfo))
		}
	}
	return err
}
