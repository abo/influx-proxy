package service

import (
	"testing"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/sharding"
)

func TestSerialize(t *testing.T) {
	nodes := []*DataNodeInfo{
		{0, backend.BackendConfig{Url: "https://influxdb1:8086", Username: "influx", Password: "pwd!@$$%"}},
		{1, backend.BackendConfig{Url: "http://influxdb2:8086"}}}
	replicas := []*sharding.ReplicaInfo{{"cpe_lw_conn_rxtx", 0, int32(5), sharding.Balanced}}
	data, err := serialize(nodes, replicas)
	if err != nil {
		t.Errorf("failed to serialize: %v", err)
	}
	nodes2, replicas2, e := unserialize(data)
	if e != nil {
		t.Errorf("failed to unserialize: %v", e)
	}
	if len(nodes) != len(nodes2) {
		t.Errorf("data nodes changed: %v", nodes2)
	}
	for i, v := range nodes {
		if nodes2[i].ID != v.ID || nodes2[i].Url != v.Url || nodes2[i].Username != v.Username || nodes2[i].Password != v.Password {
			t.Errorf("data nodes changed: %v", nodes2)
		}
	}

	if len(replicas) != len(replicas2) {
		t.Errorf("replicas changed: %v", replicas2)
	}
	for i, v := range replicas {
		if replicas2[i].Measurement != v.Measurement || replicas2[i].Index != v.Index || replicas2[i].Shards != v.Shards || replicas2[i].State != v.State {
			t.Errorf("replicas changed: %v", nodes2)
		}
	}
}
