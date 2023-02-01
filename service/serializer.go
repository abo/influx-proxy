package service

import (
	"encoding/json"

	"github.com/abo/influx-proxy/sharding"
)

type State struct {
	DateNodes []*DataNodeInfo
	Replicas  []*sharding.ReplicaInfo
}

func serialize(dateNodes []*DataNodeInfo, replicas []*sharding.ReplicaInfo) ([]byte, error) {
	var state State
	state.Replicas = replicas
	state.DateNodes = dateNodes
	return json.Marshal(state)
}

func unserialize(data []byte) ([]*DataNodeInfo, []*sharding.ReplicaInfo, error) {
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, nil, err
	}
	return state.DateNodes, state.Replicas, nil
}
