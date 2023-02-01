package sharding

type Config struct {
	Name string `mapstructure:"name"`
	Tag  string `mapstructure:"tag"`
}

// 数据管理接口
type DataManager interface {

	// 返回集群管理范围内的所有 measurement
	GetManagedMeasurements() []string

	// 判断指定 measurement 是否在代理管理范围内
	IsManagedMeasurement(dbAndMeasurement string) bool

	// 扫描指定节点的原始数据，获取所有 shardingTag 值
	ScanTagValues(dbAndMeasurement string, shard int32, shardingTag string, fn func([]string) bool) error

	// 将 measurement 中 shardingTagValue 对应的所有原始数据，从 srcShard 迁移到 destShards
	CopySeries(dbAndMeasurement string, srcShard int32, destShards []int32, shardingTag string, tagValue string) error

	// 从指定分片按 tag 删除序列
	RemoveSeries(dbAndMeasurement string, shard int32, shardingTag string, tagValue string) error

	// 从 srcNode 中将指定 Measurement 整体复制到 destNode
	CopyMeasurement(srcNode int32, destNodes []int32, dbAndMeasurement string) error

	// 从指定节点删除 measurement
	RemoveMeasurement(node int32, dbAndMeasurement string) error
}

type ReplicaState int

const (
	Balanced ReplicaState = iota
	Rebalancing
)

// func state2str(s ReplicaState) string {
// 	switch s {
// 	case Balanced:
// 		return "balanced"
// 	case Rebalancing:
// 		return "rebalancing"
// 	default:
// 		return "unknown"
// 	}
// }

type ReplicaInfo struct {
	Measurement string       // measurement
	Index       int          // replica index, 0-based
	Shards      int32        // how many shards(nodes) for this replica
	State       ReplicaState // replica state, balanced/rebalancing/
}
