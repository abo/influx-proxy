package sharding

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/abo/influx-proxy/log"
	"go.uber.org/multierr"
)

type Config struct {
	Name string `mapstructure:"name"`
	Tag  string `mapstructure:"tag"`
}

// 数据管理
type DataManager interface {

	// 返回集群管理范围内的所有 measurement
	GetManagedMeasurements() []string

	// 扫描指定节点的原始数据，获取所有 shardingTag 值
	ScanTagValues(measurement string, shard int32, shardingTag string, fn func([]uint64) bool) error
	// ScanShardingTagValues(shard int32, measurement string) []uint64

	// 将 measurement 中 shardingTagValue 对应的所有原始数据，从 srcShard 迁移到 destShards
	CopySeries(measurement string, srcShard int32, destShards []int32, shardingTag string, tagValue uint64) error

	//
	RemoveSeries(measurement string, shard int32, shardingTag string, tagValue uint64) error

	// 从 srcNode 中将指定 Measurement 整体复制到 destNode
	CopyMeasurement(srcNode int32, destNodes []int32, measurement string) error

	RemoveMeasurement(node int32, measurement string) error
}

type replicaState int

const (
	balanced replicaState = iota
	rebalancing
)

// 对 measurement 的 replica 进行分片, 计算每条数据的分片位置, 以及触发对应的迁移/均衡
// 对于不分片的 measurement, 当作分片数 1 处理,
type ReplicaSharder struct {
	dmgr        DataManager
	shardingTag map[string]string
	meta        map[string][]int32        // 各 measurement 下, 每个 replica 的分片数, 结构为 measurement -> [r0.shards, r1.shards ...];  如果该 measurement 不分片, 则存放各 replica 的节点数
	state       map[string][]replicaState // 各 replica 的状态, 结构为 measurement -> [r0.state, r1.state...]
	mu          sync.RWMutex              // mu protects meta & state
}

func NewSharder(dmgr DataManager, cfg []*Config) *ReplicaSharder {
	tags := make(map[string]string)
	for _, measurement := range cfg {
		tags[measurement.Name] = measurement.Tag
	}

	rand.Seed(time.Now().Unix())
	return &ReplicaSharder{dmgr: dmgr, shardingTag: tags, meta: make(map[string][]int32), state: make(map[string][]replicaState)}
}

// Init a sharded cluster's metadata
// 对于 sharding measurement, 默认按节点数分片, 也就是每个节点有一部分数据
// 对于 unsharding measurement, 整表在节点间复制和均衡, 相当于分片数是 1, 也就是某些分片为空
func (rs *ReplicaSharder) Init(numberOfNodes, defaultNumberOfReplicas int) {
	rs.mu.Lock()
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		rs.meta[measurement] = make([]int32, defaultNumberOfReplicas)
		rs.state[measurement] = make([]replicaState, defaultNumberOfReplicas)
		for replica := 0; replica < defaultNumberOfReplicas; replica++ {
			rs.meta[measurement][replica] = int32(numberOfNodes)
			rs.state[measurement][replica] = balanced
		}
	}
	rs.mu.Unlock()
}

// 获取指定 measurement 的分片 tag，如果该 measurement 未分片则返回空
func (rs *ReplicaSharder) GetShardingTag(measurement string) (string, bool) {
	tag, ok := rs.shardingTag[measurement]
	return tag, ok && tag != ""
}

func (rs *ReplicaSharder) getNumberOfReplicas(measurement string) int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return len(rs.meta[measurement])
}

func (rs *ReplicaSharder) getNumberOfShards(measurement string, replica int) int32 {
	if _, sharded := rs.GetShardingTag(measurement); !sharded {
		return 1
	}
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.meta[measurement][replica]
}

// 对所有 measurement 的所有 replica, 调整分片数并执行 rebalance
func (rs *ReplicaSharder) Scale(numberOfShards int) error {
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			if _, sharded := rs.GetShardingTag(m); !sharded {
				return
			}
			for replica := 0; replica < rs.getNumberOfReplicas(m); replica++ {
				if rs.getNumberOfShards(m, replica) == int32(numberOfShards) {
					continue
				}
				rs.mu.Lock()
				rs.meta[m][replica] = int32(numberOfShards)
				rs.mu.Unlock()
				err = multierr.Append(err, rs.Rebalance(m, replica))
			}
			wg.Done()
		}(measurement)

	}
	wg.Wait()
	return err
}

// 调整复制数, 跨 replica 复制数据
func (rs *ReplicaSharder) Replicate(numberOfReplicas int) error {
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			currentReplicas := rs.getNumberOfReplicas(m)
			if currentReplicas == numberOfReplicas {
				return
			} else if currentReplicas > numberOfReplicas {
				err = multierr.Append(err, rs.shrinkReplica(m, numberOfReplicas))
			} else {
				err = multierr.Append(err, rs.expandReplica(m, numberOfReplicas))
			}

			wg.Done()
		}(measurement)
	}
	wg.Wait()
	return err
}

// 删除多余的 replica, 仅保留 numberOfReplicas 个
func (rs *ReplicaSharder) shrinkReplica(measurement string, numberOfReplicas int) error {
	// 删除元数据并清理原始数据
	rs.mu.Lock()
	rs.meta[measurement] = rs.meta[measurement][:numberOfReplicas]
	rs.state[measurement] = rs.state[measurement][:numberOfReplicas]
	rs.mu.Unlock()

	return rs.Cleanup(measurement)
}

// 增加更多 replica
func (rs *ReplicaSharder) expandReplica(measurement string, numberOfReplicas int) error {
	shards := rs.getNumberOfShards(measurement, 0) // 分片数与第一个 replica 保持相同
	rs.mu.Lock()
	originReplicas := len(rs.meta[measurement])
	for len(rs.meta[measurement]) < numberOfReplicas {
		rs.meta[measurement] = append(rs.meta[measurement], shards)
		rs.state[measurement] = append(rs.state[measurement], rebalancing)
	}
	rs.mu.Unlock()

	var err error

	if tagName, sharded := rs.GetShardingTag(measurement); sharded {
		// 对于已分片的 measurement, 由于数据分散在多个节点, 从第一个 replica 的各个 shard 复制数据
		for shard := int32(0); shard < shards; shard++ {
			err = multierr.Append(err, rs.dmgr.ScanTagValues(measurement, shard, tagName, func(tagValues []uint64) bool {
				for _, tagValue := range tagValues {
					destShards := jumpHashForReplica(tagValue, numberOfReplicas-1, shards)
					err = multierr.Append(err, rs.dmgr.CopySeries(measurement, shard, destShards[originReplicas:], tagName, tagValue))
				}
				return false
			}))
		}
	} else {
		// 对于未分片的整体复制
		destNodes := jumpHashForReplica(str2key(measurement), numberOfReplicas-1, shards)
		err = multierr.Append(err, rs.dmgr.CopyMeasurement(destNodes[0], destNodes[originReplicas:], measurement))
	}

	rs.mu.Lock()
	for replica := originReplicas; replica < numberOfReplicas; replica++ {
		rs.state[measurement][replica] = balanced
	}
	rs.mu.Unlock()
	return err
}

func (rs *ReplicaSharder) RebalanceForAll() error {
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			for i := 0; i < rs.getNumberOfReplicas(m); i++ {
				err = multierr.Append(err, rs.Rebalance(m, i))
			}
			wg.Done()
		}(measurement)
	}
	wg.Wait()
	return err
}

func (rs *ReplicaSharder) Rebalance(measurement string, replica int) error {
	rs.mu.Lock()
	if rs.state[measurement][replica] == rebalancing {
		rs.mu.Unlock()
		return fmt.Errorf("sharder: measurement(%s) 's replica(%d) is rebalancing", measurement, replica)
	}
	rs.state[measurement][replica] = rebalancing
	rs.mu.Unlock()

	_, sharded := rs.GetShardingTag(measurement)
	numberOfShards := rs.getNumberOfShards(measurement, replica)

	log.Infof("start rebalance for measurement: %s, replica: %d, sharded: %v, shards/nodes: %d", measurement, replica, sharded, numberOfShards)

	var wg sync.WaitGroup
	var err error
	for i := int32(0); i < numberOfShards; i++ {
		wg.Add(1)

		go func(shard int32) {
			var shardErr error
			if sharded { // 对于已分片的, 检查并移动 series
				shardErr = rs.scanSeries(measurement, shard, func(measurement string, shard int32, shardingTag string, tagValue uint64) error {
					destShard := jumpHashForReplica(tagValue, replica, numberOfShards)
					log.Debugf("move series %s.%d from %d to %v", measurement, tagValue, shard, destShard[replica:])
					if e := rs.dmgr.CopySeries(measurement, shard, destShard[replica:], shardingTag, tagValue); e != nil {
						return e
					}
					return rs.dmgr.RemoveSeries(measurement, shard, shardingTag, tagValue)
				})
			} else if rs.isDirty(measurement, shard, 0) {
				// 对于未分片的, 检查并移动 measurement
				destShard := jumpHashForReplica(str2key(measurement), replica, numberOfShards)
				log.Debugf("move measurement %s from %d to %v", measurement, shard, destShard[replica:])
				shardErr = rs.dmgr.CopyMeasurement(shard, destShard[replica:], measurement)
				if shardErr == nil {
					shardErr = rs.dmgr.RemoveMeasurement(shard, measurement)
				}
			}

			err = multierr.Append(err, shardErr)
			wg.Done()
		}(i)
	}
	wg.Wait()

	log.Infof("rebalance done, measurement: %s, replica: %d, err: %v", measurement, replica, err)

	rs.mu.Lock()
	rs.state[measurement][replica] = balanced
	rs.mu.Unlock()
	return err
}

func (rs *ReplicaSharder) CleanupForAll() error {
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			err = multierr.Append(err, rs.Cleanup(m))
			wg.Done()
		}(measurement)
	}
	wg.Wait()
	return err
}

// 清理位置不正确的 measurement（未分片） 或 series（已分片）
func (rs *ReplicaSharder) Cleanup(measurement string) error {
	numberOfShards := int32(0) // 最大分片数，也就是节点数
	for replica := 0; replica < rs.getNumberOfReplicas(measurement); replica++ {
		if numberOfShards < rs.getNumberOfShards(measurement, replica) {
			numberOfShards = rs.getNumberOfShards(measurement, replica)
		}
	}
	_, sharded := rs.GetShardingTag(measurement)
	var err error
	for shard := int32(0); shard < numberOfShards; shard++ {
		if sharded {
			err = multierr.Append(err, rs.scanSeries(measurement, shard, rs.dmgr.RemoveSeries))
		} else if rs.isDirty(measurement, shard, 0) {
			err = multierr.Append(err, rs.dmgr.RemoveMeasurement(shard, measurement))
		}
	}
	return err
}

func (rs *ReplicaSharder) scanSeries(measurement string, shard int32, fn func(string, int32, string, uint64) error) error {
	var err error
	err = multierr.Append(err, rs.dmgr.ScanTagValues(measurement, shard, rs.shardingTag[measurement], func(tagValues []uint64) bool {
		for _, tagValue := range tagValues {
			if rs.isDirty(measurement, shard, tagValue) {
				err = multierr.Append(err, fn(measurement, shard, rs.shardingTag[measurement], tagValue))
				// TODO maybe too many errors ?
			}
		}
		return false
	}))

	return err
}

// 如果 measurement 的所有 replica 下, 都不应该分配到 shard 上, 则可以判断是一份脏数据, 需要迁移或删除
func (rs *ReplicaSharder) isDirty(measurement string, shard int32, shardingTagValue uint64) bool {
	if _, sharded := rs.GetShardingTag(measurement); !sharded {
		shardingTagValue = str2key(measurement)
	}

	for i := 0; i < rs.getNumberOfReplicas(measurement); i++ {
		if int32(shard) == jumpHashForReplica(shardingTagValue, i, rs.getNumberOfShards(measurement, i))[i] {
			return false
		}
	}
	return true
}

func (rs *ReplicaSharder) isRebalancing(measurement string, replica int) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.state[measurement][replica] == rebalancing
}

// 获取指定 shardingTag 数据的所有分片位置, 并按是否在 rebalancing 排序, 调用者可以优先查询稳定的分片
// TODO preferred
func (rs *ReplicaSharder) GetAllocatedShards(measurement string, shardingTagValue uint64) []int {
	replicas := rand.Perm(rs.getNumberOfReplicas(measurement)) // replica 的顺序随机, 从而达到负载均衡读
	sort.Slice(replicas, func(i, j int) bool {
		return !rs.isRebalancing(measurement, replicas[i]) && rs.isRebalancing(measurement, replicas[j])
	})
	shards := make([]int, len(replicas)) // 计算各个 replica 中, shardingTagValue 对应的分片/节点位置
	if _, sharded := rs.GetShardingTag(measurement); !sharded {
		shardingTagValue = str2key(measurement)
	}
	for i, replica := range replicas {
		shards[i] = int(jumpHashForReplica(shardingTagValue, replica, rs.getNumberOfShards(measurement, replica))[replica])
	}
	return shards
}
