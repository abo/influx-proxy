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

// 对 measurement 的 replica 进行分片, 计算每条数据的分片位置, 以及触发对应的迁移/均衡
// 对于不分片的 measurement, 当作分片数 1 处理,
type ReplicaSharder struct {
	dmgr                    DataManager
	defaultNumberOfReplicas int
	defaultNumberOfShards   int
	shardingTag             map[string]string
	replicas                map[string][]*Replica
	mu                      sync.RWMutex // mu protects meta & state
}

func NewSharder(dmgr DataManager, cfg []*Config) *ReplicaSharder {
	tags := make(map[string]string)
	for _, measurement := range cfg {
		tags[measurement.Name] = measurement.Tag
	}

	rand.Seed(time.Now().Unix())
	return &ReplicaSharder{dmgr: dmgr, shardingTag: tags, replicas: make(map[string][]*Replica)}
}

// Init a sharded cluster's metadata
// 对于 sharding measurement, 默认按节点数分片, 也就是每个节点有一部分数据
// 对于 unsharding measurement, 整表在节点间复制和均衡, 相当于分片数是 1, 也就是某些分片为空
func (rs *ReplicaSharder) Init(numberOfNodes, defaultNumberOfReplicas int) {
	rs.defaultNumberOfReplicas = defaultNumberOfReplicas
	rs.defaultNumberOfShards = numberOfNodes
	rs.mu.Lock()
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		rs.replicas[measurement] = make([]*Replica, defaultNumberOfReplicas)
		for replica := 0; replica < defaultNumberOfReplicas; replica++ {
			rs.replicas[measurement][replica] = &Replica{int32(numberOfNodes), balanced}
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
	replicas, prs := rs.replicas[measurement]
	rs.mu.RUnlock()

	// 在 managed measurement 使用通配符, 并且 Init 时 db 中没有数据, 后续写入时会找不到元信息, 因此需要延迟初始化
	if !prs && rs.dmgr.IsManagedMeasurement(measurement) {
		replicas = make([]*Replica, rs.defaultNumberOfReplicas)
		for i := 0; i < len(replicas); i++ {
			replicas[i] = &Replica{int32(rs.defaultNumberOfShards), balanced}
		}

		rs.mu.Lock()
		rs.replicas[measurement] = replicas
		rs.mu.Unlock()
	}
	return len(replicas)
}

// 对于分片的 measurement 返回其分片数; 对于不分片的 measurement 返回其节点数
func (rs *ReplicaSharder) getNumberOfShards(measurement string, replica int) int32 {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.replicas[measurement][replica].shards
}

// 对所有 measurement 的所有 replica, 调整分片数并执行 rebalance
// TODO 如果是 2 shard 2 replica, 其实只要在一个节点上执行就够了，因为任一节点有完整数据；推而广之 shards == replicas 时，任一节点有完整数据
// 或者使用一个hashmap 或 bloom filter 记录 ?
func (rs *ReplicaSharder) Scale(numberOfShards int) error {
	log.Infof("start scale to %d shard(s)", numberOfShards)
	rs.defaultNumberOfShards = numberOfShards
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			for replica := 0; replica < rs.getNumberOfReplicas(m); replica++ {
				if rs.getNumberOfShards(m, replica) == int32(numberOfShards) {
					continue
				}
				rs.mu.Lock()
				rs.replicas[m][replica].shards = int32(numberOfShards)
				rs.mu.Unlock()
				err = multierr.Append(err, rs.Rebalance(m, replica))
			}
		}(measurement)

	}
	wg.Wait()
	log.Infof("scale done, target: %d shard(s), err: %v", numberOfShards, err)
	return err
}

// 调整复制数, 跨 replica 复制数据
func (rs *ReplicaSharder) Replicate(numberOfReplicas int) error {
	rs.defaultNumberOfReplicas = numberOfReplicas
	var err error
	var wg sync.WaitGroup
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		wg.Add(1)
		go func(m string) {
			defer wg.Done()
			currentReplicas := rs.getNumberOfReplicas(m)
			if currentReplicas == numberOfReplicas {
				return
			} else if currentReplicas > numberOfReplicas {
				err = multierr.Append(err, rs.shrinkReplica(m, numberOfReplicas))
			} else {
				err = multierr.Append(err, rs.expandReplica(m, numberOfReplicas))
			}
		}(measurement)
	}
	wg.Wait()
	return err
}

// 删除多余的 replica, 仅保留 numberOfReplicas 个
func (rs *ReplicaSharder) shrinkReplica(measurement string, numberOfReplicas int) error {
	log.Infof("shrink %s to %d replica(s)", measurement, numberOfReplicas)
	// 删除元数据并清理原始数据
	rs.mu.Lock()
	rs.replicas[measurement] = rs.replicas[measurement][:numberOfReplicas]
	rs.mu.Unlock()

	return rs.Cleanup(measurement)
}

// 增加更多 replica
func (rs *ReplicaSharder) expandReplica(measurement string, numberOfReplicas int) error {
	log.Infof("expand %s to %d replica(s)", measurement, numberOfReplicas)
	rs.mu.Lock()
	originReplicas := len(rs.replicas[measurement])
	for len(rs.replicas[measurement]) < numberOfReplicas {
		rs.replicas[measurement] = append(rs.replicas[measurement], &Replica{int32(rs.defaultNumberOfShards), rebalancing})
	}
	rs.mu.Unlock()

	var err error

	if tagName, sharded := rs.GetShardingTag(measurement); sharded {
		// 对于已分片的 measurement, 由于数据分散在多个节点, 从第一个 replica 的各个 shard 复制数据
		for shard := int32(0); shard < rs.getNumberOfShards(measurement, 0); shard++ {
			err = multierr.Append(err, rs.dmgr.ScanTagValues(measurement, shard, tagName, func(tagValues []string) bool {
				for _, tagValue := range tagValues {
					destShards := jumpHashForReplica(str2key(tagValue), numberOfReplicas-1, int32(rs.defaultNumberOfShards))
					err = multierr.Append(err, rs.dmgr.CopySeries(measurement, shard, destShards[originReplicas:], tagName, tagValue))
				}
				return false
			}))
		}
	} else {
		// 对于未分片的整体复制
		destNodes := jumpHashForReplica(str2key(measurement), numberOfReplicas-1, int32(rs.defaultNumberOfShards))
		err = multierr.Append(err, rs.dmgr.CopyMeasurement(destNodes[0], destNodes[originReplicas:], measurement))
	}

	rs.mu.Lock()
	for replica := originReplicas; replica < numberOfReplicas; replica++ {
		rs.replicas[measurement][replica].state = balanced
	}
	rs.mu.Unlock()
	return err
}

// 修复指定分片中的数据
func (rs *ReplicaSharder) Repair(brokenShard int) error {
	log.Infof("start repair broken shard %d", brokenShard)
	var err error
	for _, measurement := range rs.dmgr.GetManagedMeasurements() {
		if _, sharded := rs.GetShardingTag(measurement); sharded {
			err = multierr.Append(err, rs.repairShardedMeasurement(measurement, int32(brokenShard)))
		} else {
			err = multierr.Append(err, rs.repairUnshardedMeasurement(measurement, int32(brokenShard)))
		}
	}
	log.Infof("repair done, err: %v", err)
	return err
}

func (rs *ReplicaSharder) repairShardedMeasurement(measurement string, brokenShard int32) error {
	var err error
	replicas := rs.getNumberOfReplicas(measurement)
	shardingTag, _ := rs.GetShardingTag(measurement)
	log.Infof("check %s (sharded) for repair %d, replicas: %d", measurement, brokenShard, replicas)
	// 每个 measurement 只要选择 2 个 replica 就能恢复该 measurement 在 shard 上的完整数据
	for i := 0; i < replicas && i < 2; i++ {
		shards := rs.getNumberOfShards(measurement, i)
		for shard := int32(0); shard < shards; shard++ {
			if shard == brokenShard {
				continue
			}
			log.Infof("repaire %s in %d, scan series at %d", measurement, brokenShard, shard)
			err = multierr.Append(err, rs.dmgr.ScanTagValues(measurement, shard, shardingTag, func(tagValues []string) bool {
				for _, tagValue := range tagValues {
					knownNeedRepair := false
					// 是否有某个 replica 下, 该数据分片到 brokenShard
					for j := 0; j < replicas && !knownNeedRepair; j++ {
						if j == i {
							continue
						}
						destShard := jumpHashForReplica(str2key(tagValue), j, rs.getNumberOfShards(measurement, j))
						knownNeedRepair = destShard[j] == brokenShard
					}
					if knownNeedRepair {
						log.Debugf("repair %s.%s in %d, copy series from %d", measurement, tagValue, brokenShard, shard)
						err = multierr.Append(err, rs.dmgr.CopySeries(measurement, shard, []int32{brokenShard}, shardingTag, tagValue))
					}
				}
				return false
			}))
		}
	}
	return err
}

func (rs *ReplicaSharder) repairUnshardedMeasurement(measurement string, brokenShard int32) error {
	replicas := rs.getNumberOfReplicas(measurement)
	log.Infof("check %s (unsharded) for repair %d, replicas: %d", measurement, brokenShard, replicas)
	knownNeedRepair := false
	from := int32(-1)
	// 计算是否该 measurement 的某 replica 在 brokenShard 上
	for i := 0; i < replicas && (!knownNeedRepair || from == -1); i++ {
		shard := jumpHashForReplica(str2key(measurement), i, rs.getNumberOfShards(measurement, i))[i]
		if shard == int32(brokenShard) {
			knownNeedRepair = true
		} else {
			from = shard
		}
	}

	if knownNeedRepair && from != -1 {
		log.Infof("repair %s in %d, copy measurement from %d", measurement, brokenShard, from)
		return rs.dmgr.CopyMeasurement(from, []int32{int32(brokenShard)}, measurement)
	}
	return nil
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
	if rs.replicas[measurement][replica].state == rebalancing {
		rs.mu.Unlock()
		return fmt.Errorf("sharder: measurement(%s) 's replica(%d) is rebalancing", measurement, replica)
	}
	rs.replicas[measurement][replica].state = rebalancing
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
				shardErr = rs.scanSeries(measurement, shard, func(measurement string, shard int32, shardingTag string, tagValue string) error {
					destShard := jumpHashForReplica(str2key(tagValue), replica, numberOfShards)
					log.Debugf("move series %s.%s from %d to %v", measurement, tagValue, shard, destShard[replica:])
					if e := rs.dmgr.CopySeries(measurement, shard, destShard[replica:], shardingTag, tagValue); e != nil {
						return e
					}
					return rs.dmgr.RemoveSeries(measurement, shard, shardingTag, tagValue)
				})
			} else if rs.isDirty(measurement, shard, "") {
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
	rs.replicas[measurement][replica].state = balanced
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
	log.Infof("start cleanup for %s", measurement)
	numberOfShards := int32(0) // 最大分片数，也就是节点数
	for replica := 0; replica < rs.getNumberOfReplicas(measurement); replica++ {
		if numberOfShards < rs.getNumberOfShards(measurement, replica) {
			numberOfShards = rs.getNumberOfShards(measurement, replica)
		}
	}
	_, sharded := rs.GetShardingTag(measurement)
	var err error
	for shard := int32(0); shard < numberOfShards; shard++ {
		log.Infof("cleanup %s (sharded=%v) from %d", measurement, sharded, shard)
		if sharded {
			err = multierr.Append(err, rs.scanSeries(measurement, shard, rs.dmgr.RemoveSeries))
		} else if rs.isDirty(measurement, shard, "") {
			err = multierr.Append(err, rs.dmgr.RemoveMeasurement(shard, measurement))
		}
	}
	log.Infof("cleanup of %s done, err: %v", measurement, err)
	return err
}

func (rs *ReplicaSharder) scanSeries(measurement string, shard int32, fn func(string, int32, string, string) error) error {
	var err error
	err = multierr.Append(err, rs.dmgr.ScanTagValues(measurement, shard, rs.shardingTag[measurement], func(tagValues []string) bool {
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
func (rs *ReplicaSharder) isDirty(measurement string, shard int32, shardingTagValue string) bool {
	if _, sharded := rs.GetShardingTag(measurement); !sharded {
		shardingTagValue = measurement
	}

	for i := 0; i < rs.getNumberOfReplicas(measurement); i++ {
		if int32(shard) == jumpHashForReplica(str2key(shardingTagValue), i, rs.getNumberOfShards(measurement, i))[i] {
			return false
		}
	}
	return true
}

func (rs *ReplicaSharder) isRebalancing(measurement string, replica int) bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.replicas[measurement][replica].state == rebalancing
}

// 获取指定 shardingTag 数据的所有分片位置, 并按是否在 rebalancing 排序, 调用者可以优先查询稳定的分片
// TODO preferred
func (rs *ReplicaSharder) GetAllocatedShards(measurement string, shardingTagValue string) []int {
	replicas := rand.Perm(rs.getNumberOfReplicas(measurement)) // replica 的顺序随机, 从而达到负载均衡读
	sort.Slice(replicas, func(i, j int) bool {
		return !rs.isRebalancing(measurement, replicas[i]) && rs.isRebalancing(measurement, replicas[j])
	})
	shards := make([]int, len(replicas)) // 计算各个 replica 中, shardingTagValue 对应的分片/节点位置
	if _, sharded := rs.GetShardingTag(measurement); !sharded {
		shardingTagValue = measurement
	}
	for i, replica := range replicas {
		shards[i] = int(jumpHashForReplica(str2key(shardingTagValue), replica, rs.getNumberOfShards(measurement, replica))[replica])
	}
	return shards
}
