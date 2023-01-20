package sharding_test

import (
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/abo/influx-proxy/sharding"
)

type dummyDM struct {
	measurements        []string
	fnGetTagValues      func(string, int32, string) ([]uint64, error)
	fnCopySeries        func(string, int32, []int32, string, uint64) error
	fnRemoveSeries      func(string, int32, string, uint64) error
	fnCopyMeasurement   func(int32, []int32, string) error
	fnRemoveMeasurement func(int32, string) error
}

func (dm *dummyDM) GetManagedMeasurements() []string {
	return dm.measurements
}

func (dm *dummyDM) IsManagedMeasurement(measurement string) bool {
	for i := 0; i < len(dm.measurements); i++ {
		if dm.measurements[i] == measurement {
			return true
		}
	}
	return false
}

func (dm *dummyDM) ScanTagValues(measurement string, shard int32, tagName string, fn func([]uint64) bool) error {
	if dm.fnGetTagValues != nil {
		tagValues, err := dm.fnGetTagValues(measurement, shard, tagName)
		if err != nil {
			return err
		}
		fn(tagValues)
	}
	return nil
}
func (dm *dummyDM) CopySeries(measurement string, fromShard int32, toShard []int32, tagName string, shardingTagValue uint64) error {
	if dm.fnCopySeries != nil {
		return dm.fnCopySeries(measurement, fromShard, toShard, tagName, shardingTagValue)
	}
	return nil
}
func (dm *dummyDM) RemoveSeries(measurement string, shard int32, tagName string, shardingTagValue uint64) error {
	if dm.fnRemoveSeries != nil {
		return dm.fnRemoveSeries(measurement, shard, tagName, shardingTagValue)
	}
	return nil
}
func (dm *dummyDM) CopyMeasurement(fromShard int32, toShard []int32, measurement string) error {
	if dm.fnCopyMeasurement != nil {
		return dm.fnCopyMeasurement(fromShard, toShard, measurement)
	}
	return nil
}
func (dm *dummyDM) RemoveMeasurement(shard int32, measurement string) error {
	if dm.fnRemoveMeasurement != nil {
		return dm.fnRemoveMeasurement(shard, measurement)
	}
	return nil
}

func TestShard(t *testing.T) {
	sharder := sharding.NewSharder(&dummyDM{
		measurements: []string{"mA", "mB"},
	}, []*sharding.Config{{Name: "mA", Tag: "A"}, {Name: "mB", Tag: "B"}})

	sharder.Init(2, 2)
	balanceCounter := []int{0, 0}
	times := 10000
	for i := 0; i < times; i++ {
		key := rand.Uint64()
		shards := sharder.GetAllocatedShards("mA", key)
		if len(shards) != 2 {
			t.Fatalf("expected 2 shards, actual %v, shardingKey %d", shards, key)
		}
		balanceCounter[shards[0]]++
	}

	if !isBalance(balanceCounter[0], balanceCounter[1]) {
		t.Fatalf("not balance %d/%d", balanceCounter[0], balanceCounter[1])
	}
}

func isBalance(a, b int) bool {
	total := a + b
	delta := total / 100 // 差异在 1% 以内
	return (a-b) < delta && (b-a) < delta
}

func TestScale(t *testing.T) {
	// 初始数据都在 shard0 上
	data := map[int]map[uint64]struct{}{
		0: make(map[uint64]struct{}),
	}
	for i := uint64(0); i < 10000; i++ {
		data[0][i] = struct{}{}
	}
	var mu sync.RWMutex

	scanShardingTagValues := func(m string, i int32, t string) ([]uint64, error) {
		mu.RLock()
		results := make([]uint64, 0, len(data[int(i)]))
		for k := range data[int(i)] {
			results = append(results, k)
		}
		mu.RUnlock()
		sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
		return results, nil
	}
	sharder := sharding.NewSharder(&dummyDM{
		measurements:   []string{"mA"},
		fnGetTagValues: scanShardingTagValues,
		fnCopySeries: func(m string, from int32, to []int32, t string, key uint64) error {
			mu.Lock()
			for _, i := range to {
				data[int(i)][key] = struct{}{}
			}
			mu.Unlock()
			return nil
		},
		fnRemoveSeries: func(m string, shard int32, t string, key uint64) error {
			mu.Lock()
			delete(data[int(shard)], key)
			mu.Unlock()
			return nil
		},
	}, []*sharding.Config{{Name: "mA", Tag: "A"}})
	sharder.Init(1, 1)

	// 扩展到 2 分片，应该有一半数据迁移到 shard1
	data[1] = make(map[uint64]struct{})
	err := sharder.Scale(2)
	if err != nil {
		t.Fatalf("scale up failed: %v", err)
	}
	if !isBalance(len(data[0]), len(data[1])) {
		t.Fatalf("not balance after scale to 2 shards %d/%d", len(data[0]), len(data[1]))
	}

	// 扩展到 3 分片，各分片数量均衡
	data[2] = make(map[uint64]struct{})
	err = sharder.Scale(3)
	if err != nil {
		t.Fatalf("scale up failed: %v", err)
	}
	if !isBalance(len(data[0]), len(data[2])) || !isBalance(len(data[1]), len(data[2])) {
		t.Fatalf("not balance after scale to 3 shards %d/%d/%d", len(data[0]), len(data[1]), len(data[2]))
	}
}

func TestReplicate(t *testing.T) {

}
