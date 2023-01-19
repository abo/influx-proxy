package dm

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/abo/influx-proxy/backend"
)

type Manager struct {
	nodes        []*backend.Backend
	dbs          map[string]struct{}
	measurements map[string]struct{}
	transfer     *Transfer
}

func NewManager(nodes []*backend.Backend, measurements []string) *Manager {
	mgr := &Manager{
		nodes:        nodes,
		measurements: make(map[string]struct{}),
		dbs:          make(map[string]struct{}),
	}
	for _, m := range measurements {
		if strings.HasSuffix(m, ".*") {
			mgr.dbs[strings.TrimRight(m, ".*")] = struct{}{}
		} else {
			mgr.measurements[m] = struct{}{}
		}
	}

	mgr.transfer = NewTransfer()
	return mgr
}

func (mgr *Manager) IsManagedMeasurement(db, measurement string) bool {
	if _, ok := mgr.dbs[db]; ok {
		return true
	}
	_, ok := mgr.measurements[db+"."+measurement]
	return ok
}

func (mgr *Manager) GetManagedMeasurements() []string {
	measurements := make(map[string]struct{})
	if len(mgr.dbs) > 0 {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, node := range mgr.nodes {
			wg.Add(1)
			go func(b *backend.Backend) {
				for db := range mgr.dbs {
					measurementsInDb := b.GetMeasurements(db)
					mu.Lock()
					for _, measurement := range measurementsInDb {
						measurements[db+"."+measurement] = struct{}{}
					}
					mu.Unlock()
				}
				wg.Done()
			}(node)
		}
		wg.Wait()
	}
	for k, v := range mgr.measurements {
		measurements[k] = v
	}
	results := make([]string, 0, len(measurements))
	for m := range measurements {
		results = append(results, m)
	}
	return results
}

func parseDbAndMeasurement(dbAndMeasurement string) (db, measurement string, err error) {
	parts := strings.SplitN(dbAndMeasurement, ".", 2)
	if len(parts) < 2 {
		return "", "", fmt.Errorf("invalid measurement name %s", dbAndMeasurement)
	}
	return parts[0], parts[1], nil
}

// 扫描指定节点的原始数据，获取所有 shardingTag 值
func (mgr *Manager) ScanTagValues(dbAndMeasurement string, shard int32, tagName string, fn func([]uint64) bool) error {
	db, measurement, err := parseDbAndMeasurement(dbAndMeasurement)
	if err != nil {
		return err
	}

	limit := 512
	for offset := 0; ; offset += limit {
		strvals := mgr.nodes[shard].GetTagValues(db, measurement, tagName, limit, offset)
		if len(strvals) == 0 {
			break
		}
		results := make([]uint64, 0, len(strvals))
		for _, strval := range strvals {
			if val, err := strconv.ParseUint(strval, 10, 64); err == nil {
				results = append(results, val)
			}
		}
		if fn(results) {
			break
		}
	}

	return nil
}

// 将 measurement 中 shardingTagValue 对应的所有原始数据，从 srcShard 迁移到 destShard
func (mgr *Manager) CopySeries(dbAndMeasurement string, src int32, dest []int32, tagName string, tagValue uint64) error {
	srcNode := mgr.nodes[src]
	destNodes := make([]*backend.Backend, 0, len(dest))
	for _, n := range dest {
		if src != n {
			destNodes = append(destNodes, mgr.nodes[n])
		}
	}

	db, measurement, err := parseDbAndMeasurement(dbAndMeasurement)
	if err != nil {
		return fmt.Errorf("cannot copy series: %w", err)
	}

	return mgr.transfer.CopySeries(srcNode, destNodes, db, measurement, tagName, tagValue)
}

func (mgr *Manager) RemoveSeries(dbAndMeasurement string, shard int32, tagName string, tagValue uint64) error {
	db, measurement, err := parseDbAndMeasurement(dbAndMeasurement)
	if err != nil {
		return fmt.Errorf("cannot remove series: %w", err)
	}

	_, err = mgr.nodes[shard].DropSeries(db, measurement, tagName, tagValue)
	return err
}

// 从 srcNode 中将指定 Measurement 整体复制到 destNode
func (mgr *Manager) CopyMeasurement(src int32, dest []int32, dbAndMeasurement string) error {
	srcNode := mgr.nodes[src]
	destNodes := make([]*backend.Backend, 0, len(dest))
	for _, n := range dest {
		if src != n {
			destNodes = append(destNodes, mgr.nodes[n])
		}
	}

	db, measurement, err := parseDbAndMeasurement(dbAndMeasurement)
	if err != nil {
		return fmt.Errorf("cannot copy measurement: %w", err)
	}

	mgr.transfer.CopyMeasurement(srcNode, destNodes, db, measurement, 0)
	return nil
}

func (mgr *Manager) RemoveMeasurement(node int32, dbAndMeasurement string) error {
	db, measurement, err := parseDbAndMeasurement(dbAndMeasurement)
	if err != nil {
		return fmt.Errorf("cannot remove %s from %d: %w", dbAndMeasurement, node, err)
	}
	_, err = mgr.nodes[node].DropMeasurement(db, measurement)
	return err
}
