// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package influxproxy

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/dm"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/sharding"
	"github.com/influxdata/influxdb1-client/models"
)

type Proxy struct {
	nodes   []*backend.Backend // InfluxNodes
	dmgr    *dm.Manager
	sharder *sharding.ReplicaSharder
}

func NewProxy(cfg *backend.Config, nodes []*backend.Backend, dmgr *dm.Manager, sharder *sharding.ReplicaSharder) *Proxy {

	return &Proxy{nodes: nodes, dmgr: dmgr, sharder: sharder}
}

func (proxy *Proxy) GetAllocatedNodes(database, measurement string, parseTag ...func(tagName string) (string, error)) ([]*backend.Backend, error) {
	var shards []int
	if shardingTagName, sharded := proxy.sharder.GetShardingTag(database + "." + measurement); !sharded {
		shards = proxy.sharder.GetAllocatedShards(database+"."+measurement, "")
		log.Debugf("%s.%s allocated at %v", database, measurement, shards)
	} else if len(parseTag) == 0 {
		return proxy.nodes, nil
	} else if key, err := parseTag[0](shardingTagName); err != nil {
		return nil, fmt.Errorf("cannot determine allocated node: %w", err)
	} else {
		shards = proxy.sharder.GetAllocatedShards(database+"."+measurement, key)
		log.Debugf("%s.%s.%s allocated at %v", database, measurement, key, shards)
	}
	nodes := make([]*backend.Backend, len(shards))
	for i, shard := range shards {
		nodes[i] = proxy.nodes[shard]
	}
	return nodes, nil
}

func (ip *Proxy) GetAllBackends() []*backend.Backend {
	return ip.nodes
}

func (ip *Proxy) SetDataNodes(nodes []*backend.Backend) {
	ip.nodes = nodes
}

func (ip *Proxy) QueryFlux(w http.ResponseWriter, req *http.Request, qr *backend.QueryRequest) (err error) {
	var bucket, meas string
	if qr.Query != "" {
		bucket, meas, err = backend.ScanQuery(qr.Query)
	} else if qr.Spec != nil {
		bucket, meas, err = backend.ScanSpec(qr.Spec)
	}
	if err != nil {
		return
	}
	if bucket == "" {
		return backend.ErrGetBucket
	} else if meas == "" {
		return backend.ErrGetMeasurement
	} else if !ip.dmgr.IsManagedMeasurement(bucket + "." + meas) {
		return fmt.Errorf("measurement forbidden: %s.%s", bucket, meas)
	}
	// TODO 解析 flux 查询中的 shardingTag 并按其值路由
	if nodes, err := ip.GetAllocatedNodes(bucket, meas); err != nil {
		return err
	} else {
		return backend.QueryFlux(w, req, nodes)
	}
}

func (ip *Proxy) Query(w http.ResponseWriter, req *http.Request) (body []byte, err error) {
	q := strings.TrimSpace(req.FormValue("q"))
	if q == "" {
		return nil, backend.ErrEmptyQuery
	}

	tokens, check, from := backend.CheckQuery(q)
	if !check {
		return nil, backend.ErrIllegalQL
	}

	checkDb, showDb, alterDb, db := backend.CheckDatabaseFromTokens(tokens)
	if !checkDb {
		db, _ = backend.GetDatabaseFromTokens(tokens)
		if db == "" {
			db = req.FormValue("db")
		}
	}
	if !showDb && db == "" {
		return nil, backend.ErrDatabaseNotFound
	}

	selectOrShow := backend.CheckSelectOrShowFromTokens(tokens)
	if selectOrShow && from {
		// 如果 measurement 未分片，则按 db+measurement 路由（分表）
		// 如果 measurement 已分片，则判断查询中是否带了 shardingKey 匹配条件，路由到正确的分片
		// 否则报错
		measurement, err := backend.GetMeasurementFromTokens(tokens)
		if err != nil {
			return nil, backend.ErrGetMeasurement
		}
		if !ip.dmgr.IsManagedMeasurement(db + "." + measurement) {
			return nil, fmt.Errorf("measurement forbidden: %s.%s", db, measurement)
		}

		if nodes, err := ip.GetAllocatedNodes(db, measurement, func(tagName string) (string, error) {
			return parseTagValueFromQuery(tagName, tokens)
		}); err == nil {
			return backend.QueryFromQL(w, req, nodes)
		} else if backend.CheckShowFromTokens(tokens) {
			// 如果不能解析 shardingTag, 对于 show xxx 请求发送到所有分片, 例如 show tag values from meas
			return backend.QueryShowQL(w, req, ip.GetAllBackends(), tokens)
		} else {
			return nil, backend.ErrShardingTagNotFound
		}
	} else if selectOrShow && !from {
		return backend.QueryShowQL(w, req, ip.GetAllBackends(), tokens)
	} else if backend.CheckDeleteOrDropMeasurementFromTokens(tokens) {
		// 如果删除某个 shardingTag 对应的数据 (series), 按分片路由
		// 如果不指定 shardingTag, 请求发送到所有分片
		measurement, err := backend.GetMeasurementFromTokens(tokens)
		if err != nil {
			return nil, err
		}
		if nodes, err := ip.GetAllocatedNodes(db, measurement, func(tagName string) (string, error) {
			return parseTagValueFromQuery(tagName, tokens)
		}); err == nil {
			return backend.QueryAll(req, w, nodes)
		} else {
			return backend.QueryAll(req, w, ip.GetAllBackends())
		}
	} else if alterDb || backend.CheckRetentionPolicyFromTokens(tokens) {
		return backend.QueryAll(req, w, ip.GetAllBackends())
	}
	return nil, backend.ErrUnsupportQL
}

func (ip *Proxy) Write(p []byte, db, rp, precision string) (err error) {
	var (
		pos   int
		block []byte
	)
	for pos < len(p) {
		pos, block = backend.ScanLine(p, pos)
		pos++

		if len(block) == 0 {
			continue
		}
		start := backend.SkipWhitespace(block, 0)
		if start >= len(block) || block[start] == '#' {
			continue
		}
		if block[len(block)-1] == '\n' {
			block = block[:len(block)-1]
		}

		line := make([]byte, len(block[start:]))
		copy(line, block[start:])
		ip.WriteRow(line, db, rp, precision)
	}
	return
}

func (ip *Proxy) WriteRow(line []byte, db, rp, precision string) {
	nanoLine := backend.AppendNano(line, precision)
	measurement, err := backend.ScanKey(nanoLine)
	if err != nil {
		log.Warnf("scan key error: %s", err)
		return
	}
	if !backend.RapidCheck(nanoLine[len(measurement):]) {
		log.Warnf("invalid format, db: %s, rp: %s, precision: %s, line: %s", db, rp, precision, string(line))
		return
	}

	backends, err := ip.GetAllocatedNodes(db, measurement, func(tagName string) (string, error) {
		return parseTagValueFromLine(tagName, nanoLine)
	})
	if err != nil || len(backends) == 0 {
		log.Warnf("write data error: can't get backends, db: %s, meas: %s", db, measurement)
		return
	}

	point := &backend.LinePoint{db, rp, nanoLine}
	for _, be := range backends {
		err = be.WritePoint(point)
		if err != nil {
			log.Warnf("write data to buffer error: %s, url: %s, db: %s, rp: %s, precision: %s, line: %s", err, be.Url, db, rp, precision, string(line))
		}
	}
}

func parseTagValueFromQuery(tagName string, tokens []string) (string, error) {
	shardingTagValue, err := backend.GetConditionFromTokens(tokens, tagName)
	if err != nil {
		return "", err
	}
	return shardingTagValue, nil
}

func parseTagValueFromLine(tagName string, line []byte) (string, error) {
	shardingTagValue, err := backend.ScanTagValue(line, tagName)
	if err != nil {
		return "", err
	}
	return shardingTagValue, nil
}

func parseTagValueFromPoint(tagName string, point models.Point) (string, error) {
	exists := false
	var shardingTagValue string
	point.ForEachTag(func(k, v []byte) bool {
		if bytes.Equal(k, []byte(tagName)) {
			exists = true
			shardingTagValue = string(v)
			return false
		}
		return true
	})
	if !exists {
		return "", backend.ErrShardingTagNotFound
	}
	return shardingTagValue, nil
}

func (ip *Proxy) WritePoints(points []models.Point, db, rp string) error {
	var err error
	for _, pt := range points {
		meas := string(pt.Name())
		backends, err := ip.GetAllocatedNodes(db, meas, func(tagName string) (string, error) {
			return parseTagValueFromPoint(tagName, pt)
		})
		if err != nil || len(backends) == 0 {
			log.Warnf("write point error: can't get backends, db: %s, meas: %s, err: %v", db, meas, err)
			if err == nil {
				err = backend.ErrEmptyBackends
			}
			continue
		}

		point := &backend.LinePoint{db, rp, []byte(pt.String())}
		for _, be := range backends {
			err = be.WritePoint(point)
			if err != nil {
				log.Warnf("write point to buffer error: %s, url: %s, db: %s, rp: %s, point: %s", err, be.Url, db, rp, pt.String())
			}
		}
	}
	return err
}

func (ip *Proxy) ReadProm(w http.ResponseWriter, req *http.Request, db, metric string) (err error) {
	// TODO 解析 prometheus 请求中的 shardingTag 并按其值路由
	nodes, err := ip.GetAllocatedNodes(db, metric)
	if err != nil {
		return err
	}

	return backend.ReadProm(w, req, nodes)
}

func (ip *Proxy) Close() {
	for _, n := range ip.nodes {
		n.Close()
	}
}
