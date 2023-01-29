// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abo/influx-proxy/log"
	"github.com/panjf2000/ants/v2"
)

type CacheBuffer struct {
	Buffer  *bytes.Buffer
	Counter int
}

type Backend struct {
	*HttpBackend
	fb   *FileBackend
	pool *ants.Pool

	running         atomic.Value
	flushSize       int
	flushTime       int
	rewriteInterval int
	rewriteTicker   *time.Ticker
	chWrite         chan *LinePoint
	chTimer         <-chan time.Time
	buffers         map[string]map[string]*CacheBuffer
	wg              sync.WaitGroup
}

func NewBackend(ID int, cfg *BackendConfig, pxcfg *ProxyConfig) (ib *Backend) {
	ib = &Backend{
		HttpBackend:     NewHttpBackend(cfg, pxcfg),
		flushSize:       pxcfg.FlushSize,
		flushTime:       pxcfg.FlushTime,
		rewriteInterval: pxcfg.RewriteInterval,
		rewriteTicker:   time.NewTicker(time.Duration(pxcfg.RewriteInterval) * time.Second),
		chWrite:         make(chan *LinePoint, 16),
		buffers:         make(map[string]map[string]*CacheBuffer),
	}
	ib.running.Store(true)

	var err error
	ib.fb, err = NewFileBackend(fmt.Sprintf("node%d", ID), pxcfg.DataDir)
	if err != nil {
		panic(err)
	}
	ib.pool, err = ants.NewPool(pxcfg.ConnPoolSize)
	if err != nil {
		panic(err)
	}

	go ib.worker()
	return
}

func NewSimpleBackend(cfg *BackendConfig) *Backend {
	return &Backend{HttpBackend: NewSimpleHttpBackend(cfg)}
}

func (ib *Backend) worker() {
	for ib.IsRunning() {
		select {
		case p, ok := <-ib.chWrite:
			if !ok {
				// closed
				ib.Flush()
				ib.wg.Wait()
				ib.HttpBackend.Close()
				ib.fb.Close()
				ib.pool.Release()
				return
			}
			ib.WriteBuffer(p)

		case <-ib.chTimer:
			ib.Flush()
			if !ib.IsRunning() {
				ib.wg.Wait()
				ib.HttpBackend.Close()
				ib.fb.Close()
				ib.pool.Release()
				return
			}

		case <-ib.rewriteTicker.C:
			ib.RewriteIdle()
		}
	}
}

func (ib *Backend) WritePoint(point *LinePoint) (err error) {
	if !ib.IsRunning() {
		return io.ErrClosedPipe
	}
	ib.chWrite <- point
	return
}

func (ib *Backend) WriteBuffer(point *LinePoint) (err error) {
	db, rp, line := point.Db, point.Rp, point.Line
	// it's thread-safe since ib.buffers is only used (read-write) in ib.worker() goroutine
	if _, ok := ib.buffers[db]; !ok {
		ib.buffers[db] = make(map[string]*CacheBuffer)
	}
	if _, ok := ib.buffers[db][rp]; !ok {
		ib.buffers[db][rp] = &CacheBuffer{Buffer: &bytes.Buffer{}}
	}
	cb := ib.buffers[db][rp]
	cb.Counter++
	if cb.Buffer == nil {
		cb.Buffer = &bytes.Buffer{}
	}
	n, err := cb.Buffer.Write(line)
	if err != nil {
		log.Errorf("buffer write error: %s", err)
		return
	}
	if n != len(line) {
		err = io.ErrShortWrite
		log.Errorf("buffer write error: %s", err)
		return
	}
	if line[len(line)-1] != '\n' {
		err = cb.Buffer.WriteByte('\n')
		if err != nil {
			log.Errorf("buffer write error: %s", err)
			return
		}
	}

	switch {
	case cb.Counter >= ib.flushSize:
		ib.FlushBuffer(db, rp)
	case ib.chTimer == nil:
		ib.chTimer = time.After(time.Duration(ib.flushTime) * time.Second)
	}
	return
}

func (ib *Backend) FlushBuffer(db, rp string) {
	cb := ib.buffers[db][rp]
	if cb.Buffer == nil {
		return
	}
	p := cb.Buffer.Bytes()
	cb.Buffer = nil
	cb.Counter = 0
	if len(p) == 0 {
		return
	}

	ib.wg.Add(1)
	ib.pool.Submit(func() {
		defer ib.wg.Done()
		var buf bytes.Buffer
		err := Compress(&buf, p)
		if err != nil {
			log.Errorf("compress buffer error: ", err)
			return
		}

		p = buf.Bytes()

		if ib.IsActive() {
			err = ib.WriteCompressed(db, rp, p)
			switch err {
			case nil:
				return
			case ErrBadRequest:
				log.Error("bad request, drop all data")
				return
			case ErrNotFound:
				log.Error("bad backend, drop all data")
				return
			default:
				log.Errorf("write http error, url: %s, db: %s, rp: %s, plen: %d", ib.Url, db, rp, len(p))
			}
		}

		b := bytes.Join([][]byte{[]byte(url.QueryEscape(db)), []byte(url.QueryEscape(rp)), p}, []byte{' '})
		err = ib.fb.Write(b)
		if err != nil {
			log.Errorf("write db and data to file error: %s, db: %s, rp: %s, plen: %d", err, db, rp, len(p))
			return
		}
	})
}

func (ib *Backend) Flush() {
	ib.chTimer = nil
	for db := range ib.buffers {
		for rp := range ib.buffers[db] {
			if ib.buffers[db][rp].Counter > 0 {
				ib.FlushBuffer(db, rp)
			}
		}
	}
}

func (ib *Backend) RewriteIdle() {
	if !ib.IsRewriting() && ib.fb.IsData() {
		ib.SetRewriting(true)
		go ib.RewriteLoop()
	}
}

func (ib *Backend) RewriteLoop() {
	for ib.fb.IsData() {
		if !ib.IsRunning() {
			return
		}
		if !ib.IsActive() {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
		err := ib.Rewrite()
		if err != nil {
			time.Sleep(time.Duration(ib.rewriteInterval) * time.Second)
			continue
		}
	}
	ib.SetRewriting(false)
}

func (ib *Backend) Rewrite() (err error) {
	b, err := ib.fb.Read()
	if err != nil {
		log.Errorf("rewrite read file error: ", err)
		return
	}
	if b == nil {
		return
	}

	p := bytes.SplitN(b, []byte{' '}, 3)
	if len(p) < 3 {
		log.Errorf("rewrite read invalid data with length: ", len(p))
		return
	}
	db, err := url.QueryUnescape(string(p[0]))
	if err != nil {
		log.Errorf("rewrite db unescape error: ", err)
		return
	}
	rp, err := url.QueryUnescape(string(p[1]))
	if err != nil {
		log.Errorf("rewrite rp unescape error: ", err)
		return
	}
	err = ib.WriteCompressed(db, rp, p[2])

	switch err {
	case nil:
	case ErrBadRequest:
		log.Error("bad request, drop all data")
		err = nil
	case ErrNotFound:
		log.Error("bad backend, drop all data")
		err = nil
	default:
		log.Errorf("rewrite http error, url: %s, db: %s, rp: %s, plen: %d", ib.Url, db, rp, len(p[1]))

		err = ib.fb.RollbackMeta()
		if err != nil {
			log.Errorf("rollback meta error: %s", err)
		}
		return
	}

	err = ib.fb.UpdateMeta()
	if err != nil {
		log.Errorf("update meta error: %s", err)
	}
	return
}

func (ib *Backend) IsRunning() (b bool) {
	return ib.running.Load().(bool)
}

func (ib *Backend) Close() {
	ib.running.Store(false)
	close(ib.chWrite)
}

func (ib *Backend) GetHealth(withStats bool) interface{} {
	health := struct {
		Url       string      `json:"url"` // nolint:golint
		Active    bool        `json:"active"`
		Backlog   bool        `json:"backlog"`
		Rewriting bool        `json:"rewriting"`
		WriteOnly bool        `json:"write_only"`
		Healthy   bool        `json:"healthy,omitempty"`
		Stats     interface{} `json:"stats,omitempty"`
	}{
		Url:       ib.Url,
		Active:    ib.IsActive(),
		Backlog:   ib.fb.IsData(),
		Rewriting: ib.IsRewriting(),
		WriteOnly: ib.IsWriteOnly(),
	}
	if !withStats {
		return health
	}
	var wg sync.WaitGroup
	var smap sync.Map
	dbs := ib.GetDatabases()
	for _, db := range dbs {
		wg.Add(1)
		go func(db string) {
			defer wg.Done()
			inplace, incorrect := 0, 0
			measurements := ib.GetMeasurements(db)
			// for _, measurement := range measurements {
			// is is managed measurement,

			// }
			// TODO 重新设计该信息，基于元数据 ,  show tag values exact cardinality from measurement with key="customer"
			// 包含哪些 measurement ？ 已分片的/未分片的，
			smap.Store(db, map[string]int{
				"measurements": len(measurements),
				"inplace":      inplace,
				"incorrect":    incorrect,
			})
		}(db)
	}
	wg.Wait()
	healthy := true
	stats := make(map[string]map[string]int)
	smap.Range(func(k, v interface{}) bool {
		sm := v.(map[string]int)
		stats[k.(string)] = sm
		if sm["incorrect"] > 0 {
			healthy = false
		}
		return true
	})
	health.Healthy = healthy
	health.Stats = stats
	return health
}
