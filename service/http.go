// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package service

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"mime"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"

	influxproxy "github.com/abo/influx-proxy"
	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/dm"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/service/prometheus"
	"github.com/abo/influx-proxy/service/prometheus/remote"
	"github.com/abo/influx-proxy/sharding"
	"github.com/abo/influx-proxy/util"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
)

var (
	ErrInvalidTick    = errors.New("invalid tick, require non-negative integer")
	ErrInvalidWorker  = errors.New("invalid worker, require positive integer")
	ErrInvalidBatch   = errors.New("invalid batch, require positive integer")
	ErrInvalidLimit   = errors.New("invalid limit, require positive integer")
	ErrInvalidHaAddrs = errors.New("invalid ha_addrs, require at least two addresses as <host:port>, comma-separated")
)

type HttpService struct { // nolint:golint
	nodes   []*backend.Backend
	proxy   *influxproxy.Proxy
	dmgr    *dm.Manager
	sharder *sharding.ReplicaSharder
	cfg     *backend.Config

	// writeTracing bool
	// queryTracing bool
}

func NewHttpService(cfg *backend.Config) (hs *HttpService) { // nolint:golint
	nodes := make([]*backend.Backend, 0, len(cfg.Nodes))
	for i, nodeCfg := range cfg.Nodes {
		nodes = append(nodes, backend.NewBackend(i, nodeCfg, cfg.Proxy))
	}

	dmgr := dm.NewManager(nodes, cfg.Proxy.Measurements)
	sharder := sharding.NewSharder(dmgr, cfg.Sharding)
	sharder.Init(len(nodes), 1)                              // TODO  by legacy config
	proxy := influxproxy.NewProxy(cfg, nodes, dmgr, sharder) //cfg.Proxy.WriteTracing,  //cfg.Proxy.QueryTracing,

	return &HttpService{
		nodes:   nodes,
		proxy:   proxy,
		dmgr:    dmgr,
		sharder: sharder,
		cfg:     cfg,
	}
}

func (hs *HttpService) Handler() http.Handler {
	r := mux.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Influxdb-Version", backend.Version)
			w.Header().Add("X-Influxdb-Build", "Proxy")
			next.ServeHTTP(w, r)
		})
	})

	// biz
	r.HandleFunc("/ping", hs.HandlerPing)
	r.HandleFunc("/query", hs.HandlerQuery)
	r.HandleFunc("/write", hs.HandlerWrite)
	r.HandleFunc("/api/v2/query", hs.HandlerQueryV2)
	r.HandleFunc("/api/v2/write", hs.HandlerWriteV2)
	r.HandleFunc("/api/v1/prom/read", hs.HandlerPromRead)
	r.HandleFunc("/api/v1/prom/write", hs.HandlerPromWrite)

	// mgmt
	r.HandleFunc("/health", hs.HandlerHealth)
	r.HandleFunc("/encrypt", hs.HandlerEncrypt)
	r.HandleFunc("/decrypt", hs.HandlerDecrypt)
	r.HandleFunc("/rebalance", hs.HandlerRebalance) // 数据再均衡
	r.HandleFunc("/scale", hs.HandlerScale)         // 添加/删除节点, 并同时调整分片数以及再均衡
	r.HandleFunc("/replicate", hs.HandlerReplicate) // 调整备份数
	r.HandleFunc("/repair", hs.HandlerRepair)       // 修复指定节点的数据
	r.HandleFunc("/replace", hs.HandlerReplace)     // 替换节点
	r.HandleFunc("/cleanup", hs.HandlerCleanup)     // 清理位置不正确的数据
	r.HandleFunc("/state", hs.HandlerState)         // 当前状态
	r.HandleFunc("/stats", hs.HandlerStats)         // 数据分布统计

	// debug
	r.HandleFunc("/debug/pprof/{name}", func(resp http.ResponseWriter, req *http.Request) {
		name := mux.Vars(req)["name"]
		pprof.Handler(name).ServeHTTP(resp, req)
	})

	return r
}

func (hs *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (hs *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := hs.proxy.Query(w, req)
	if err != nil {
		log.Errorf("influxql query error: %s, query: %s, db: %s, client: %s", err, q, db, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	hs.WriteBody(w, body)
	log.Debugf("influxql query: %s, db: %s, client: %s", q, db, req.RemoteAddr)
}

func (hs *HttpService) HandlerQueryV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	var contentType = "application/json"
	if ct := req.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rbody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	qr := &backend.QueryRequest{}
	switch mt {
	case "application/vnd.flux":
		qr.Query = string(rbody)
	case "application/json":
		fallthrough
	default:
		if err = json.Unmarshal(rbody, qr); err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("failed parsing request body as JSON: %s", err))
			return
		}
	}

	if qr.Query == "" && qr.Spec == nil {
		hs.WriteError(w, req, http.StatusBadRequest, "request body requires either spec or query")
		return
	}
	if qr.Type != "" && qr.Type != "flux" {
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("unknown query type: %s", qr.Type))
		return
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(rbody))
	err = hs.proxy.QueryFlux(w, req, qr)
	if err != nil {
		log.Errorf("flux query error: %s, query: %s, spec: %s, client: %s", err, qr.Query, qr.Spec, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("flux query: %s, spec: %s, client: %s", qr.Query, qr.Spec, req.RemoteAddr)
}

func (hs *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
		if precision == "" {
			precision = "ns"
		}
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use n, ns, u, ms, s, m or h)", precision))
		return
	}

	db, err := hs.queryDB(req, false)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rp := req.URL.Query().Get("rp")

	hs.writeInternal(db, rp, precision, w, req)
}

func (hs *HttpService) HandlerWriteV2(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	precision := req.URL.Query().Get("precision")
	switch precision {
	case "ns":
		precision = "n"
	case "us":
		precision = "u"
	case "ms", "s", "":
		// same as v1 so do nothing
	default:
		hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision))
		return
	}

	db, rp, err := hs.bucket2dbrp(req.URL.Query().Get("bucket"))
	if err != nil {
		hs.WriteError(w, req, http.StatusNotFound, err.Error())
		return
	}
	// if hs.ip.IsForbiddenDB(db) {
	// 	hs.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("database forbidden: %s", db))
	// 	return
	// }

	hs.writeInternal(db, rp, precision, w, req)
}

func (hs *HttpService) writeInternal(db, rp, precision string, w http.ResponseWriter, req *http.Request) {
	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			hs.WriteError(w, req, http.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	err = hs.proxy.Write(p, db, rp, precision)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
	log.Debugf("write line protocol, db: %s, rp: %s, precision: %s, data: %s, client: %s", db, rp, precision, p, req.RemoteAddr)
}

func (hs *HttpService) HandlerPromRead(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := hs.queryDB(req, true)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		hs.WriteError(w, req, http.StatusInternalServerError, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	var readReq remote.ReadRequest
	if err = proto.Unmarshal(reqBuf, &readReq); err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if len(readReq.Queries) != 1 {
		err = errors.New("prometheus read endpoint currently only supports one query at a time")
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	var metric string
	q := readReq.Queries[0]
	for _, m := range q.Matchers {
		if m.Name == "__name__" {
			metric = m.Value
		}
	}
	if metric == "" {
		log.Infof("prometheus query: %v", q)
		err = errors.New("prometheus metric not found")
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(compressed))
	err = hs.proxy.ReadProm(w, req, db, metric)
	if err != nil {
		log.Warnf("prometheus read error: %s, query: %s %s %v, client: %s", err, req.Method, db, q, req.RemoteAddr)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("prometheus read: %s %s %v, client: %s", req.Method, db, q, req.RemoteAddr)
}

func (hs *HttpService) HandlerPromWrite(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := hs.queryDB(req, false)
	if err != nil {
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rp := req.URL.Query().Get("rp")

	body := req.Body
	var bs []byte
	if req.ContentLength > 0 {
		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, req.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err = buf.ReadFrom(body)
	if err != nil {
		log.Warnf("prom write handler unable to read bytes from request body")
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		log.Warnf("prom write handler unable to snappy decode from request body, error: %s", err)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var writeReq remote.WriteRequest
	if err = proto.Unmarshal(reqBuf, &writeReq); err != nil {
		log.Warnf("prom write handler unable to unmarshal from snappy decoded bytes, error: %s", err)
		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	points, err := prometheus.WriteRequestToPoints(&writeReq)
	if err != nil {
		log.Warnf("prom write handler, error: %s", err)
		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			hs.WriteError(w, req, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Write points.
	err = hs.proxy.WritePoints(points, db, rp)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (hs *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status/100 >= 4 {
		hs.WriteError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (hs *HttpService) WriteError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	w.WriteHeader(status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (hs *HttpService) WriteBody(w http.ResponseWriter, body []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (hs *HttpService) WriteText(w http.ResponseWriter, status int, text string) {
	w.WriteHeader(status)
	w.Write([]byte(text + "\n"))
}

func (hs *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	return hs.checkMethod(w, req, methods...) && hs.checkAuth(w, req)
}

func (hs *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	hs.WriteError(w, req, http.StatusMethodNotAllowed, "method not allow")
	return false
}

func (hs *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if hs.cfg.Proxy.Username == "" && hs.cfg.Proxy.Password == "" {
		return true
	}
	q := req.URL.Query()
	if u, p := q.Get("u"), q.Get("p"); hs.compareAuth(u, p) {
		return true
	}
	if u, p, ok := req.BasicAuth(); ok && hs.compareAuth(u, p) {
		return true
	}
	if u, p, ok := hs.parseAuth(req); ok && hs.compareAuth(u, p) {
		return true
	}
	hs.WriteError(w, req, http.StatusUnauthorized, "authentication failed")
	return false
}

func (hs *HttpService) parseAuth(req *http.Request) (string, string, bool) {
	if auth := req.Header.Get("Authorization"); auth != "" {
		items := strings.Split(auth, " ")
		if len(items) == 2 && items[0] == "Token" {
			token := items[1]
			i := strings.IndexByte(token, ':')
			if i >= 0 {
				return token[:i], token[i+1:], true
			}
		}
	}
	return "", "", false
}

func (hs *HttpService) compareAuth(u, p string) bool {
	return hs.transAuth(u) == hs.cfg.Proxy.Username && hs.transAuth(p) == hs.cfg.Proxy.Password
}

func (hs *HttpService) transAuth(text string) string {
	if hs.cfg.Proxy.AuthEncrypt {
		return util.AesEncrypt(text)
	}
	return text
}

func (hs *HttpService) bucket2dbrp(bucket string) (string, string, error) {
	// test for a slash in our bucket name.
	switch idx := strings.IndexByte(bucket, '/'); idx {
	case -1:
		// if there is no slash, we're mapping bucket to the database.
		switch db := bucket; db {
		case "":
			// if our "database" is an empty string, this is an error.
			return "", "", fmt.Errorf(`bucket name %q is missing a slash; not in "database/retention-policy" format`, bucket)
		default:
			return db, "", nil
		}
	default:
		// there is a slash
		switch db, rp := bucket[:idx], bucket[idx+1:]; {
		case db == "":
			// empty database is unrecoverable
			return "", "", fmt.Errorf(`bucket name %q is in db/rp form but has an empty database`, bucket)
		default:
			return db, rp, nil
		}
	}
}

func (hs *HttpService) queryDB(req *http.Request, form bool) (string, error) {
	var db string
	if form {
		db = req.FormValue("db")
	} else {
		db = req.URL.Query().Get("db")
	}
	if db == "" {
		return db, errors.New("database not found")
	}
	return db, nil
}

func (hs *HttpService) formValues(req *http.Request, key string) []string {
	var values []string
	str := strings.Trim(req.FormValue(key), ", ")
	if str != "" {
		values = strings.Split(str, ",")
	}
	return values
}

func (hs *HttpService) formBool(req *http.Request, key string) (bool, error) {
	return strconv.ParseBool(req.FormValue(key))
}

func (hs *HttpService) formTick(req *http.Request) (int64, error) {
	str := strings.TrimSpace(req.FormValue("tick"))
	if str == "" {
		return 0, nil
	}
	tick, err := strconv.ParseInt(str, 10, 64)
	if err != nil || tick < 0 {
		return 0, ErrInvalidTick
	}
	return tick, nil
}
