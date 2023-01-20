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
	proxy       *influxproxy.Proxy
	dmgr        *dm.Manager
	sharder     *sharding.ReplicaSharder
	username    string
	password    string
	authEncrypt bool
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
	sharder.Init(len(nodes), 1) // TODO  by legacy config
	ip := influxproxy.NewProxy(cfg, nodes, dmgr, sharder)

	return &HttpService{
		proxy:       ip,
		dmgr:        dmgr,
		sharder:     sharder,
		username:    cfg.Proxy.Username,
		password:    cfg.Proxy.Password,
		authEncrypt: cfg.Proxy.AuthEncrypt,
		// writeTracing: true, //cfg.Proxy.WriteTracing,
		// queryTracing: true, //cfg.Proxy.QueryTracing,
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
	// r.HandleFunc("/replica", hs.HandlerReplica)
	r.HandleFunc("/rebalance", hs.HandlerRebalance)
	// r.HandleFunc("/recovery", hs.HandlerRecovery)
	// r.HandleFunc("/resync", hs.HandlerResync)
	// r.HandleFunc("/cleanup", hs.HandlerCleanup)
	// r.HandleFunc("/transfer/state", hs.HandlerTransferState)
	// r.HandleFunc("/transfer/stats", hs.HandlerTransferStats)

	// TODO mgmt api
	// scale 添加/删除节点
	// replicate 调整备份数
	// repair 修复某些 replica 的数据
	// cleanup 清理位置不正确的数据
	// state

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

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {

	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	stats := req.URL.Query().Get("stats") == "true"
	resp := map[string]interface{}{
		"name":    "influx-proxy",
		"message": "ready for queries and writes",
		"status":  "ok",
		"checks":  []string{},
		"circles": hs.proxy.GetHealth(stats),
		"version": backend.Version,
	}
	hs.Write(w, req, http.StatusOK, resp)
}

func (hs *HttpService) HandlerEncrypt(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	text := req.URL.Query().Get("text")
	encrypt := util.AesEncrypt(text)
	hs.WriteText(w, http.StatusOK, encrypt)
}

func (hs *HttpService) HandlerDecrypt(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethod(w, req, "GET") {
		return
	}
	key := req.URL.Query().Get("key")
	text := req.URL.Query().Get("text")
	if !util.CheckCipherKey(key) {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(text)
	hs.WriteText(w, http.StatusOK, decrypt)
}

// func (hs *HttpService) HandlerReplica(w http.ResponseWriter, req *http.Request) {
// 	if !hs.checkMethodAndAuth(w, req, "GET") {
// 		return
// 	}

// 	db := req.URL.Query().Get("db")
// 	meas := req.URL.Query().Get("meas")
// 	if db != "" && meas != "" {
// 		backends, err := hs.proxy.GetAllocatedNodes(db, meas)
// 		data := make([]map[string]interface{}, len(backends))
// 		for i, b := range backends {
// 			c := hs.proxy.Circles[i]
// 			data[i] = map[string]interface{}{
// 				"backend": map[string]string{"name": b.Name, "url": b.Url},
// 				"circle":  map[string]interface{}{"id": c.CircleId, "name": c.Name},
// 			}
// 		}
// 		hs.Write(w, req, http.StatusOK, data)
// 	} else {
// 		hs.WriteError(w, req, http.StatusBadRequest, "invalid db or meas")
// 	}
// }

// 对指定的 measurement 或所有 measurement 做再均衡
func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	go hs.sharder.RebalanceForAll()

	hs.WriteText(w, http.StatusAccepted, "rebalance accepted")
}

// func (hs *HttpService) HandlerRecovery(w http.ResponseWriter, req *http.Request) {
// 	if !hs.checkMethodAndAuth(w, req, "POST") {
// 		return
// 	}

// 	// TODO

// 	fromCircleId, err := hs.formCircleId(req, "from_circle_id") // nolint:golint
// 	if err != nil {
// 		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
// 		return
// 	}
// 	toCircleId, err := hs.formCircleId(req, "to_circle_id") // nolint:golint
// 	if err != nil {
// 		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
// 		return
// 	}
// 	if fromCircleId == toCircleId {
// 		hs.WriteError(w, req, http.StatusBadRequest, "from_circle_id and to_circle_id cannot be same")
// 		return
// 	}

// 	if hs.tx.CircleStates[fromCircleId].Transferring || hs.tx.CircleStates[toCircleId].Transferring {
// 		hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d or %d is transferring", fromCircleId, toCircleId))
// 		return
// 	}
// 	if hs.tx.Resyncing {
// 		hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
// 		return
// 	}

// 	err = hs.setParam(req)
// 	if err != nil {
// 		hs.WriteError(w, req, http.StatusBadRequest, err.Error())
// 		return
// 	}

// 	backendUrls := hs.formValues(req, "backend_urls")
// 	dbs := hs.formValues(req, "dbs")
// 	go hs.tx.Recovery(fromCircleId, toCircleId, backendUrls, dbs)
// 	hs.WriteText(w, http.StatusAccepted, "accepted")
// }

// func (hs *HttpService) HandlerResync(w http.ResponseWriter, req *http.Request) {
// 	if !hs.checkMethodAndAuth(w, req, "POST") {
// 		return
// 	}

// 	var sharder sharding.ReplicaSharder

// 	go sharder.Replicate(n) // add/rm nodes

// 	// tick, err := hs.formTick(req)
// 	// if err != nil {
// 	// 	hs.WriteError(w, req, http.StatusBadRequest, err.Error())
// 	// 	return
// 	// }

// 	// for _, cs := range hs.tx.CircleStates {
// 	// 	if cs.Transferring {
// 	// 		hs.WriteText(w, http.StatusBadRequest, fmt.Sprintf("circle %d is transferring", cs.CircleId))
// 	// 		return
// 	// 	}
// 	// }
// 	// if hs.tx.Resyncing {
// 	// 	hs.WriteText(w, http.StatusBadRequest, "proxy is resyncing")
// 	// 	return
// 	// }

// 	// err = hs.setParam(req)
// 	// if err != nil {
// 	// 	hs.WriteError(w, req, http.StatusBadRequest, err.Error())
// 	// 	return
// 	// }

// 	// dbs := hs.formValues(req, "dbs")
// 	// go hs.tx.Resync(dbs, tick)
// 	hs.WriteText(w, http.StatusAccepted, "accepted")
// }

func (hs *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	measurement := req.FormValue("measurement")
	if measurement == "" {
		go hs.sharder.CleanupForAll()
		hs.WriteText(w, http.StatusAccepted, "cleanup all measurements, accepted")
		return
	} else if hs.dmgr.IsManagedMeasurement(measurement) {
		go hs.sharder.Cleanup(measurement)
		hs.WriteText(w, http.StatusAccepted, "cleanup "+measurement+", accepted")
	} else {
		hs.WriteError(w, req, http.StatusForbidden, "unmanaged measurement, forbidden")
	}
}

func (hs *HttpService) HandlerState(w http.ResponseWriter, req *http.Request) {
	// measurement: cpe.lwconn_rxtx, mode: sharding replication, replica: 0/4, shards: 5, state: rebalancing
	// measurement: cpe.lwconn_rxtx, mode: replication, replica: 0/4, nodes: 5, state: rebalancing
	// measurement: cpe.lwconn_rxtx, mode: sharding, shards: 5, state: rebalancing
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
	if hs.username == "" && hs.password == "" {
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
	return hs.transAuth(u) == hs.username && hs.transAuth(p) == hs.password
}

func (hs *HttpService) transAuth(text string) string {
	if hs.authEncrypt {
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

// func (hs *HttpService) setParam(req *http.Request) error {
// 	var err error
// 	err = hs.setWorker(req)
// 	if err != nil {
// 		return err
// 	}
// 	err = hs.setBatch(req)
// 	if err != nil {
// 		return err
// 	}
// 	err = hs.setLimit(req)
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

// func (hs *HttpService) setWorker(req *http.Request) error {
// 	str := strings.TrimSpace(req.FormValue("worker"))
// 	if str != "" {
// 		worker, err := strconv.Atoi(str)
// 		if err != nil || worker <= 0 {
// 			return ErrInvalidWorker
// 		}
// 		hs.tx.Worker = worker
// 	} else {
// 		hs.tx.Worker = transfer.DefaultWorker
// 	}
// 	return nil
// }

// func (hs *HttpService) setBatch(req *http.Request) error {
// 	str := strings.TrimSpace(req.FormValue("batch"))
// 	if str != "" {
// 		batch, err := strconv.Atoi(str)
// 		if err != nil || batch <= 0 {
// 			return ErrInvalidBatch
// 		}
// 		hs.tx.Batch = batch
// 	} else {
// 		hs.tx.Batch = transfer.DefaultBatch
// 	}
// 	return nil
// }

// func (hs *HttpService) setLimit(req *http.Request) error {
// 	str := strings.TrimSpace(req.FormValue("limit"))
// 	if str != "" {
// 		limit, err := strconv.Atoi(str)
// 		if err != nil || limit <= 0 {
// 			return ErrInvalidLimit
// 		}
// 		hs.tx.Limit = limit
// 	} else {
// 		hs.tx.Limit = transfer.DefaultLimit
// 	}
// 	return nil
// }
