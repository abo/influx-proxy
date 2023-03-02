// Copyright 2023 Shengbo Huang. All rights reserved.
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
	"github.com/abo/influx-proxy/raft"
	"github.com/abo/influx-proxy/service/prometheus"
	"github.com/abo/influx-proxy/service/prometheus/remote"
	"github.com/abo/influx-proxy/sharding"
	"github.com/abo/influx-proxy/util"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

var (
	ErrInvalidTick    = errors.New("invalid tick, require non-negative integer")
	ErrInvalidWorker  = errors.New("invalid worker, require positive integer")
	ErrInvalidBatch   = errors.New("invalid batch, require positive integer")
	ErrInvalidLimit   = errors.New("invalid limit, require positive integer")
	ErrInvalidHaAddrs = errors.New("invalid ha_addrs, require at least two addresses as <host:port>, comma-separated")
)

type HttpService struct {
	dataNodes []*backend.Backend       // InfluxDB nodes
	proxy     *influxproxy.Proxy       // Influx query & write routing
	dmgr      *dm.Manager              // Raw data management & migration between node
	sharder   *sharding.ReplicaSharder // Sharding & allocation
	cluster   *raft.Cluster            // State sync & persistence
	cfg       *backend.Config
	// writeTracing bool
	// queryTracing bool
}

func NewHttpService(cfg *backend.Config) (*HttpService, error) {
	svc := &HttpService{}

	// 1) initialize raft cluster
	log.Infof("initialize raft cluster, local-id: %d, peers: %v, dir: %s", cfg.Cluster.LocalID, cfg.Cluster.GetPeers(), cfg.Proxy.DataDir)
	svc.cluster = raft.StartCluster(cfg.Cluster.LocalID, cfg.Cluster.GetPeers(), cfg.Proxy.DataDir, func() ([]byte, error) {
		dataNodes := make([]*DataNodeInfo, len(svc.cfg.DataNodes))
		for i, node := range svc.cfg.DataNodes {
			dataNodes[i] = &DataNodeInfo{i, *node}
		}
		return serialize(dataNodes, svc.sharder.CurrentState())
	})

	// 2) restore previous state if exists
	var replicas []*sharding.ReplicaInfo
	snapshot, err := svc.cluster.LoadSnapshot()
	if err == nil {
		// recover from snapshot
		log.Infof("load snapshot from %s", cfg.Proxy.DataDir)
		var dataNodeInfos []*DataNodeInfo
		dataNodeInfos, replicas, err = unserialize(snapshot.Data)
		if err != nil {
			return nil, err
		}
		cfg.DataNodes = make([]*backend.BackendConfig, len(dataNodeInfos))
		for i, nodeInfo := range dataNodeInfos {
			svc.cfg.DataNodes[i] = &nodeInfo.BackendConfig
		}
	} else if err == snap.ErrNoSnapshot {
		// init by config & legacy env
		log.Infof("startup from scratch, init by config")
	} else {
		// failed to load snapshot
		log.Errorf("failed to load snapshot: %v", err)
		return nil, err
	}
	svc.cfg = cfg

	// 3) initialize data nodes
	log.Infof("connect to %d influxdb nodes", len(cfg.DataNodes))
	dataNodes := make([]*backend.Backend, 0, len(cfg.DataNodes))
	for i, nodeCfg := range cfg.DataNodes {
		dataNodes = append(dataNodes, backend.NewBackend(i, nodeCfg, cfg.Proxy))
		svc.proposeDataNodeChanges(&DataNodeInfo{i, *nodeCfg})
	}
	svc.dataNodes = dataNodes

	// 4) initialize data manager
	log.Infof("start data manager for measurements: %v", cfg.Proxy.Measurements)
	svc.dmgr = dm.NewManager(dataNodes, cfg.Proxy.Measurements)

	// 5) initialize sharder
	log.Infof("start sharder, %d measurement(s) sharded", len(cfg.Sharding))
	proposeReplicas := func(changes []*sharding.ReplicaInfo) error {
		if data, err := serialize(nil, changes); err != nil {
			return err
		} else {
			svc.cluster.Propose(data)
			return nil
		}
	}

	svc.sharder = sharding.NewSharder(svc.dmgr, cfg.Sharding, proposeReplicas)
	if replicas != nil {
		log.Info("recover meta from snapshot")
		svc.sharder.Init(len(dataNodes), 1)
		svc.sharder.ApplyReplicaChanges(replicas)
	} else {
		log.Infof("initialize meta with number-of-shards: %d, number-of-replicas: %d", len(dataNodes), 1)
		svc.sharder.Init(len(dataNodes), 1) // TODO  by legacy config
		proposeReplicas(svc.sharder.CurrentState())
	}

	// 6) initialize proxy
	log.Infof("start proxy at %s", cfg.Proxy.ListenAddr)
	svc.proxy = influxproxy.NewProxy(cfg, svc.dataNodes, svc.dmgr, svc.sharder) // cfg.Proxy.WriteTracing, cfg.Proxy.QueryTracing,

	go svc.applyRaftCommits()
	return svc, nil
}

func (svc *HttpService) Handler(intercept func(next http.Handler) http.Handler) http.Handler {
	r := mux.NewRouter()
	if intercept != nil {
		r.Use(intercept)
	}

	// biz for influx query & write
	r.HandleFunc("/ping", svc.HandlerPing)
	r.HandleFunc("/query", svc.HandlerQuery)
	r.HandleFunc("/write", svc.HandlerWrite)
	r.HandleFunc("/api/v2/query", svc.HandlerQueryV2)
	r.HandleFunc("/api/v2/write", svc.HandlerWriteV2)
	r.HandleFunc("/api/v1/prom/read", svc.HandlerPromRead)
	r.HandleFunc("/api/v1/prom/write", svc.HandlerPromWrite)

	// mgmt for proxy management
	r.HandleFunc("/health", svc.HandlerHealth)
	r.HandleFunc("/encrypt", svc.HandlerEncrypt)
	r.HandleFunc("/decrypt", svc.HandlerDecrypt)
	r.HandleFunc("/rebalance", svc.HandlerRebalance) // 数据再均衡
	r.HandleFunc("/scale", svc.HandlerScale)         // 添加/删除节点, 并同时调整分片数以及再均衡
	r.HandleFunc("/replicate", svc.HandlerReplicate) // 调整备份数
	r.HandleFunc("/repair", svc.HandlerRepair)       // 修复指定节点的数据
	r.HandleFunc("/replace", svc.HandlerReplace)     // 替换节点
	r.HandleFunc("/cleanup", svc.HandlerCleanup)     // 清理位置不正确的数据
	r.HandleFunc("/state", svc.HandlerState)         // 当前状态
	r.HandleFunc("/stats", svc.HandlerStats)         // 数据分布统计

	// debug for profile
	r.HandleFunc("/debug/pprof/{name}", func(resp http.ResponseWriter, req *http.Request) {
		name := mux.Vars(req)["name"]
		pprof.Handler(name).ServeHTTP(resp, req)
	})

	// raft provides a commit stream for the proposals from the http api
	r.PathPrefix(rafthttp.RaftPrefix).Handler(svc.cluster.TransportHandler())

	return r
}

func (svc *HttpService) HandlerPing(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusNoContent)
}

func (svc *HttpService) HandlerQuery(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "GET", "POST") {
		return
	}

	db := req.FormValue("db")
	q := req.FormValue("q")
	body, err := svc.proxy.Query(w, req)
	if err != nil {
		log.Errorf("influxql query error: %s, query: %s, db: %s, client: %s", err, q, db, req.RemoteAddr)
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	svc.WriteBody(w, body)
	log.Debugf("influxql query: %s, db: %s, client: %s", q, db, req.RemoteAddr)
}

func (svc *HttpService) HandlerQueryV2(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	var contentType = "application/json"
	if ct := req.Header.Get("Content-Type"); ct != "" {
		contentType = ct
	}
	mt, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rbody, err := ioutil.ReadAll(req.Body)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
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
			svc.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("failed parsing request body as JSON: %s", err))
			return
		}
	}

	if qr.Query == "" && qr.Spec == nil {
		svc.WriteError(w, req, http.StatusBadRequest, "request body requires either spec or query")
		return
	}
	if qr.Type != "" && qr.Type != "flux" {
		svc.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("unknown query type: %s", qr.Type))
		return
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(rbody))
	err = svc.proxy.QueryFlux(w, req, qr)
	if err != nil {
		log.Errorf("flux query error: %s, query: %s, spec: %s, client: %s", err, qr.Query, qr.Spec, req.RemoteAddr)
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("flux query: %s, spec: %s, client: %s", qr.Query, qr.Spec, req.RemoteAddr)
}

func (svc *HttpService) HandlerWrite(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
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
		svc.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use n, ns, u, ms, s, m or h)", precision))
		return
	}

	db, err := svc.queryDB(req, false)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	rp := req.URL.Query().Get("rp")

	svc.writeInternal(db, rp, precision, w, req)
}

func (svc *HttpService) HandlerWriteV2(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
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
		svc.WriteError(w, req, http.StatusBadRequest, fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision))
		return
	}

	db, rp, err := svc.bucket2dbrp(req.URL.Query().Get("bucket"))
	if err != nil {
		svc.WriteError(w, req, http.StatusNotFound, err.Error())
		return
	}

	svc.writeInternal(db, rp, precision, w, req)
}

func (svc *HttpService) writeInternal(db, rp, precision string, w http.ResponseWriter, req *http.Request) {
	body := req.Body
	if req.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(body)
		if err != nil {
			svc.WriteError(w, req, http.StatusBadRequest, "unable to decode gzip body")
			return
		}
		defer b.Close()
		body = b
	}
	p, err := ioutil.ReadAll(body)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	err = svc.proxy.Write(p, db, rp, precision)
	if err != nil {
		log.Errorf("write line protocol err: %v, db: %s, rp: %s, precision: %s, data: %s, client: %s", err, db, rp, precision, p, req.RemoteAddr)
		svc.WriteError(w, req, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
	log.Debugf("write line protocol, db: %s, rp: %s, precision: %s, data: %s, client: %s", db, rp, precision, p, req.RemoteAddr)
}

func (svc *HttpService) HandlerPromRead(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := svc.queryDB(req, true)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	compressed, err := ioutil.ReadAll(req.Body)
	if err != nil {
		svc.WriteError(w, req, http.StatusInternalServerError, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	var readReq remote.ReadRequest
	if err = proto.Unmarshal(reqBuf, &readReq); err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	if len(readReq.Queries) != 1 {
		err = errors.New("prometheus read endpoint currently only supports one query at a time")
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
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
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
	}

	req.Body = ioutil.NopCloser(bytes.NewBuffer(compressed))
	err = svc.proxy.ReadProm(w, req, db, metric)
	if err != nil {
		log.Warnf("prometheus read error: %s, query: %s %s %v, client: %s", err, req.Method, db, q, req.RemoteAddr)
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}
	log.Debugf("prometheus read: %s %s %v, client: %s", req.Method, db, q, req.RemoteAddr)
}

func (svc *HttpService) HandlerPromWrite(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	db, err := svc.queryDB(req, false)
	if err != nil {
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
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
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		log.Warnf("prom write handler unable to snappy decode from request body, error: %s", err)
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var writeReq remote.WriteRequest
	if err = proto.Unmarshal(reqBuf, &writeReq); err != nil {
		log.Warnf("prom write handler unable to unmarshal from snappy decoded bytes, error: %s", err)
		svc.WriteError(w, req, http.StatusBadRequest, err.Error())
		return
	}

	points, err := prometheus.WriteRequestToPoints(&writeReq)
	if err != nil {
		log.Warnf("prom write handler, error: %s", err)
		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			svc.WriteError(w, req, http.StatusBadRequest, err.Error())
			return
		}
	}

	// Write points.
	err = svc.proxy.WritePoints(points, db, rp)
	if err == nil {
		w.WriteHeader(http.StatusNoContent)
	}
}

func (svc *HttpService) Write(w http.ResponseWriter, req *http.Request, status int, data interface{}) {
	if status/100 >= 4 {
		svc.WriteError(w, req, status, data.(string))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(data, pretty))
}

func (svc *HttpService) WriteError(w http.ResponseWriter, req *http.Request, status int, err string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Influxdb-Error", err)
	w.WriteHeader(status)
	rsp := backend.ResponseFromError(err)
	pretty := req.URL.Query().Get("pretty") == "true"
	w.Write(util.MarshalJSON(rsp, pretty))
}

func (svc *HttpService) WriteBody(w http.ResponseWriter, body []byte) {
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (svc *HttpService) WriteText(w http.ResponseWriter, status int, text string) {
	w.WriteHeader(status)
	w.Write([]byte(text + "\n"))
}

func (svc *HttpService) checkMethodAndAuth(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	return svc.checkMethod(w, req, methods...) && svc.checkAuth(w, req)
}

func (svc *HttpService) checkMethod(w http.ResponseWriter, req *http.Request, methods ...string) bool {
	for _, method := range methods {
		if req.Method == method {
			return true
		}
	}
	svc.WriteError(w, req, http.StatusMethodNotAllowed, "method not allow")
	return false
}

func (svc *HttpService) checkAuth(w http.ResponseWriter, req *http.Request) bool {
	if svc.cfg.Proxy.Username == "" && svc.cfg.Proxy.Password == "" {
		return true
	}
	q := req.URL.Query()
	if u, p := q.Get("u"), q.Get("p"); svc.compareAuth(u, p) {
		return true
	}
	if u, p, ok := req.BasicAuth(); ok && svc.compareAuth(u, p) {
		return true
	}
	if u, p, ok := svc.parseAuth(req); ok && svc.compareAuth(u, p) {
		return true
	}
	svc.WriteError(w, req, http.StatusUnauthorized, "authentication failed")
	return false
}

func (svc *HttpService) parseAuth(req *http.Request) (string, string, bool) {
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

func (svc *HttpService) compareAuth(u, p string) bool {
	return svc.transAuth(u) == svc.cfg.Proxy.Username && svc.transAuth(p) == svc.cfg.Proxy.Password
}

func (svc *HttpService) transAuth(text string) string {
	if svc.cfg.Proxy.AuthEncrypt {
		return util.AesEncrypt(text)
	}
	return text
}

func (svc *HttpService) bucket2dbrp(bucket string) (string, string, error) {
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

func (svc *HttpService) queryDB(req *http.Request, form bool) (string, error) {
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

func (svc *HttpService) formValues(req *http.Request, key string) []string {
	var values []string
	str := strings.Trim(req.FormValue(key), ", ")
	if str != "" {
		values = strings.Split(str, ",")
	}
	return values
}

func (svc *HttpService) formBool(req *http.Request, key string) (bool, error) {
	return strconv.ParseBool(req.FormValue(key))
}

func (svc *HttpService) formTick(req *http.Request) (int64, error) {
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
