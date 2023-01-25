package service

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/util"
)

func (hs *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "GET") {
		return
	}
	stats := req.URL.Query().Get("stats") == "true"

	var wg sync.WaitGroup
	health := make([]interface{}, len(hs.nodes))
	for i, n := range hs.nodes {
		wg.Add(1)
		go func(i int, node *backend.Backend) {
			defer wg.Done()
			health[i] = node.GetHealth(stats)
		}(i, n)
	}
	wg.Wait()

	resp := map[string]interface{}{
		"name":    "influx-proxy",
		"message": "ready for queries and writes",
		"status":  "ok",
		"checks":  []string{},
		"nodes":   health,
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

// 对指定的 measurement 或所有 measurement 做再均衡
func (hs *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	go hs.sharder.RebalanceForAll()

	hs.WriteText(w, http.StatusAccepted, "rebalance accepted")
}

func (hs *HttpService) HandlerScale(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	// TODO scale in
	url := req.FormValue("url")
	user := req.FormValue("username")
	pwd := req.FormValue("password")

	nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
	hs.cfg.Nodes = append(hs.cfg.Nodes, nodeCfg)
	hs.nodes = append(hs.nodes, backend.NewBackend(len(hs.nodes), nodeCfg, hs.cfg.Proxy))

	go hs.sharder.Scale(len(hs.nodes))

	hs.WriteText(w, http.StatusAccepted, "scale accepted")
}

func (hs *HttpService) HandlerReplicate(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}
	replicas, err := strconv.Atoi(req.FormValue("replicas"))
	if err != nil || replicas > len(hs.nodes) {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid replicas")
		return
	}
	go hs.sharder.Replicate(replicas)
	hs.WriteText(w, http.StatusAccepted, "replication accepted")
}

func (hs *HttpService) HandlerRepair(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}
	id, err := strconv.Atoi(req.FormValue("id"))
	if err != nil || id >= len(hs.nodes) {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid id")
		return
	}
	go hs.sharder.Repair(id)
	hs.WriteText(w, http.StatusAccepted, "repair accepted")
}

func (hs *HttpService) HandlerReplace(w http.ResponseWriter, req *http.Request) {
	if !hs.checkMethodAndAuth(w, req, "POST") {
		return
	}

	id, err := strconv.Atoi(req.FormValue("id"))
	if err != nil || id >= len(hs.nodes) {
		hs.WriteError(w, req, http.StatusBadRequest, "invalid id")
		return
	}
	url := req.FormValue("url")
	user := req.FormValue("username")
	pwd := req.FormValue("password")

	nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
	dest := backend.NewBackend(len(hs.nodes), nodeCfg, hs.cfg.Proxy)

	go func() {
		err := hs.dmgr.CopyNode(int32(id), dest)
		if err != nil {
			log.Errorf("failed to replace node(%d) by %s: %v", id, url, err)
		}
		hs.nodes[id] = dest
		hs.cfg.Nodes[id] = nodeCfg
		log.Infof("node(%d) replaced by %s", id, url)
	}()

	hs.WriteText(w, http.StatusAccepted, "replace accepted")
}

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

func (hs *HttpService) HandlerStats(w http.ResponseWriter, req *http.Request) {}
