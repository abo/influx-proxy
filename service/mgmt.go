package service

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/util"
)

type DataNodeInfo struct {
	ID int
	backend.BackendConfig
}

func (svc *HttpService) HandlerHealth(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "GET") {
		return
	}
	stats := req.URL.Query().Get("stats") == "true"

	var wg sync.WaitGroup
	health := make([]interface{}, len(svc.dataNodes))
	for i, n := range svc.dataNodes {
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
		// "version": backend.Version,
	}
	svc.Write(w, req, http.StatusOK, resp)
}

func (svc *HttpService) HandlerEncrypt(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethod(w, req, "GET") {
		return
	}
	text := req.URL.Query().Get("text")
	encrypt := util.AesEncrypt(text)
	svc.WriteText(w, http.StatusOK, encrypt)
}

func (svc *HttpService) HandlerDecrypt(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethod(w, req, "GET") {
		return
	}
	key := req.URL.Query().Get("key")
	text := req.URL.Query().Get("text")
	if !util.CheckCipherKey(key) {
		svc.WriteError(w, req, http.StatusBadRequest, "invalid key")
		return
	}
	decrypt := util.AesDecrypt(text)
	svc.WriteText(w, http.StatusOK, decrypt)
}

// 对指定的 measurement 或所有 measurement 做再均衡
func (svc *HttpService) HandlerRebalance(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	go svc.sharder.RebalanceForAll()

	svc.WriteText(w, http.StatusAccepted, "rebalance accepted")
}

func (svc *HttpService) HandlerScale(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	// TODO scale in
	url := req.FormValue("url")
	user := req.FormValue("username")
	pwd := req.FormValue("password")

	nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
	svc.cfg.DataNodes = append(svc.cfg.DataNodes, nodeCfg)
	svc.dataNodes = append(svc.dataNodes, backend.NewBackend(len(svc.dataNodes), nodeCfg, svc.cfg.Proxy))
	svc.dmgr.SetDataNodes(svc.dataNodes)
	svc.proxy.SetDataNodes(svc.dataNodes)
	err := svc.proposeDataNodeChanges(&DataNodeInfo{len(svc.dataNodes) - 1, *nodeCfg})
	go svc.sharder.Scale(len(svc.dataNodes))

	if err != nil {
		svc.WriteText(w, http.StatusPartialContent, fmt.Sprintf("scale partially accepted: %v", err))
	} else {
		svc.WriteText(w, http.StatusAccepted, "scale accepted")
	}
}

func (svc *HttpService) HandlerReplicate(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}
	replicas, err := strconv.Atoi(req.FormValue("replicas"))
	if err != nil || replicas > len(svc.dataNodes) {
		svc.WriteError(w, req, http.StatusBadRequest, "invalid replicas")
		return
	}
	go svc.sharder.Replicate(replicas)
	svc.WriteText(w, http.StatusAccepted, "replication accepted")
}

func (svc *HttpService) HandlerRepair(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}
	id, err := strconv.Atoi(req.FormValue("id"))
	if err != nil || id >= len(svc.dataNodes) {
		svc.WriteError(w, req, http.StatusBadRequest, "invalid id")
		return
	}
	go svc.sharder.Repair(id)
	svc.WriteText(w, http.StatusAccepted, "repair accepted")
}

func (svc *HttpService) HandlerReplace(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	id, err := strconv.Atoi(req.FormValue("id"))
	if err != nil || id >= len(svc.dataNodes) {
		svc.WriteError(w, req, http.StatusBadRequest, "invalid id")
		return
	}
	url := req.FormValue("url")
	user := req.FormValue("username")
	pwd := req.FormValue("password")

	nodeCfg := &backend.BackendConfig{Url: url, Username: user, Password: pwd}
	origin := svc.dataNodes[id].HttpBackend
	svc.dataNodes[id].HttpBackend = backend.NewHttpBackend(nodeCfg, svc.cfg.Proxy)
	svc.cfg.DataNodes[id] = nodeCfg

	perr := svc.proposeDataNodeChanges(&DataNodeInfo{id, *nodeCfg})
	log.Infof("node(%d) replaced by %s, origin: %s, start migrate history data", id, url, origin.Url)
	go func() {
		err := svc.dmgr.CopyNode(origin, svc.dataNodes[id].HttpBackend)
		if err != nil {
			log.Errorf("failed to migrate data from %s to %s after replace node(%d): %v", origin.Url, url, id, err)
		} else {
			log.Infof("migrate done, from: %s, to: %s", origin.Url, url)
		}
		origin.Close()
	}()
	if perr != nil {
		svc.WriteText(w, http.StatusPartialContent, fmt.Sprintf("replace partially accepted: %v", perr))
	} else {
		svc.WriteText(w, http.StatusAccepted, "replace accepted")
	}
}

func (svc *HttpService) HandlerCleanup(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "POST") {
		return
	}

	measurement := req.FormValue("measurement")
	if measurement == "" {
		go svc.sharder.CleanupForAll()
		svc.WriteText(w, http.StatusAccepted, "cleanup all measurements, accepted")
		return
	} else if svc.dmgr.IsManagedMeasurement(measurement) {
		go svc.sharder.Cleanup(measurement)
		svc.WriteText(w, http.StatusAccepted, "cleanup "+measurement+", accepted")
	} else {
		svc.WriteError(w, req, http.StatusForbidden, "unmanaged measurement, forbidden")
	}
}

func (svc *HttpService) HandlerState(w http.ResponseWriter, req *http.Request) {
	if !svc.checkMethodAndAuth(w, req, "GET") {
		return
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%d data-nodes: ", len(svc.dataNodes)))
	for _, node := range svc.dataNodes {
		b.WriteString(node.Url)
		b.WriteString(",")
	}
	b.WriteString("\n\nreplicas:\n")
	for _, info := range svc.sharder.CurrentState() {
		b.WriteString(fmt.Sprintf("%s-%d, \t\tshards: %d, \t\tstate: %v \n", info.Measurement, info.Index, info.Shards, info.State))
	}

	svc.WriteText(w, http.StatusOK, b.String())
	// measurement: cpe.lwconn_rxtx, mode: sharding replication, replica: 0/4, shards: 5, state: rebalancing
	// measurement: cpe.lwconn_rxtx, mode: replication, replica: 0/4, nodes: 5, state: rebalancing
	// measurement: cpe.lwconn_rxtx, mode: sharding, shards: 5, state: rebalancing
}

func (svc *HttpService) HandlerStats(w http.ResponseWriter, req *http.Request) {}
