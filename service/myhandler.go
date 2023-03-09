package service

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	influxproxy "github.com/abo/influx-proxy"
	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/dm"
	"github.com/abo/influx-proxy/raft"
	"github.com/abo/influx-proxy/sharding"
	"github.com/golang/snappy"
	"github.com/gorilla/mux"
	"github.com/influxdata/flux"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/storage"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

type ProxyHandler struct {
	dataNodes []*backend.Backend       // InfluxDB nodes
	proxy     *influxproxy.Proxy       // Influx query & write routing
	dmgr      *dm.Manager              // Raw data management & migration between node
	sharder   *sharding.ReplicaSharder // Sharding & allocation
	cluster   *raft.Cluster            // State sync & persistence
	cfg       *backend.Config

	stats *Statistics
}

// Statistics maintains statistics for the httpd service.
type Statistics struct {
	Requests                     int64
	CQRequests                   int64
	QueryRequests                int64
	WriteRequests                int64
	PingRequests                 int64
	StatusRequests               int64
	WriteRequestBytesReceived    int64
	QueryRequestBytesTransmitted int64
	PointsWrittenOK              int64
	PointsWrittenDropped         int64
	PointsWrittenFail            int64
	AuthenticationFailures       int64
	RequestDuration              int64
	QueryRequestDuration         int64
	WriteRequestDuration         int64
	ActiveRequests               int64
	ActiveWriteRequests          int64
	ClientErrors                 int64
	ServerErrors                 int64
	RecoveredPanics              int64
	PromWriteRequests            int64
	PromReadRequests             int64
	FluxQueryRequests            int64
	FluxQueryRequestDuration     int64
}

// Statistics returns statistics for periodic monitoring.
func (ph *ProxyHandler) Statistics(tags map[string]string) *Statistics {
	return &Statistics{}
}

func (ph *ProxyHandler) serve() {

	r := mux.NewRouter()

	r.Path("/query").Methods(http.MethodOptions).HandlerFunc(ph.serveOptions).Name("query-options")
	r.Path("/query").Methods(http.MethodGet, http.MethodPost).HandlerFunc(ph.serveQuery).Name("query")
	r.Path("/write").Methods(http.MethodOptions).HandlerFunc(ph.serveOptions).Name("write-options")
	r.Path("/write").Methods(http.MethodPost).HandlerFunc(ph.serveWriteV1).Name("write")
	r.Path("/api/v2/delete").Methods(http.MethodPost).HandlerFunc(ph.serveDeleteV2).Name("delete")
	r.Path("/api/v2/write").Methods(http.MethodPost).HandlerFunc(ph.serveWriteV2).Name("write")
	r.Path("/api/v2/write").Methods(http.MethodOptions).HandlerFunc(ph.serveOptions).Name("write-options")
	r.Path("/api/v1/prom/write").Methods(http.MethodPost).HandlerFunc(ph.servePromWrite).Name("prometheus-write")
	r.Path("/api/v1/prom/read").Methods(http.MethodPost).HandlerFunc(ph.servePromRead).Name("prometheus-read")
	r.Path("/ping").Methods(http.MethodGet, http.MethodHead).HandlerFunc(ph.servePing).Name("ping")
	r.Path("/status").Methods(http.MethodGet, http.MethodHead).HandlerFunc(ph.serveStatus).Name("status")
	r.Path("/health").Methods(http.MethodGet).HandlerFunc(ph.serveHealth).Name("health")
	r.Path("/health").Methods(http.MethodOptions).HandlerFunc(ph.serveOptions).Name("health-options")
	r.Path("/metrics").Methods(http.MethodGet).HandlerFunc(promhttp.Handler().ServeHTTP).Name("prometheus-metrics")
	r.Path("/api/v2/query").Methods(http.MethodPost).HandlerFunc(ph.serveFluxQuery).Name("flux-read")
	r.Path("/api/v2/query").Methods(http.MethodOptions).HandlerFunc(ph.serveOptions).Name("flux-read-options")

}

// serveOptions returns an empty response to comply with OPTIONS pre-flight requests
func (ph *ProxyHandler) serveOptions(w http.ResponseWriter, r *http.Request) {
	ph.writeHeader(w, http.StatusNoContent)
}

// servePing returns a simple response to let the client know the server is running.
func (ph *ProxyHandler) servePing(w http.ResponseWriter, r *http.Request) {
	verbose := r.URL.Query().Get("verbose")
	atomic.AddInt64(&ph.stats.PingRequests, 1)

	if verbose != "" && verbose != "0" && verbose != "false" {
		ph.writeHeader(w, http.StatusOK)
		b, _ := json.Marshal(map[string]string{"version": ph.Version})
		w.Write(b)
	} else {
		ph.writeHeader(w, http.StatusNoContent)
	}
}

// serveStatus has been deprecated.
func (ph *ProxyHandler) serveStatus(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.StatusRequests, 1)
	ph.writeHeader(w, http.StatusNoContent)
}

// serveHealth maps v2 health endpoint to ping endpoint
func (ph *ProxyHandler) serveHealth(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"name":    "influxsharder",
		"message": "ready for queries and writes",
		"status":  "pass",
		"checks":  []string{},
		"version": ph.Version,
	}
	b, _ := json.Marshal(resp)
	ph.writeHeader(w, http.StatusOK)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(b); err != nil {
		ph.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// serveQuery parses an incoming query and, if valid, executes the query.
func (ph *ProxyHandler) serveQuery(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.QueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ph.stats.QueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	// Retrieve the underlying ResponseWriter or initialize our own.
	rw, ok := w.(ResponseWriter)
	if !ok {
		rw = NewResponseWriter(w, r)
	}

	// Retrieve the node id the query should be executed on.
	nodeID, _ := strconv.ParseUint(r.FormValue("node_id"), 10, 64)

	var qr io.Reader
	// Attempt to read the form value from the "q" form value.
	if qp := strings.TrimSpace(r.FormValue("q")); qp != "" {
		qr = strings.NewReader(qp)
	} else if r.MultipartForm != nil && r.MultipartForm.File != nil {
		// If we have a multipart/form-data, try to retrieve a file from 'q'.
		if fhs := r.MultipartForm.File["q"]; len(fhs) > 0 {
			f, err := fhs[0].Open()
			if err != nil {
				ph.httpError(rw, err.Error(), http.StatusBadRequest)
				return
			}
			defer f.Close()
			qr = f
		}
	}

	if qr == nil {
		ph.httpError(rw, `missing required parameter "q"`, http.StatusBadRequest)
		return
	}

	epoch := strings.TrimSpace(r.FormValue("epoch"))

	p := influxql.NewParser(qr)
	db := r.FormValue("db")

	// Sanitize the request query params so it doesn't show up in the response logger.
	// Do this before anything else so a parsing error doesn't leak passwords.
	sanitize(r)

	// Parse the parameters
	rawParams := r.FormValue("params")
	if rawParams != "" {
		var params map[string]interface{}
		decoder := json.NewDecoder(strings.NewReader(rawParams))
		decoder.UseNumber()
		if err := decoder.Decode(&params); err != nil {
			ph.httpError(rw, "error parsing query parameters: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Convert json.Number into int64 and float64 values
		for k, v := range params {
			if v, ok := v.(json.Number); ok {
				var err error
				if strings.Contains(string(v), ".") {
					params[k], err = v.Float64()
				} else {
					params[k], err = v.Int64()
				}

				if err != nil {
					ph.httpError(rw, "error parsing json value: "+err.Error(), http.StatusBadRequest)
					return
				}
			}
		}
		p.SetParams(params)
	}

	// Parse query from query string.
	q, err := p.ParseQuery()
	if err != nil {
		ph.httpError(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Check authorization.
	var fineAuthorizer query.FineAuthorizer
	if h.Config.AuthEnabled {
		var err error
		if fineAuthorizer, err = h.QueryAuthorizer.AuthorizeQuery(user, q, db); err != nil {
			if authErr, ok := err.(meta.ErrAuthorize); ok {
				h.Logger.Info("Unauthorized request",
					zap.String("user", authErr.User),
					zap.Stringer("query", authErr.Query),
					logger.Database(authErr.Database))
			} else {
				h.Logger.Info("Error authorizing query", zap.Error(err))
			}
			h.httpError(rw, "error authorizing query: "+err.Error(), http.StatusForbidden)
			return
		}
	} else {
		fineAuthorizer = query.OpenAuthorizer
	}

	// Parse chunk size. Use default if not provided or unparsable.
	chunked := r.FormValue("chunked") == "true"
	chunkSize := DefaultChunkSize
	if chunked {
		if n, err := strconv.ParseInt(r.FormValue("chunk_size"), 10, 64); err == nil && int(n) > 0 {
			chunkSize = int(n)
		}
	}

	// Parse whether this is an async command.
	async := r.FormValue("async") == "true"

	opts := query.ExecutionOptions{
		Database:        db,
		RetentionPolicy: r.FormValue("rp"),
		ChunkSize:       chunkSize,
		ReadOnly:        r.Method == "GET",
		NodeID:          nodeID,
		Authorizer:      fineAuthorizer,
	}

	// Make sure if the client disconnects we signal the query to abort
	var closing chan struct{}
	if !async {
		closing = make(chan struct{})
		if notifier, ok := w.(http.CloseNotifier); ok {
			// CloseNotify() is not guaranteed to send a notification when the query
			// is closed. Use this channel to signal that the query is finished to
			// prevent lingering goroutines that may be stuck.
			done := make(chan struct{})
			defer close(done)

			notify := notifier.CloseNotify()
			go func() {
				// Wait for either the request to finish
				// or for the client to disconnect
				select {
				case <-done:
				case <-notify:
					close(closing)
				}
			}()
			opts.AbortCh = done
		} else {
			defer close(closing)
		}
	}

	// Execute query.
	results := h.QueryExecutor.ExecuteQuery(q, opts, closing)

	for _, stmt := range q.Statements {
		switch stmt.(type) {
		case *influxql.AlterRetentionPolicyStatement:
		case *influxql.CreateContinuousQueryStatement:
		case *influxql.CreateDatabaseStatement:
		case *influxql.CreateRetentionPolicyStatement:
		case *influxql.CreateSubscriptionStatement:
		case *influxql.CreateUserStatement:
		case *influxql.DeleteSeriesStatement:
		case *influxql.DeleteStatement:
		case *influxql.DropContinuousQueryStatement:
		case *influxql.DropDatabaseStatement:
		case *influxql.DropMeasurementStatement:
		case *influxql.DropRetentionPolicyStatement:
		case *influxql.DropSeriesStatement:
		case *influxql.DropSubscriptionStatement:
		case *influxql.DropUserStatement:
		case *influxql.ExplainStatement:
		case *influxql.GrantStatement:
		case *influxql.GrantAdminStatement:
		case *influxql.KillQueryStatement:
		case *influxql.ShowContinuousQueriesStatement:
		case *influxql.ShowGrantsForUserStatement:
		case *influxql.ShowDatabasesStatement:
		case *influxql.ShowFieldKeyCardinalityStatement:
		case *influxql.ShowFieldKeysStatement:
		case *influxql.ShowMeasurementCardinalityStatement:
		case *influxql.ShowMeasurementsStatement:
		case *influxql.ShowQueriesStatement:
		case *influxql.ShowRetentionPoliciesStatement:
		case *influxql.ShowSeriesStatement:
		case *influxql.ShowSeriesCardinalityStatement:
		case *influxql.ShowShardGroupsStatement:
		case *influxql.ShowShardsStatement:
		case *influxql.ShowStatsStatement:
		case *influxql.DropShardStatement:
		case *influxql.ShowSubscriptionsStatement:
		case *influxql.ShowDiagnosticsStatement:
		case *influxql.ShowTagKeyCardinalityStatement:
		case *influxql.ShowTagKeysStatement:
		case *influxql.ShowTagValuesCardinalityStatement:
		case *influxql.ShowTagValuesStatement:
		case *influxql.ShowUsersStatement:
		case *influxql.RevokeStatement:
		case *influxql.RevokeAdminStatement:
		case *influxql.SelectStatement:
			cond := stmt.(*influxql.SelectStatement).Condition
			// take out the {shardingtag} = 'shardingkey' clause to pass separately
			_, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(cond), func(e influxql.Expr) (bool, error) {
				if e, ok := e.(*influxql.BinaryExpr); ok && e.Op == influxql.EQ {

					if tag, ok := e.LHS.(*influxql.VarRef); ok && tag.Val == "shardingTagName" {
						tagValue := e.RHS.String()
					}

				}
				// switch e := e.(type) {
				// case *influxql.BinaryExpr:
				// 	switch e.Op {
				// 	case influxql.EQ:
				// 		tag, ok := e.LHS.(*influxql.VarRef)
				// 		if ok && tag.Val == measurement {
				// 			srcs = append(srcs, &influxql.Measurement{Database: db, RetentionPolicy: rp, Name: e.RHS.String()})
				// 			return true, nil
				// 		}
				// 	// Not permitted in V2 API DELETE predicates
				// 	case influxql.NEQ, influxql.REGEX, influxql.NEQREGEX, influxql.OR:
				// 		return true,
				// 			fmt.Errorf("delete - predicate only supports equality operators and conjunctions. database: %q, retention policy: %q, start: %q, stop: %q, predicate: %q",
				// 				db, rp, drd.Start, drd.Stop, drd.Predicate)
				// 	}
				// }
				return false, nil
			})

			stmt.(*influxql.SelectStatement).Condition

			influxql.WalkFunc(stmt.(*influxql.SelectStatement), func(n influxql.Node) {

			})
		case *influxql.SetPasswordUserStatement:
		default:
		}
	}

	// If we are running in async mode, open a goroutine to drain the results
	// and return with a StatusNoContent.
	if async {
		go ph.async(q, results)
		ph.writeHeader(w, http.StatusNoContent)
		return
	}

	// if we're not chunking, this will be the in memory buffer for all results before sending to client
	resp := Response{Results: make([]*query.Result, 0)}

	// Status header is OK once this point is reached.
	// Attempt to flush the header immediately so the client gets the header information
	// and knows the query was accepted.
	h.writeHeader(rw, http.StatusOK)
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	// pull all results from the channel
	rows := 0
	for r := range results {
		// Ignore nil results.
		if r == nil {
			continue
		}

		// if requested, convert result timestamps to epoch
		if epoch != "" {
			convertToEpoch(r, epoch)
		}

		// Write out result immediately if chunked.
		if chunked {
			n, _ := rw.WriteResponse(Response{
				Results: []*query.Result{r},
			})
			atomic.AddInt64(&h.stats.QueryRequestBytesTransmitted, int64(n))
			w.(http.Flusher).Flush()
			continue
		}

		// Limit the number of rows that can be returned in a non-chunked
		// response.  This is to prevent the server from going OOM when
		// returning a large response.  If you want to return more than the
		// default chunk size, then use chunking to process multiple blobs.
		// Iterate through the series in this result to count the rows and
		// truncate any rows we shouldn't return.
		if h.Config.MaxRowLimit > 0 {
			for i, series := range r.Series {
				n := h.Config.MaxRowLimit - rows
				if n < len(series.Values) {
					// We have reached the maximum number of values. Truncate
					// the values within this row.
					series.Values = series.Values[:n]
					// Since this was truncated, it will always be a partial return.
					// Add this so the client knows we truncated the response.
					series.Partial = true
				}
				rows += len(series.Values)

				if rows >= h.Config.MaxRowLimit {
					// Drop any remaining series since we have already reached the row limit.
					if i < len(r.Series) {
						r.Series = r.Series[:i+1]
					}
					break
				}
			}
		}

		// It's not chunked so buffer results in memory.
		// Results for statements need to be combined together.
		// We need to check if this new result is for the same statement as
		// the last result, or for the next statement
		l := len(resp.Results)
		if l == 0 {
			resp.Results = append(resp.Results, r)
		} else if resp.Results[l-1].StatementID == r.StatementID {
			if r.Err != nil {
				resp.Results[l-1] = r
				continue
			}

			cr := resp.Results[l-1]
			rowsMerged := 0
			if len(cr.Series) > 0 {
				lastSeries := cr.Series[len(cr.Series)-1]

				for _, row := range r.Series {
					if !lastSeries.SameSeries(row) {
						// Next row is for a different series than last.
						break
					}
					// Values are for the same series, so append them.
					lastSeries.Values = append(lastSeries.Values, row.Values...)
					lastSeries.Partial = row.Partial
					rowsMerged++
				}
			}

			// Append remaining rows as new rows.
			r.Series = r.Series[rowsMerged:]
			cr.Series = append(cr.Series, r.Series...)
			cr.Messages = append(cr.Messages, r.Messages...)
			cr.Partial = r.Partial
		} else {
			resp.Results = append(resp.Results, r)
		}

		// Drop out of this loop and do not process further results when we hit the row limit.
		if h.Config.MaxRowLimit > 0 && rows >= h.Config.MaxRowLimit {
			// If the result is marked as partial, remove that partial marking
			// here. While the series is partial and we would normally have
			// tried to return the rest in the next chunk, we are not using
			// chunking and are truncating the series so we don't want to
			// signal to the client that we plan on sending another JSON blob
			// with another result.  The series, on the other hand, still
			// returns partial true if it was truncated or had more data to
			// send in a future chunk.
			r.Partial = false
			break
		}
	}

	// If it's not chunked we buffered everything in memory, so write it out
	if !chunked {
		n, _ := rw.WriteResponse(resp)
		atomic.AddInt64(&ph.stats.QueryRequestBytesTransmitted, int64(n))
	}
}

// serveWriteV2 maps v2 write parameters to a v1 style handler.  the concepts
// of a "bucket" is mapped to v1 "database" and "retention
// policies".
func (ph *ProxyHandler) serveWriteV2(w http.ResponseWriter, r *http.Request) {
	precision := r.URL.Query().Get("precision")
	switch precision {
	case "ns":
		precision = "n"
	case "us":
		precision = "u"
	case "ms", "s", "":
		// same as v1 so do nothing
	default:
		err := fmt.Sprintf("invalid precision %q (use ns, us, ms or s)", precision)
		ph.httpError(w, err, http.StatusBadRequest)
		return
	}

	db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusNotFound)
		return
	}
	ph.serveWrite(db, rp, precision, w, r, user)
}

// serveWriteV1 handles v1 style writes.
func (ph *ProxyHandler) serveWriteV1(w http.ResponseWriter, r *http.Request) {
	precision := r.URL.Query().Get("precision")
	switch precision {
	case "", "n", "ns", "u", "ms", "s", "m", "h":
		// it's valid
	default:
		err := fmt.Sprintf("invalid precision %q (use n, u, ms, s, m or h)", precision)
		ph.httpError(w, err, http.StatusBadRequest)
		return
	}

	db := r.URL.Query().Get("db")
	rp := r.URL.Query().Get("rp")

	ph.serveWrite(db, rp, precision, w, r, user)
}

// serveWrite receives incoming series data in line protocol format and writes
// it to the database.
func (ph *ProxyHandler) serveWrite(database, retentionPolicy, precision string, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.WriteRequests, 1)
	atomic.AddInt64(&ph.stats.ActiveWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ph.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&ph.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	if database == "" {
		ph.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	// if di := h.MetaClient.Database(database); di == nil {
	// 	h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
	// 	return
	// }

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	// Handle gzip decoding of the body
	if r.Header.Get("Content-Encoding") == "gzip" {
		b, err := gzip.NewReader(r.Body)
		if err != nil {
			ph.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer b.Close()
		body = b
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the gzip reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&h.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Info("Write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	points, parseError := models.ParsePointsWithPrecision(buf.Bytes(), time.Now().UTC(), precision)
	// Not points parsed correctly so return the error now
	if parseError != nil && len(points) == 0 {
		if parseError.Error() == "EOF" {
			ph.writeHeader(w, http.StatusOK)
			return
		}
		ph.httpError(w, parseError.Error(), http.StatusBadRequest)
		return
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		var err error
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			ph.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := ph.proxy.WritePoints(database, retentionPolicy, consistency, points); influxdb.IsClientError(err) {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		// Note - we don't always collect all the errors before returning from the call,
		// so PointsWrittenOK might overestimate the number of successful points if multiple shards have errors
		atomic.AddInt64(&ph.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&ph.stats.PointsWrittenDropped, int64(werr.Dropped))
		ph.httpError(w, werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	} else if parseError != nil {
		// We wrote some of the points
		atomic.AddInt64(&ph.stats.PointsWrittenOK, int64(len(points)))
		// The other points failed to parse which means the client sent invalid line protocol.  We return a 400
		// response code as well as the lines that failed to parse.
		ph.httpError(w, tsdb.PartialWriteError{Reason: parseError.Error()}.Error(), http.StatusBadRequest)
		return
	}

	atomic.AddInt64(&ph.stats.PointsWrittenOK, int64(len(points)))
	ph.writeHeader(w, http.StatusNoContent)
}

type DeleteBody struct {
	Start     string `json:"start"`
	Stop      string `json:"stop"`
	Predicate string `json:"predicate"`
}

// serveDeleteV2 maps v2 write parameters to a v1 style handler.  the concepts
// of an "org" and "bucket" are mapped to v1 "database" and "retention
// policies".
func (ph *ProxyHandler) serveDeleteV2(w http.ResponseWriter, r *http.Request) {
	db, rp, err := bucket2dbrp(r.URL.Query().Get("bucket"))

	if err != nil {
		ph.httpError(w, fmt.Sprintf("delete - bucket: %s", err.Error()), http.StatusNotFound)
		return
	}

	if di := h.MetaClient.Database(db); di == nil {
		h.httpError(w, fmt.Sprintf("delete - database not found: %q", db), http.StatusNotFound)
		return
	} else if nil == di.RetentionPolicy(rp) {
		h.httpError(w, fmt.Sprintf("delete - retention policy not found in %q: %q", db, rp), http.StatusNotFound)
		return
	}

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("delete - user is required to delete from database %q", db), http.StatusForbidden)
			return
		}

		// DeleteSeries requires write permission to the database
		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), db); err != nil {
			h.httpError(w, fmt.Sprintf("delete - %q is not authorized to delete from %q: %s", user.ID(), db, err.Error()), http.StatusForbidden)
			return
		}
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot read request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	var drd DeleteBody
	if err := json.Unmarshal(buf.Bytes(), &drd); err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot parse request body: %s", err.Error()), http.StatusBadRequest)
		return
	}

	if len(drd.Start) <= 0 {
		h.httpError(w, "delete - start field in RFC3339Nano format required", http.StatusBadRequest)
		return
	}

	if len(drd.Stop) <= 0 {
		h.httpError(w, "delete - stop field in RFC3339Nano format required", http.StatusBadRequest)
		return
	}
	// Avoid injection errors by converting and back-converting time.
	start, err := time.Parse(time.RFC3339Nano, drd.Start)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - invalid format for start field %q, please use RFC3339Nano: %s",
			drd.Start, err.Error()), http.StatusBadRequest)
		return
	}

	// Avoid injection errors by converting and back-converting time.
	stop, err := time.Parse(time.RFC3339Nano, drd.Stop)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - invalid format for stop field %q, please use RFC3339Nano: %s",
			drd.Stop, err.Error()), http.StatusBadRequest)
		return
	}

	var timePredicate string
	timeRange := fmt.Sprintf("time >= '%s' AND time < '%s'", start.Format(time.RFC3339Nano), stop.Format(time.RFC3339Nano))

	if drd.Predicate != "" {
		timePredicate = fmt.Sprintf("%s AND %s", drd.Predicate, timeRange)
	} else {
		timePredicate = timeRange
	}

	cond, err := influxql.ParseExpr(timePredicate)
	if err != nil {
		h.httpError(w, fmt.Sprintf("delete - cannot parse predicate %q: %s", timePredicate, err.Error()), http.StatusBadRequest)
		return
	}

	srcs := make([]influxql.Source, 0)
	const measurement = "_measurement"

	// take out the _measurement = 'mymeasurement' clause to pass separately
	// Also check for illegal operands.
	_, remainingExpr, err := influxql.PartitionExpr(influxql.CloneExpr(cond), func(e influxql.Expr) (bool, error) {
		switch e := e.(type) {
		case *influxql.BinaryExpr:
			switch e.Op {
			case influxql.EQ:
				tag, ok := e.LHS.(*influxql.VarRef)
				if ok && tag.Val == measurement {
					srcs = append(srcs, &influxql.Measurement{Database: db, RetentionPolicy: rp, Name: e.RHS.String()})
					return true, nil
				}
			// Not permitted in V2 API DELETE predicates
			case influxql.NEQ, influxql.REGEX, influxql.NEQREGEX, influxql.OR:
				return true,
					fmt.Errorf("delete - predicate only supports equality operators and conjunctions. database: %q, retention policy: %q, start: %q, stop: %q, predicate: %q",
						db, rp, drd.Start, drd.Stop, drd.Predicate)
			}
		}
		return false, nil
	})

	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	influxql.WalkFunc(remainingExpr, fixLiterals)

	if err = h.Store.Delete(db, srcs, remainingExpr); err != nil {
		h.httpError(w,
			fmt.Sprintf("delete - database: %q, retention policy: %q, start: %q, stop: %q, predicate: %q, error: %s",
				db, rp, drd.Start, drd.Stop, drd.Predicate, err.Error()), http.StatusBadRequest)
		return
	}
}

// servePromWrite receives data in the Prometheus remote write protocol and writes it
// to the database
func (ph *ProxyHandler) servePromWrite(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.WriteRequests, 1)
	atomic.AddInt64(&ph.stats.ActiveWriteRequests, 1)
	atomic.AddInt64(&ph.stats.PromWriteRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ph.stats.ActiveWriteRequests, -1)
		atomic.AddInt64(&ph.stats.WriteRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	database := r.URL.Query().Get("db")
	if database == "" {
		ph.httpError(w, "database is required", http.StatusBadRequest)
		return
	}

	// if di := ph.MetaClient.Database(database); di == nil {
	// 	h.httpError(w, fmt.Sprintf("database not found: %q", database), http.StatusNotFound)
	// 	return
	// }

	if h.Config.AuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to write to database %q", database), http.StatusForbidden)
			return
		}

		if err := h.WriteAuthorizer.AuthorizeWrite(user.ID(), database); err != nil {
			h.httpError(w, fmt.Sprintf("%q user is not authorized to write to database %q", user.ID(), database), http.StatusForbidden)
			return
		}
	}

	body := r.Body
	if h.Config.MaxBodySize > 0 {
		body = truncateReader(body, int64(h.Config.MaxBodySize))
	}

	var bs []byte
	if r.ContentLength > 0 {
		if h.Config.MaxBodySize > 0 && r.ContentLength > int64(h.Config.MaxBodySize) {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		// This will just be an initial hint for the reader, as the
		// bytes.Buffer will grow as needed when ReadFrom is called
		bs = make([]byte, 0, r.ContentLength)
	}
	buf := bytes.NewBuffer(bs)

	_, err := buf.ReadFrom(body)
	if err != nil {
		if err == errTruncated {
			h.httpError(w, http.StatusText(http.StatusRequestEntityTooLarge), http.StatusRequestEntityTooLarge)
			return
		}

		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler unable to read bytes from request body")
		}
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	atomic.AddInt64(&ph.stats.WriteRequestBytesReceived, int64(buf.Len()))

	if h.Config.WriteTracing {
		h.Logger.Info("Prom write body received by handler", zap.ByteString("body", buf.Bytes()))
	}

	reqBuf, err := snappy.Decode(nil, buf.Bytes())
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Convert the Prometheus remote write request to Influx Points
	var req prompb.WriteRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	points, err := prometheus.WriteRequestToPoints(&req)
	if err != nil {
		if h.Config.WriteTracing {
			h.Logger.Info("Prom write handler", zap.Error(err))
		}

		// Check if the error was from something other than dropping invalid values.
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			ph.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Determine required consistency level.
	level := r.URL.Query().Get("consistency")
	consistency := models.ConsistencyLevelOne
	if level != "" {
		consistency, err = models.ParseConsistencyLevel(level)
		if err != nil {
			ph.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Write points.
	if err := ph.proxy.WritePoints(database, r.URL.Query().Get("rp"), consistency, points); influxdb.IsClientError(err) {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	} else if influxdb.IsAuthorizationError(err) {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusForbidden)
		return
	} else if werr, ok := err.(tsdb.PartialWriteError); ok {
		atomic.AddInt64(&ph.stats.PointsWrittenOK, int64(len(points)-werr.Dropped))
		atomic.AddInt64(&ph.stats.PointsWrittenDropped, int64(werr.Dropped))
		ph.httpError(w, werr.Error(), http.StatusBadRequest)
		return
	} else if err != nil {
		atomic.AddInt64(&ph.stats.PointsWrittenFail, int64(len(points)))
		ph.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	atomic.AddInt64(&ph.stats.PointsWrittenOK, int64(len(points)))
	ph.writeHeader(w, http.StatusNoContent)
}

// servePromRead will convert a Prometheus remote read request into a storage
// query and returns data in Prometheus remote read protobuf format.
func (ph *ProxyHandler) servePromRead(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.PromReadRequests, 1)

	compressed, err := io.ReadAll(r.Body)
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req prompb.ReadRequest
	if err := req.Unmarshal(reqBuf); err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query the DB and create a ReadResponse for Prometheus
	db := r.FormValue("db")
	rp := r.FormValue("rp")

	if h.Config.AuthEnabled && h.Config.PromReadAuthEnabled {
		if user == nil {
			h.httpError(w, fmt.Sprintf("user is required to read from database %q", db), http.StatusForbidden)
			return
		}
		if h.QueryAuthorizer.AuthorizeDatabase(user, influxql.ReadPrivilege, db) != nil {
			h.httpError(w, fmt.Sprintf("user %q is not authorized to read from database %q", user.ID(), db), http.StatusForbidden)
			return
		}
	}

	readRequest, err := prometheus.ReadRequestToInfluxStorageRequest(&req, db, rp)
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	respond := func(resp *prompb.ReadResponse) {
		data, err := resp.Marshal()
		if err != nil {
			ph.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")

		compressed = snappy.Encode(nil, data)
		if _, err := w.Write(compressed); err != nil {
			ph.httpError(w, err.Error(), http.StatusInternalServerError)
			return
		}

		atomic.AddInt64(&ph.stats.QueryRequestBytesTransmitted, int64(len(compressed)))
	}

	ctx := context.Background()
	rs, err := h.Store.ReadFilter(ctx, readRequest)
	if err != nil {
		h.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := &prompb.ReadResponse{
		Results: []*prompb.QueryResult{{}},
	}

	if rs == nil {
		respond(resp)
		return
	}
	defer rs.Close()

	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		tags := prometheus.RemoveInfluxSystemTags(rs.Tags())
		var unsupportedCursor string
		switch cur := cur.(type) {
		case tsdb.FloatArrayCursor:
			var series *prompb.TimeSeries
			for {
				a := cur.Next()
				if a.Len() == 0 {
					break
				}

				// We have some data for this series.
				if series == nil {
					series = &prompb.TimeSeries{
						Labels: prometheus.ModelTagsToLabelPairs(tags),
					}
				}

				for i, ts := range a.Timestamps {
					series.Samples = append(series.Samples, prompb.Sample{
						Timestamp: ts / int64(time.Millisecond),
						Value:     a.Values[i],
					})
				}
			}

			// There was data for the series.
			if series != nil {
				resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, series)
			}
		case tsdb.IntegerArrayCursor:
			unsupportedCursor = "int64"
		case tsdb.UnsignedArrayCursor:
			unsupportedCursor = "uint"
		case tsdb.BooleanArrayCursor:
			unsupportedCursor = "bool"
		case tsdb.StringArrayCursor:
			unsupportedCursor = "string"
		default:
			panic(fmt.Sprintf("unreachable: %T", cur))
		}
		cur.Close()

		if len(unsupportedCursor) > 0 {
			h.Logger.Info("Prometheus can't read cursor",
				zap.String("cursor_type", unsupportedCursor),
				zap.Stringer("series", tags),
			)
		}
	}

	respond(resp)
}

func (ph *ProxyHandler) serveFluxQuery(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&ph.stats.FluxQueryRequests, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&ph.stats.FluxQueryRequestDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	req, err := decodeQueryRequest(r)
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	if val := r.FormValue("node_id"); val != "" {
		if nodeID, err := strconv.ParseUint(val, 10, 64); err == nil {
			ctx = storage.NewContextWithReadOptions(ctx, &storage.ReadOptions{NodeID: nodeID})
		}
	}

	if h.Config.AuthEnabled {
		ctx = meta.NewContextWithUser(ctx, user)
	}

	pr := req.ProxyRequest()

	// Logging
	var (
		stats flux.Statistics
		n     int64
	)
	if h.Config.FluxLogEnabled {
		defer func() {
			h.logFluxQuery(n, stats, pr.Compiler, err)
		}()
	}

	q, err := h.Controller.Query(ctx, pr.Compiler)
	if err != nil {
		ph.httpError(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		q.Cancel()
		q.Done()
	}()

	// NOTE: We do not write out the headers here.
	// It is possible that if the encoding step fails
	// that we can write an error header so long as
	// the encoder did not write anything.
	// As such we rely on the http.ResponseWriter behavior
	// to write an StatusOK header with the first write.

	switch r.Header.Get("Accept") {
	case "text/csv":
		fallthrough
	default:
		if hd, ok := pr.Dialect.(httpDialect); !ok {
			ph.httpError(w, fmt.Sprintf("unsupported dialect over HTTP %T", req.Dialect), http.StatusBadRequest)
			return
		} else {
			hd.SetHeaders(w)
		}
		encoder := pr.Dialect.Encoder()
		results := flux.NewResultIteratorFromQuery(q)
		if h.Config.FluxLogEnabled {
			defer func() {
				stats = results.Statistics()
			}()
		}
		defer results.Release()

		n, err = encoder.Encode(w, results)
		if err != nil {
			if n == 0 {
				// If the encoder did not write anything, we can write an error header.
				ph.httpError(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
}

// bucket2drbp extracts a bucket and retention policy from a properly formatted
// string.
//
// The 2.x compatible endpoints encode the database and retention policy names
// in the database URL query value.  It is encoded using a forward slash like
// "database/retentionpolicy" and we should be able to simply split that string
// on the forward slash.
func bucket2dbrp(bucket string) (string, string, error) {
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

// writeHeader writes the provided status code in the response, and
// updates relevant http error statistics.
func (ph *ProxyHandler) writeHeader(w http.ResponseWriter, code int) {
	switch code / 100 {
	case 4:
		atomic.AddInt64(&ph.stats.ClientErrors, 1)
	case 5:
		atomic.AddInt64(&ph.stats.ServerErrors, 1)
	}
	w.WriteHeader(code)
}

// httpError writes an error to the client in a standard format.
func (ph *ProxyHandler) httpError(w http.ResponseWriter, errmsg string, code int) {
	if code == http.StatusUnauthorized {
		// If an unauthorized header will be sent back, add a WWW-Authenticate header
		// as an authorization challenge.
		w.Header().Set("WWW-Authenticate", fmt.Sprintf("Basic realm=\"%s\"", h.Config.Realm))
	} else if code/100 != 2 {
		sz := math.Min(float64(len(errmsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errmsg[:int(sz)])
	}

	response := Response{Err: errors.New(errmsg)}
	if rw, ok := w.(ResponseWriter); ok {
		ph.writeHeader(w, code)
		rw.WriteResponse(response)
		return
	}

	// Default implementation if the response writer hasn't been replaced
	// with our special response writer type.
	w.Header().Add("Content-Type", "application/json")
	h.writeHeader(w, code)
	b, _ := json.Marshal(response)
	w.Write(b)
}
