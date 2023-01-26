package dm

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/util"
	"github.com/influxdata/influxdb1-client/models"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/multierr"
)

var (
	FieldTypes    = []string{"float", "integer", "string", "boolean"}
	RetryCount    = 10
	RetryInterval = 15
	DefaultWorker = 20
	DefaultBatch  = 25000
	DefaultLimit  = 1000000
)

type QueryResult struct {
	Series models.Rows
	Err    error
}

type Transfer struct {
	pool  *ants.Pool
	Batch int
	Limit int
}

func NewTransfer() (tx *Transfer) {
	pool, err := ants.NewPool(DefaultWorker)
	if err != nil {
		panic(err)
	}
	return &Transfer{
		pool:  pool,
		Batch: DefaultBatch,
		Limit: DefaultLimit,
	}
}

func getBackendUrls(backends []*backend.HttpBackend) []string {
	backendUrls := make([]string, len(backends))
	for k, be := range backends {
		backendUrls[k] = be.Url
	}
	return backendUrls
}

func reformFieldKeys(fieldKeys map[string][]string) map[string]string {
	// The SELECT statement returns all field values if all values have the same type.
	// If field value types differ across shards, InfluxDB first performs any applicable cast operations and
	// then returns all values with the type that occurs first in the following list: float, integer, string, boolean.
	fieldSet := make(map[string]util.Set, len(fieldKeys))
	for field, types := range fieldKeys {
		fieldSet[field] = util.NewSetFromSlice(types)
	}
	fieldMap := make(map[string]string, len(fieldKeys))
	for field, types := range fieldKeys {
		if len(types) == 1 {
			fieldMap[field] = types[0]
		} else {
			for _, dt := range FieldTypes {
				if fieldSet[field][dt] {
					fieldMap[field] = dt
					break
				}
			}
		}
	}
	return fieldMap
}

func (tx *Transfer) write(ch chan *QueryResult, dsts []*backend.HttpBackend, db, rp, meas string, tags map[string]struct{}, fields map[string]string) error {
	var buf bytes.Buffer
	var wg sync.WaitGroup
	pool, err := ants.NewPool(len(dsts) * 20)
	if err != nil {
		return err
	}
	defer pool.Release()
	for qr := range ch {
		if qr.Err != nil {
			return qr.Err
		}
		serie := qr.Series[0]
		columns := serie.Columns
		valen := len(serie.Values)
		for idx, value := range serie.Values {
			mtagSet := []string{util.EscapeMeasurement(meas)}
			fieldSet := make([]string, 0)
			for i := 1; i < len(value); i++ {
				k := columns[i]
				v := value[i]
				if _, ok := tags[k]; ok {
					if v != nil {
						mtagSet = append(mtagSet, fmt.Sprintf("%s=%s", util.EscapeTag(k), util.EscapeTag(util.CastString(v))))
					}
				} else if vtype, ok := fields[k]; ok {
					if v != nil {
						if vtype == "float" || vtype == "boolean" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=%v", util.EscapeTag(k), v))
						} else if vtype == "integer" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=%vi", util.EscapeTag(k), v))
						} else if vtype == "string" {
							fieldSet = append(fieldSet, fmt.Sprintf("%s=\"%s\"", util.EscapeTag(k), models.EscapeStringField(util.CastString(v))))
						}
					}
				}
			}
			mtagStr := strings.Join(mtagSet, ",")
			fieldStr := strings.Join(fieldSet, ",")
			line := fmt.Sprintf("%s %s %v\n", mtagStr, fieldStr, value[0])
			buf.WriteString(line)
			if (idx+1)%tx.Batch == 0 || idx+1 == valen {
				p := buf.Bytes()
				for _, dst := range dsts {
					dst := dst
					wg.Add(1)
					pool.Submit(func() {
						defer wg.Done()
						var err error
						for i := 0; i <= RetryCount; i++ {
							if i > 0 {
								time.Sleep(time.Duration(RetryInterval) * time.Second)
								log.Warnf("transfer write retry: %d, err:%s dst:%s db:%s rp:%s meas:%s", i, err, dst.Url, db, rp, meas)
							}
							err = dst.Write(db, rp, p)
							if err == nil {
								break
							}
						}
						if err != nil {
							log.Errorf("transfer write error: %s, dst:%s db:%s rp:%s meas:%s", err, dst.Url, db, rp, meas)
						}
					})
				}
				buf = bytes.Buffer{}
			}
		}
	}
	wg.Wait()
	return nil
}

func (tx *Transfer) query(ch chan *QueryResult, src *backend.HttpBackend, db, rp, meas string, tick int64, conditions map[string]interface{}) {
	defer close(ch)
	for offset := 0; ; offset += tx.Limit {
		whereClause := ""
		if tick > 0 {
			whereClause = fmt.Sprintf("where time >= %ds", tick)
		}
		if len(conditions) > 0 {
			for k, v := range conditions {
				sep := " and"
				if whereClause == "" {
					sep = "where"
				}
				whereClause += fmt.Sprintf("%s %s = '%v'", sep, k, v)
			}
		}
		q := fmt.Sprintf("select * from \"%s\".\"%s\" %s order by time desc limit %d offset %d", util.EscapeIdentifier(rp), util.EscapeIdentifier(meas), whereClause, tx.Limit, offset)
		var rsp []byte
		var err error
		for i := 0; i <= RetryCount; i++ {
			if i > 0 {
				time.Sleep(time.Duration(RetryInterval) * time.Second)
				log.Warnf("transfer query retry: %d, err:%s src:%s db:%s rp:%s meas:%s tick:%d limit:%d offset:%d", i, err, src.Url, db, rp, meas, tick, tx.Limit, offset)
			}
			rsp, err = src.QueryIQL("GET", db, q, "ns")
			if err == nil {
				break
			}
		}
		if err != nil {
			ch <- &QueryResult{Err: err}
			return
		}
		series, err := backend.SeriesFromResponseBytes(rsp)
		if err != nil {
			ch <- &QueryResult{Err: err}
			return
		}
		if len(series) == 0 || len(series[0].Values) == 0 {
			return
		}
		ch <- &QueryResult{Series: series}
	}
}

func (tx *Transfer) transfer(src *backend.HttpBackend, dsts []*backend.HttpBackend, db, rp, meas string, tick int64, conditions map[string]interface{}) error {
	ch := make(chan *QueryResult, 4)
	go tx.query(ch, src, db, rp, meas, tick, conditions)

	tags := make(map[string]struct{})
	fields := make(map[string]string)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tagKeys := src.GetTagKeys(db, rp, meas)
		for _, tk := range tagKeys {
			tags[tk] = struct{}{}
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		fieldKeys := src.GetFieldKeys(db, rp, meas)
		fields = reformFieldKeys(fieldKeys)
	}()
	wg.Wait()
	return tx.write(ch, dsts, db, rp, meas, tags, fields)
}

func (tx *Transfer) CopyMeasurement(src *backend.HttpBackend, dsts []*backend.HttpBackend, db, meas string, tick int64) error {
	var err error
	rps := src.GetRetentionPolicies(db)
	for _, rp := range rps {
		rp := rp
		// tx.pool.Submit(func() {
		rpe := tx.transfer(src, dsts, db, rp, meas, tick, map[string]interface{}{})
		if err == nil {
			log.Infof("transfer done, src:%s dst:%v db:%s rp:%s meas:%s tick:%d", src.Url, getBackendUrls(dsts), db, rp, meas, tick)
		} else {
			log.Errorf("transfer error: %s, src:%s dst:%v db:%s rp:%s meas:%s tick:%d", err, src.Url, getBackendUrls(dsts), db, rp, meas, tick)
		}
		err = multierr.Append(err, rpe)
		// })
	}
	return err
}

func (tx *Transfer) CopySeries(src *backend.HttpBackend, dsts []*backend.HttpBackend, db, measurement, tag string, tagVal string) error {
	var err error
	rps := src.GetRetentionPolicies(db)
	for _, rp := range rps {
		rp := rp
		// tx.pool.Submit(func() {
		rpe := tx.transfer(src, dsts, db, rp, measurement, 0, map[string]interface{}{tag: tagVal})
		if err == nil {
			log.Infof("transfer done, src:%s dst:%v db:%s rp:%s meas:%s %s:%s", src.Url, getBackendUrls(dsts), db, rp, measurement, tag, tagVal)
		} else {
			log.Errorf("transfer error: %s, src:%s dst:%v db:%s rp:%s meas:%s %s:%s", err, src.Url, getBackendUrls(dsts), db, rp, measurement, tag, tagVal)
		}
		err = multierr.Append(err, rpe)
		// })
	}
	return err
}
