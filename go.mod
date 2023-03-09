module github.com/abo/influx-proxy

go 1.18

require (
	github.com/bmizerany/pat v0.0.0-20210406213842-e4b6760bdd6f
	github.com/gogo/protobuf v1.3.2
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang/snappy v0.0.4
	github.com/google/go-cmp v0.5.9
	github.com/gorilla/mux v1.8.0
	github.com/influxdata/flux v0.188.0
	github.com/influxdata/influxdb v1.11.0
	github.com/influxdata/influxdb1-client v0.0.0-20220302092344-a9ab5670611c
	github.com/influxdata/influxql v1.1.1-0.20211004132434-7e7d61973256
	github.com/json-iterator/go v1.1.12
	github.com/panjf2000/ants/v2 v2.4.8
	github.com/prometheus/client_golang v1.14.0
	github.com/prometheus/prometheus v0.42.0
	github.com/spf13/viper v1.10.1
	go.etcd.io/etcd/client/pkg/v3 v3.5.7
	go.etcd.io/etcd/raft/v3 v3.5.7
	go.etcd.io/etcd/server/v3 v3.5.7
	go.uber.org/multierr v1.9.0
	go.uber.org/zap v1.24.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

require (
	cloud.google.com/go v0.107.0 // indirect
	cloud.google.com/go/bigquery v1.44.0 // indirect
	cloud.google.com/go/iam v0.12.0 // indirect
	cloud.google.com/go/longrunning v0.4.1 // indirect
	github.com/BurntSushi/toml v1.2.1 // indirect
	github.com/Masterminds/semver v1.4.2 // indirect
	github.com/Masterminds/sprig v2.16.0+incompatible // indirect
	github.com/andreyvit/diff v0.0.0-20170406064948-c7f18ee00883 // indirect
	github.com/aokoli/goutils v1.0.1 // indirect
	github.com/apache/arrow/go/v7 v7.0.1 // indirect
	github.com/benbjohnson/immutable v0.3.0 // indirect
	github.com/benbjohnson/tmpl v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cockroachdb/datadriven v1.0.2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/deepmap/oapi-codegen v1.6.0 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eclipse/paho.mqtt.golang v1.2.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20180323001048-9f0cb55181dd // indirect
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31 // indirect
	github.com/go-chi/chi v4.1.0+incompatible // indirect
	github.com/goccy/go-json v0.9.6 // indirect
	github.com/gofrs/uuid v3.3.0+incompatible // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/flatbuffers v22.9.30-0.20221019131441-5792623df42e+incompatible // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/huandu/xstrings v1.0.0 // indirect
	github.com/imdario/mergo v0.3.12 // indirect
	github.com/influxdata/httprouter v1.3.1-0.20191122104820-ee83e2772f69 // indirect
	github.com/influxdata/influxdb-client-go/v2 v2.3.1-0.20210518120617-5d1fff431040 // indirect
	github.com/influxdata/line-protocol v0.0.0-20200327222509-2487e7298839 // indirect
	github.com/influxdata/line-protocol/v2 v2.2.1 // indirect
	github.com/influxdata/pkg-config v0.2.11 // indirect
	github.com/influxdata/roaring v0.4.13-0.20180809181101-fc520f41fab6 // indirect
	github.com/jsternberg/zap-logfmt v1.2.0 // indirect
	github.com/magiconair/properties v1.8.5 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pelletier/go-toml v1.9.4 // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.39.0 // indirect
	github.com/prometheus/procfs v0.8.0 // indirect
	github.com/sergi/go-diff v1.1.0 // indirect
	github.com/spf13/afero v1.6.0 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	github.com/subosito/gotenv v1.2.0 // indirect
	github.com/tinylib/msgp v1.1.0 // indirect
	github.com/uber/jaeger-client-go v2.28.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/willf/bitset v1.1.9 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/xlab/treeprint v0.0.0-20180616005107-d6fb6747feb6 // indirect
	go.etcd.io/etcd/api/v3 v3.5.7 // indirect
	go.etcd.io/etcd/pkg/v3 v3.5.7 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/mod v0.7.0 // indirect
	golang.org/x/net v0.6.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.3.0 // indirect
	golang.org/x/tools v0.5.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/api v0.110.0 // indirect
	google.golang.org/genproto v0.0.0-20230216225411-c8e22ba71e44 // indirect
	google.golang.org/grpc v1.53.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/ini.v1 v1.66.6 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
