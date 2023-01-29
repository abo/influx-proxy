// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import (
	"errors"

	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/sharding"
	jsoniter "github.com/json-iterator/go"
	"github.com/spf13/viper"
)

var (
	Version   = "not build"
	GitCommit = "not build"
	BuildTime = "not build"
)

var (
	ErrEmptyMeasurements = errors.New("measurements cannot be empty")
	ErrEmptyBackendUrl   = errors.New("node url cannot be empty")
	ErrDuplicatedBackend = errors.New("node url duplicated")
	ErrEmptyBackends     = errors.New("non nodes")
)

type Config struct {
	Proxy     *ProxyConfig       `mapstructure:"proxy"`
	Sharding  []*sharding.Config `mapstructure:"sharding"`
	DataNodes []*BackendConfig   `mapstructure:"data-nodes"`
	Logging   log.Options        `mapstructure:"logging"`
}

type BackendConfig struct { // nolint:golint
	// Name        string `mapstructure:"name"`
	Url         string `mapstructure:"url"` // nolint:golint
	Username    string `mapstructure:"username"`
	Password    string `mapstructure:"password"`
	AuthEncrypt bool   `mapstructure:"auth-encrypt"`
	WriteOnly   bool   `mapstructure:"write-only"`
}

type ProxyConfig struct {
	ListenAddr   string   `mapstructure:"bind-addr"`
	HTTPSCert    string   `mapstructure:"https-cert"`
	HTTPSKey     string   `mapstructure:"https-cert-key"`
	Username     string   `mapstructure:"username"`
	Password     string   `mapstructure:"password"`
	AuthEncrypt  bool     `mapstructure:"auth_encrypt"`
	DataDir      string   `mapstructure:"data_dir"`
	Measurements []string `mapstructure:"measurements"`

	FlushSize       int `mapstructure:"flush-size"`
	FlushTime       int `mapstructure:"flush-time"`
	CheckInterval   int `mapstructure:"node-check-interval"`
	RewriteInterval int `mapstructure:"rewrite-interval"`
	ConnPoolSize    int `mapstructure:"flush-pool-size"`
	WriteTimeout    int `mapstructure:"node-write-timeout"`
	IdleTimeout     int `mapstructure:"idle-timeout"`

	// WriteTracing    bool `mapstructure:"write_tracing"`
	// QueryTracing    bool `mapstructure:"query_tracing"`
	// Circles    []*CircleConfig `mapstructure:"circles"`
	// TLogDir         string          `mapstructure:"tlog_dir"`
	// HashKey         string `mapstructure:"hash_key"`
	// PprofEnabled    bool   `mapstructure:"pprof_enabled"`
}

// type LoggingConfig struct {
// 	Level        string `mapstructure:"level"`
// 	File         string `mapstructure:"file"` // if empty, output to stdout
// 	MaxSizeMB    int    `mapstructure:"file-max-size"`
// 	MaxAgeDays   int    `mapstructure:"file-max-age"`
// 	WriteTracing bool   `mapstructure:"write-tracing"`
// 	QueryTracing bool   `mapstructure:"query-tracing"`
// }

func NewFileConfig(cfgfile string) (cfg *Config, err error) {
	viper.SetConfigFile(cfgfile)
	err = viper.ReadInConfig()
	if err != nil {
		return
	}
	cfg = &Config{
		Proxy:     &ProxyConfig{},
		Sharding:  []*sharding.Config{},
		DataNodes: []*BackendConfig{},
		Logging:   log.Options{},
	}
	err = viper.Unmarshal(cfg)
	if err != nil {
		return
	}
	cfg.setDefault()
	err = cfg.checkConfig()
	return
}

func (cfg *Config) setDefault() {
	if cfg.Proxy.ListenAddr == "" {
		cfg.Proxy.ListenAddr = ":7076"
	}
	if cfg.Proxy.DataDir == "" {
		cfg.Proxy.DataDir = "data"
	}
	if cfg.Proxy.FlushSize <= 0 {
		cfg.Proxy.FlushSize = 10000
	}
	if cfg.Proxy.FlushTime <= 0 {
		cfg.Proxy.FlushTime = 1
	}
	if cfg.Proxy.CheckInterval <= 0 {
		cfg.Proxy.CheckInterval = 1
	}
	if cfg.Proxy.RewriteInterval <= 0 {
		cfg.Proxy.RewriteInterval = 10
	}
	if cfg.Proxy.ConnPoolSize <= 0 {
		cfg.Proxy.ConnPoolSize = 20
	}
	if cfg.Proxy.WriteTimeout <= 0 {
		cfg.Proxy.WriteTimeout = 10
	}
	if cfg.Proxy.IdleTimeout <= 0 {
		cfg.Proxy.IdleTimeout = 10
	}
}

func (cfg *Config) checkConfig() (err error) {
	urls := make(map[string]struct{})
	for _, backend := range cfg.DataNodes {
		if backend.Url == "" {
			return ErrEmptyBackendUrl
		}
		if _, prs := urls[backend.Url]; prs {
			return ErrDuplicatedBackend
		}
		urls[backend.Url] = struct{}{}
	}
	if len(cfg.Proxy.Measurements) == 0 {
		return ErrEmptyMeasurements
	}
	return nil
}

func (cfg *Config) PrintSummary() {
	// log.Printf("%d circles loaded from file", len(cfg.Circles))
	// for id, circle := range cfg.Circles {
	// 	log.Printf("circle %d: %d backends loaded", id, len(circle.Backends))
	// }
	// log.Printf("hash key: %s", cfg.HashKey)
	// if len(cfg.DBList) > 0 {
	// 	log.Printf("db list: %v", cfg.DBList)
	// }
	// log.Printf("auth: %t, encrypt: %t", cfg.Username != "" || cfg.Password != "", cfg.AuthEncrypt)
}

func (cfg *Config) String() string {
	json := jsoniter.Config{TagKey: "mapstructure"}.Froze()
	b, _ := json.Marshal(cfg)
	return string(b)
}
