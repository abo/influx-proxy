// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"net/http"
	"runtime"
	"time"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/service"
)

var (
	configFile string
	version    bool
)

func init() {
	log.Init(log.Options{Level: "info", File: ""})

	flag.StringVar(&configFile, "config", "proxy.json", "proxy config file with json/yaml/toml format")
	flag.BoolVar(&version, "version", false, "proxy version")
	flag.Parse()
}

func printVersion() {
	fmt.Printf("Version:    %s\n", backend.Version)
	fmt.Printf("Git commit: %s\n", backend.GitCommit)
	fmt.Printf("Build time: %s\n", backend.BuildTime)
	fmt.Printf("Go version: %s\n", runtime.Version())
	fmt.Printf("OS/Arch:    %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func main() {
	if version {
		printVersion()
		return
	}

	cfg, err := backend.NewFileConfig(configFile)
	if err != nil {
		fmt.Printf("illegal config file: %s\n", err)
		return
	}
	log.Infof("version: %s, commit: %s, build: %s", backend.Version, backend.GitCommit, backend.BuildTime)
	cfg.PrintSummary()
	log.Init(cfg.Logging)

	server := &http.Server{
		Addr:        cfg.Proxy.ListenAddr,
		Handler:     service.NewHttpService(cfg).Handler(),
		IdleTimeout: time.Duration(cfg.Proxy.IdleTimeout) * time.Second,
	}

	if cfg.Proxy.HTTPSCert != "" || cfg.Proxy.HTTPSKey != "" {
		log.Infof("https service start, listen on %s", server.Addr)
		err = server.ListenAndServeTLS(cfg.Proxy.HTTPSCert, cfg.Proxy.HTTPSKey)
	} else {
		log.Infof("http service start, listen on %s", server.Addr)
		err = server.ListenAndServe()
	}
	if err != nil {
		log.Error(err)
		return
	}
}
