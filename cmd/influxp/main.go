// Copyright 2023 Shengbo Huang. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	glog "log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/abo/influx-proxy/backend"
	"github.com/abo/influx-proxy/log"
	"github.com/abo/influx-proxy/service"
)

var (
	Version   = "dev"
	Commit    = "dev"
	BuildTime = "none"
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
	fmt.Printf("Version:    %s\n", Version)
	fmt.Printf("Git commit: %s\n", Commit)
	fmt.Printf("Build time: %s\n", BuildTime)
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
		fmt.Printf("illegal config file: %v\n", err)
		return
	}
	log.Infof("version: %s, commit: %s, build: %s", Version, Commit, BuildTime)
	cfg.PrintSummary()
	log.Init(cfg.Logging)
	defer log.Flush()

	svc, err := service.NewHttpService(cfg)
	if err != nil {
		log.Error(err)
		return
	}
	server := &http.Server{
		Addr: cfg.Proxy.ListenAddr,
		Handler: svc.Handler(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("X-Influxdb-Version", Version)
				w.Header().Add("X-Influxdb-Build", "Proxy")
				next.ServeHTTP(w, r)
			})
		}),
		IdleTimeout: time.Duration(cfg.Proxy.IdleTimeout) * time.Second,
	}

	signs := make(chan os.Signal, 1)
	signal.Notify(signs, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		buf := make([]byte, 1<<20)
		for {
			sign := <-signs
			switch sign {
			case os.Interrupt, syscall.SIGTERM:
				server.Close()
				svc.Stop()
				return
			case syscall.SIGQUIT:
				stacklen := runtime.Stack(buf, true)
				glog.Printf("\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
			}
		}
	}()

	if cfg.Proxy.HTTPSCert != "" || cfg.Proxy.HTTPSKey != "" {
		log.Infof("influxproxy started at %s(https)", server.Addr)
		err = server.ListenAndServeTLS(cfg.Proxy.HTTPSCert, cfg.Proxy.HTTPSKey)
	} else {
		log.Infof("influxproxy started at %s(http)", server.Addr)
		err = server.ListenAndServe()
	}
	if err != nil && err != http.ErrServerClosed {
		log.Errorf("influxproxy quit with err: %v", err)
	} else {
		log.Infof("influxproxy stopped")
	}
}
