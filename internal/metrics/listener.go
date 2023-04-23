// Copyright 2022-2023 The MaxMQ Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Listener represents an HTTP server responsible for exporting metrics.
type Listener struct {
	conf Configuration
	mux  *http.ServeMux
	srv  *http.Server
	log  *logger.Logger
	wg   sync.WaitGroup
}

// NewListener creates a Metrics Listener instance.
func NewListener(c Configuration, log *logger.Logger) (l *Listener, err error) {
	if c.Address == "" {
		return nil, errors.New("metrics missing address")
	}
	if c.Path == "" {
		return nil, errors.New("metrics missing path")
	}

	lg := log.WithPrefix("metrics")
	m := http.NewServeMux()
	m.Handle(c.Path, promhttp.Handler())
	if c.Profiling {
		lg.Info().Msg("Profiling metrics enabled")
		m.HandleFunc("/debug/pprof/", pprof.Index)
		m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		m.HandleFunc("/debug/pprof/profile", pprof.Profile)
		m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		m.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	s := &http.Server{
		Addr:         c.Address,
		Handler:      m,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	l = &Listener{conf: c, srv: s, mux: m, log: lg}
	return l, nil
}

// Start starts the execution of the listener.
func (l *Listener) Start() error {
	l.log.Debug().
		Str("Address", l.conf.Address).
		Str("Path", l.conf.Path).
		Msg("Exporting metrics")

	lsn, err := net.Listen("tcp", l.srv.Addr)
	if err != nil {
		return err
	}

	l.log.Info().Msg("Listening on " + lsn.Addr().String())

	starting := make(chan bool)
	l.wg.Add(1)

	go func() {
		defer l.wg.Done()
		close(starting)

		for {
			err = l.srv.Serve(lsn)
			if err == http.ErrServerClosed {
				break
			}
		}
	}()

	<-starting
	l.log.Debug().Msg("Listener started with success")
	return nil
}

// Stop stops the Metrics Listener. Once called, it unblocks the listener.
func (l *Listener) Stop() {
	l.log.Debug().Msg("Stopping listener")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := l.srv.Shutdown(ctx)
	if err != nil {
		_ = l.srv.Close()
	}

	l.wg.Wait()
	l.log.Debug().Msg("Listener stopped with success")
}

func (l *Listener) Wait() {
	l.wg.Wait()
}
