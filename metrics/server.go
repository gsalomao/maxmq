// Copyright 2022 The MaxMQ Authors
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
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server represents an HTTP server responsible for exporting metrics.
type Server struct {
	conf Configuration
	mux  *http.ServeMux
	srv  *http.Server
	log  *logger.Logger
}

// NewServer creates a Server instance.
func NewServer(c Configuration, log *logger.Logger) (*Server, error) {
	if c.Address == "" {
		return nil, errors.New("metrics missing address")
	}
	if c.Path == "" {
		return nil, errors.New("metrics missing path")
	}

	m := http.NewServeMux()
	m.Handle(c.Path, promhttp.Handler())
	if c.Profiling {
		log.Info().Msg("Profiling metrics enabled")
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

	return &Server{conf: c, srv: s, mux: m, log: log}, nil
}

// Run starts the execution of the server.
// Once called, it blocks waiting for connections until it's stopped by the
// Stop function.
func (p *Server) Run() error {
	lsn, err := net.Listen("tcp", p.srv.Addr)
	if err != nil {
		return err
	}

	p.log.Info().Msg("Metrics Listening on " + lsn.Addr().String())
	if err := p.srv.Serve(lsn); err != http.ErrServerClosed {
		return err
	}

	p.log.Debug().Msg("Metrics Server stopped with success")
	return nil
}

// Stop stops the server.
// Once called, it unblocks the Run function.
func (p *Server) Stop() {
	p.log.Debug().Msg("Metrics Stopping server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := p.srv.Shutdown(ctx)
	if err != nil {
		_ = p.srv.Close()
	}
}
