// Copyright 2023 The MaxMQ Authors
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

package metric

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	DefaultAddress = ":8888"
	DefaultPath    = "/metrics"
)

// Server represents an HTTP server responsible for exporting metrics.
type Server struct {
	address string
	path    string
	profile bool
	mux     *http.ServeMux
	srv     *http.Server
	log     *logger.Logger
}

// NewServer creates a metrics Server instance.
func NewServer(log *logger.Logger, opts ...Option) *Server {
	s := Server{
		address: DefaultAddress,
		path:    DefaultPath,
		mux:     http.NewServeMux(),
		log:     log,
	}

	for _, opt := range opts {
		opt(&s)
	}

	s.mux.Handle(s.path, promhttp.Handler())
	if s.profile {
		s.mux.HandleFunc("/debug/pprof/", pprof.Index)
		s.mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		s.mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		s.mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		s.mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	s.srv = &http.Server{
		Addr:         s.address,
		Handler:      s.mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return &s
}

// Serve starts the server.
func (s *Server) Serve(ctx context.Context) error {
	lsn, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		s.log.Error(ctx, "Failed to start listener",
			logger.Str("address", s.address),
			logger.Str("path", s.path),
		)
		return err
	}

	s.log.Info(ctx, "Metrics server listening on "+lsn.Addr().String(),
		logger.Str("address", s.address),
		logger.Str("path", s.path),
	)

	for {
		err = s.srv.Serve(lsn)
		if err != nil {
			break
		}
	}

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	s.log.Debug(ctx, "Metrics server stopped with success")
	return nil
}

// Shutdown gracefully shuts down the server without interrupting any active connections.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Close immediately closes all active listeners and any connections.
func (s *Server) Close() error {
	return s.srv.Close()
}
