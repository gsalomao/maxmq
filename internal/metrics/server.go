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

// Server represents an HTTP server responsible for exporting metrics.
type Server struct {
	conf Configuration
	mux  *http.ServeMux
	srv  *http.Server
	log  *logger.Logger
	wg   sync.WaitGroup
}

// NewServer creates a metrics Server instance.
func NewServer(c Configuration, log *logger.Logger) (s *Server, err error) {
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

	httpSrv := &http.Server{
		Addr:         c.Address,
		Handler:      m,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	s = &Server{conf: c, srv: httpSrv, mux: m, log: lg}
	return s, nil
}

// Start starts the execution of the Server.
func (s *Server) Start() error {
	lsn, err := net.Listen("tcp", s.srv.Addr)
	if err != nil {
		s.log.Error().Err(err).
			Str("Address", s.conf.Address).
			Str("Path", s.conf.Path).
			Msg("Failed to start listener")
		return err
	}

	s.log.Debug().
		Str("Address", s.conf.Address).
		Str("Path", s.conf.Path).
		Msg("Listening on " + lsn.Addr().String())

	starting := make(chan bool)
	s.wg.Add(1)

	go func() {
		defer s.wg.Done()
		close(starting)

		for {
			err = s.srv.Serve(lsn)
			if err == http.ErrServerClosed {
				break
			}
		}
	}()

	<-starting
	s.log.Info().
		Str("Address", s.conf.Address).
		Str("Path", s.conf.Path).
		Msg("Metrics server started with success")
	return nil
}

// Stop stops the Metrics Server. Once called, it unblocks the Server.
func (s *Server) Stop() {
	s.log.Debug().Msg("Stopping metrics server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.srv.Shutdown(ctx)
	if err != nil {
		_ = s.srv.Close()
	}

	s.wg.Wait()
	s.log.Info().Msg("Metrics server stopped with success")
}
