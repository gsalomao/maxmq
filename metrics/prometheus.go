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
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus represents a Runner responsible for exporting metrics.
type Prometheus struct {
	conf   Configuration
	server *http.Server
	log    *logger.Logger
}

// NewPrometheus creates a Prometheus instance.
func NewPrometheus(c Configuration, log *logger.Logger) (*Prometheus, error) {
	if c.Address == "" {
		return nil, errors.New("metrics missing address")
	}
	if c.Path == "" {
		return nil, errors.New("metrics missing path")
	}

	mux := http.NewServeMux()
	mux.Handle(c.Path, promhttp.Handler())
	srv := &http.Server{
		Addr:         c.Address,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	return &Prometheus{conf: c, server: srv, log: log}, nil
}

// Run starts the execution of the Prometheus.
// Once called, it blocks waiting for connections until it's stopped by the
// Stop function.
func (p *Prometheus) Run() error {
	lsn, err := net.Listen("tcp", p.server.Addr)
	if err != nil {
		return err
	}

	p.log.Info().Msg("Metrics Listening on " + lsn.Addr().String() +
		p.conf.Path)

	if err := p.server.Serve(lsn); err != http.ErrServerClosed {
		return err
	}

	p.log.Debug().Msg("Metrics Server stopped with success")
	return nil
}

// Stop stops the Prometheus.
// Once called, it unblocks the Run function.
func (p *Prometheus) Stop() {
	p.log.Debug().Msg("Metrics Stopping server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := p.server.Shutdown(ctx)
	if err != nil {
		_ = p.server.Close()
	}
}
