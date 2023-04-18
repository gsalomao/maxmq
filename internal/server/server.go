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

package server

import (
	"errors"
	"sync"

	"github.com/gsalomao/maxmq/internal/logger"
	"go.uber.org/multierr"
)

// Listener is an interface for network listeners.
type Listener interface {
	// Listen starts listening and block until the listener stops.
	Listen() error

	// Stop stops the listener unblocking the Listen function.
	Stop()
}

// Server represents the MaxMQ server.
type Server struct {
	log       *logger.Logger
	wg        sync.WaitGroup
	listeners []Listener
	err       error
}

// New creates a new server.
func New(l *logger.Logger) *Server {
	return &Server{log: l.WithPrefix("server")}
}

// AddListener adds a listener to the server.
func (s *Server) AddListener(l Listener) {
	s.listeners = append(s.listeners, l)
}

// Start starts the server running all listeners.
func (s *Server) Start() error {
	s.log.Info().Msg("Starting server")

	if len(s.listeners) == 0 {
		return errors.New("no available listener")
	}

	var starting sync.WaitGroup
	starting.Add(len(s.listeners))
	s.wg.Add(len(s.listeners))

	for _, lsn := range s.listeners {
		go func(l Listener) {
			defer s.wg.Done()
			starting.Done()

			err := l.Listen()
			if err != nil {
				s.err = multierr.Combine(s.err, err)
			}
		}(lsn)
	}

	starting.Wait()
	s.log.Info().Msg("Server started with success")
	return nil
}

// Stop stops the server by stopping all listeners.
func (s *Server) Stop() {
	s.log.Info().Msg("Stopping server")

	for _, l := range s.listeners {
		l.Stop()
	}
}

// Wait blocks while the server is running.
func (s *Server) Wait() error {
	s.wg.Wait()
	s.log.Info().Msg("Server stopped")
	return s.err
}
