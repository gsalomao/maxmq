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
)

// Listener is an interface for network listeners.
type Listener interface {
	// Start starts the listener.
	Start() error

	// Stop stops the listener.
	Stop()
}

type serverListener struct {
	listener Listener
	running  bool
}

// Server represents the MaxMQ server.
type Server struct {
	log       *logger.Logger
	wg        sync.WaitGroup
	listeners []serverListener
}

// New creates a new server.
func New(l *logger.Logger) *Server {
	return &Server{log: l.WithPrefix("server")}
}

// AddListener adds a listener to the server.
func (s *Server) AddListener(l Listener) {
	s.listeners = append(s.listeners, serverListener{listener: l})
}

// Start starts the server running all listeners.
func (s *Server) Start() error {
	s.log.Info().Msg("Starting server")

	if len(s.listeners) == 0 {
		return errors.New("no available listener")
	}

	var err error

	for i := range s.listeners {
		lsn := &s.listeners[i]

		err = lsn.listener.Start()
		if err != nil {
			break
		}

		lsn.running = true
		s.wg.Add(1)
	}

	if err != nil {
		s.log.Error().Msg("Stopping started listeners due to error: " + err.Error())
		s.stopRunningListeners()
		s.wg.Wait()
		return err
	}

	s.log.Info().Msg("Server started with success")
	return nil
}

// Stop stops the server by stopping all listeners.
func (s *Server) Stop() {
	s.log.Info().Msg("Stopping server")
	s.stopRunningListeners()

	s.wg.Wait()
	s.log.Info().Msg("Server stopped with success")
}

func (s *Server) stopRunningListeners() {
	for _, lsn := range s.listeners {
		if lsn.running {
			lsn.listener.Stop()
			lsn.running = false
			s.wg.Done()
		}
	}
}
