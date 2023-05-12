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

package mqtt

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/safe"
)

// Listener is responsible for listen and accept the connections.
type Listener interface {
	// Name returns the name of the listener.
	Name() string

	// Listen starts listening for new connections without blocking the caller.
	Listen() (c <-chan net.Conn, err error)

	// Close closes the listener. Once called, it blocks the caller until the listener has stopped to accept new
	// connections.
	Close() error
}

type listener struct {
	listener   Listener
	name       string
	running    bool
	connStream <-chan net.Conn
}

// Server is an MQTT server.
type Server struct {
	// Unexported fields
	log           *logger.Logger
	connectionMgr *connectionManager
	conf          Config
	listeners     safe.Value[[]*listener]
	waitGroup     sync.WaitGroup
}

// NewServer creates a new instance of the MQTT Server.
func NewServer(c Config, g IDGenerator, l *logger.Logger) *Server {
	store := newSessionStore(g, l)
	mt := newMetrics(c.MetricsEnabled, l)
	return &Server{
		log:           l.WithPrefix("mqtt"),
		connectionMgr: newConnectionManager(c, store, mt, g, l),
		conf:          c,
		listeners:     safe.NewValue(make([]*listener, 0)),
	}
}

// AddListener adds a Listener to the MQTT Server. The listeners must be added into the Server before it has been
// started.
func (s *Server) AddListener(l Listener) {
	s.listeners.Lock()
	defer s.listeners.Unlock()

	lsn := &listener{listener: l, name: l.Name()}
	s.listeners.Value = append(s.listeners.Value, lsn)
	s.log.Info().Str("Name", lsn.name).Msg("Listener added to MQTT server")
}

// Start starts the MQTT Server. The listeners must be added into the Server before it has been started.
func (s *Server) Start() error {
	s.log.Info().Msg("Starting MQTT server")

	connStream, err := s.serveListeners()
	if err != nil {
		s.log.Error().Msg("Failed to start MQTT server: " + err.Error())
		s.Stop()
		return err
	}

	ready := s.run(connStream)
	<-ready

	s.log.Info().Msg("MQTT server started with success")
	return nil
}

// Shutdown stops the MQTT Server gracefully by closing all listeners and waiting for all connections to have been
// closed, or the context has been cancelled.
func (s *Server) Shutdown(ctx context.Context) error {
	s.log.Info().Msg("Shutting down MQTT server")
	s.closeListeners()

	var poolIntervalCnt int
	poolInterval := time.Second

	timer := time.NewTimer(poolInterval)
	defer timer.Stop()

	for {
		if s.connectionMgr.closeIdleConnections() {
			break
		}
		select {
		case <-ctx.Done():
			s.log.Info().Msg("Graceful shutdown timed out")
			return ctx.Err()
		case <-timer.C:
			poolIntervalCnt++
			if poolIntervalCnt == 5 {
				s.log.Info().Msg("Still shutting down the MQTT server")
				poolIntervalCnt = 0
			}
			timer.Reset(poolInterval)
		}
	}

	s.connectionMgr.wait(ctx)
	s.log.Info().Msg("MQTT server stopped with success")
	return nil
}

// Stop stops the MQTT Server by closing all listeners and connections.
func (s *Server) Stop() {
	s.log.Info().Msg("Stopping MQTT server")
	s.closeListeners()

	s.connectionMgr.closeAllConnections()
	s.waitGroup.Wait()
	s.log.Info().Msg("MQTT server stopped with success")
}

func (s *Server) serveListeners() (c <-chan *connection, err error) {
	s.listeners.Lock()
	defer s.listeners.Unlock()

	connStreams := make([]<-chan net.Conn, len(s.listeners.Value))

	for i, lsn := range s.listeners.Value {
		var stream <-chan net.Conn

		stream, err = lsn.listener.Listen()
		if err != nil {
			return nil, err
		}

		lsn.running = true
		lsn.connStream = stream
		connStreams[i] = stream
	}

	return s.mergeConnStreamsLocked(connStreams...), nil
}

func (s *Server) mergeConnStreamsLocked(streams ...<-chan net.Conn) <-chan *connection {
	connStreams := make(chan *connection)

	var wg sync.WaitGroup
	wg.Add(len(streams))

	for i, st := range streams {
		go func(l *listener, stream <-chan net.Conn) {
			defer wg.Done()

			for {
				nc, ok := <-stream
				if !ok {
					break
				}

				c := s.connectionMgr.newConnection(nc)
				c.listener = l.name
				connStreams <- c
			}
		}(s.listeners.Value[i], st)
	}

	s.waitGroup.Add(1)
	go func() {
		defer s.waitGroup.Done()

		wg.Wait()
		close(connStreams)
	}()

	return connStreams
}

func (s *Server) run(connStream <-chan *connection) <-chan struct{} {
	ready := make(chan struct{})
	s.waitGroup.Add(1)

	go func() {
		defer s.waitGroup.Done()

		s.log.Trace().Msg("Waiting for connections")
		close(ready)

		for {
			c, ok := <-connStream
			if !ok {
				break
			}

			s.log.Trace().
				Str("Address", c.netConn.RemoteAddr().String()).
				Str("Listener", c.listener).
				Msg("Accepted new connection")

			s.connectionMgr.addConnection(c)
			s.waitGroup.Add(1)

			go func() {
				defer s.waitGroup.Done()
				s.connectionMgr.serveConnection(c)
			}()
		}
	}()

	return ready
}

func (s *Server) closeListeners() {
	s.listeners.Lock()
	defer s.listeners.Unlock()

	for _, lsn := range s.listeners.Value {
		if lsn.running {
			s.log.Trace().
				Str("Name", lsn.name).
				Msg("Closing Listener")

			err := lsn.listener.Close()
			if err == nil {
				s.log.Debug().
					Str("Name", lsn.name).
					Msg("Listener closed with success")
			} else {
				s.log.Error().
					Str("Name", lsn.name).
					Msg("Listener failed to close: " + err.Error())
			}
			lsn.running = false
		}
	}
}
