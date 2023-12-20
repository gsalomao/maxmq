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
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/gsalomao/maxmq/internal/logger"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
)

// Server is a MQTT broker responsible for implementing the MQTT 3.1, 3.1.1, and 5.0 specifications.
// To create a Server instance, use the NewServer factory functions.
type Server struct {
	conf  Config
	ctx   context.Context
	log   *logger.Logger
	mochi *mochi.Server
	wg    sync.WaitGroup
}

func NewServer(ctx context.Context, log *logger.Logger, opts ...Option) *Server {
	srvOpts := Options{Config: NewDefaultConfig()}
	mochiOpts := &mochi.Options{
		Capabilities: mochi.DefaultServerCapabilities,
		Logger:       slog.New(&mochiLoggerWrapper{log: log, ctx: ctx}),
	}

	for _, opt := range opts {
		opt(&srvOpts)
	}

	srv := &Server{
		ctx:  ctx,
		log:  log,
		conf: *srvOpts.Config,
	}

	var sharedSubAvailable byte
	var retainAvailable byte
	var wildcardSubAvailable byte
	var subIDAvailable byte

	if srv.conf.SharedSubscriptionAvailable {
		sharedSubAvailable = 1
	}
	if srv.conf.RetainAvailable {
		retainAvailable = 1
	}
	if srv.conf.WildcardSubscriptionAvailable {
		wildcardSubAvailable = 1
	}
	if srv.conf.SubscriptionIDAvailable {
		subIDAvailable = 1
	}

	mochiOpts.Capabilities.MaximumMessageExpiryInterval = int64(srv.conf.MaxMessageExpiryInterval)
	mochiOpts.Capabilities.MaximumClientWritesPending = int32(srv.conf.MaxOutboundMessages)
	mochiOpts.Capabilities.MaximumSessionExpiryInterval = uint32(srv.conf.MaxSessionExpiryInterval)
	mochiOpts.Capabilities.MaximumPacketSize = uint32(srv.conf.MaxPacketSize)
	mochiOpts.Capabilities.ReceiveMaximum = uint16(srv.conf.ReceiveMaximum)
	mochiOpts.Capabilities.TopicAliasMaximum = uint16(srv.conf.MaxTopicAlias)
	mochiOpts.Capabilities.SharedSubAvailable = sharedSubAvailable
	mochiOpts.Capabilities.MinimumProtocolVersion = srv.conf.MinProtocolVersion
	mochiOpts.Capabilities.MaximumQos = srv.conf.MaximumQoS
	mochiOpts.Capabilities.RetainAvailable = retainAvailable
	mochiOpts.Capabilities.WildcardSubAvailable = wildcardSubAvailable
	mochiOpts.Capabilities.SubIDAvailable = subIDAvailable

	mochiOpts.ClientNetWriteBufferSize = srv.conf.BufferSize
	mochiOpts.ClientNetReadBufferSize = srv.conf.BufferSize
	mochiOpts.SysTopicResendInterval = int64(srv.conf.SysTopicUpdateInterval)

	srv.mochi = mochi.New(mochiOpts)
	_ = srv.mochi.AddHook(&mochiLoggerHook{log: log, ctx: ctx}, nil)

	return srv
}

func (s *Server) Serve() error {
	return s.mochi.Serve()
}

// Shutdown gracefully closes the server.
//
// This method blocks until the server has been closed or the context has been cancelled.
// If the server has already closed, this function has no side effect.
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.mochi.Close()
	if err != nil {
		return fmt.Errorf("failed to close mochi: %w", err)
	}

	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		s.wg.Wait()
	}()

	select {
	case <-doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close closes the Server without waiting for a graceful stop.
func (s *Server) Close() error {
	return s.mochi.Close()
}

// AddListener adds a listener to the server with the provided configuration.
func (s *Server) AddListener(l Listener) error {
	return s.mochi.AddListener(&listenerMochiAdaptor{
		listener: l,
		log:      s.log,
		ctx:      logger.Context(s.ctx, logger.Str("listener", l.Name())),
		callback: s.onConnection,
	})
}

func (s *Server) onConnection(l listeners.Listener, nc net.Conn, fn listeners.EstablishFn) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		err := fn(l.ID(), nc)
		if err != nil {
			s.log.Warn(s.ctx, "Failed to handle connection",
				logger.Err(err),
				logger.Str("address", nc.RemoteAddr().String()),
			)
		}
	}()
}
