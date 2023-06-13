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
	"os"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/hooks/auth"
	"github.com/mochi-co/mqtt/v2/listeners"
	"github.com/rs/zerolog"
)

// Server is an MQTT server. It must be created using the NewServer function.
type Server struct {
	// Unexported fields
	mochi     *mqtt.Server
	conf      *Config
	log       *logger.Logger
	machineID int
}

// NewServer creates an instance of the MQTT Server. It takes a variable number of OptionFn and returns the Server
// instance or an error.
func NewServer(opts ...OptionFn) (*Server, error) {
	s := &Server{}

	for _, opt := range opts {
		opt(s)
	}

	if s.conf == nil {
		s.conf = &defaultConfig
	}
	if s.log == nil {
		s.log = logger.New(os.Stdout, nil, logger.Json)
	}

	log := zerolog.New(nil)
	opt := &mqtt.Options{
		Capabilities: mqtt.DefaultServerCapabilities,
		Logger:       &log,
	}

	if s.conf != nil {
		var sharedSubAvailable byte
		var retainAvailable byte
		var wildcardSubAvailable byte
		var subIDAvailable byte

		if s.conf.SharedSubscriptionAvailable {
			sharedSubAvailable = 1
		}
		if s.conf.RetainAvailable {
			retainAvailable = 1
		}
		if s.conf.WildcardSubscriptionAvailable {
			wildcardSubAvailable = 1
		}
		if s.conf.SubscriptionIDAvailable {
			subIDAvailable = 1
		}

		opt.Capabilities.MaximumMessageExpiryInterval = int64(s.conf.MaxMessageExpiryInterval)
		opt.Capabilities.MaximumClientWritesPending = int32(s.conf.MaxOutboundMessages)
		opt.Capabilities.MaximumSessionExpiryInterval = uint32(s.conf.MaxSessionExpiryInterval)
		opt.Capabilities.MaximumPacketSize = uint32(s.conf.MaxPacketSize)
		opt.Capabilities.ReceiveMaximum = uint16(s.conf.ReceiveMaximum)
		opt.Capabilities.TopicAliasMaximum = uint16(s.conf.MaxTopicAlias)
		opt.Capabilities.SharedSubAvailable = sharedSubAvailable
		opt.Capabilities.MinimumProtocolVersion = s.conf.MinProtocolVersion
		opt.Capabilities.MaximumQos = s.conf.MaximumQoS
		opt.Capabilities.RetainAvailable = retainAvailable
		opt.Capabilities.WildcardSubAvailable = wildcardSubAvailable
		opt.Capabilities.SubIDAvailable = subIDAvailable

		opt.ClientNetWriteBufferSize = s.conf.BufferSize
		opt.ClientNetReadBufferSize = s.conf.BufferSize
		opt.SysTopicResendInterval = int64(s.conf.SysTopicUpdateInterval)
	}

	s.mochi = mqtt.New(opt)
	_ = s.mochi.AddHook(newLoggingHook(s.log), nil)
	_ = s.mochi.AddHook(new(auth.AllowHook), nil)

	return s, nil
}

// Start starts the MQTT Server. In case of failure, it returns the error.
func (s *Server) Start() error {
	s.log.Debug().Msg("Starting MQTT server")

	err := s.mochi.AddListener(listeners.NewTCP("tcp", s.conf.TCPAddress, nil))
	if err != nil {
		s.log.Error().Err(err).Msg("Failed to add tcp listener")
		return err
	}

	_ = s.mochi.Serve()
	s.log.Info().Msg("MQTT server started with success")
	return nil
}

// Stop stops gracefully the MQTT Server.
func (s *Server) Stop() {
	s.log.Debug().Msg("Stopping MQTT server")

	_ = s.mochi.Close()
	s.log.Info().Msg("MQTT server stopped with success")
}
