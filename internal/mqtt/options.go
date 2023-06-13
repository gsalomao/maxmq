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
	"github.com/gsalomao/maxmq/internal/logger"
)

// OptionFn is a function responsible to inject an option inside the MQTT Server.
type OptionFn func(*Server)

// WithConfig is an option function to inject the Config inside the MQTT Server.
func WithConfig(c *Config) OptionFn {
	return func(s *Server) {
		s.conf = c
	}
}

// WithLogger is an option function to inject the logger.Logger inside the MQTT Server.
func WithLogger(l *logger.Logger) OptionFn {
	return func(s *Server) {
		s.log = l
	}
}

// WithMachineID is an option function to inject the machine ID inside the MQTT Server.
func WithMachineID(id int) OptionFn {
	return func(s *Server) {
		s.machineID = id
	}
}
