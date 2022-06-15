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

package mqtt

import (
	"github.com/gsalomao/maxmq/logger"
)

// OptionsFn represents a function which sets an option in the MQTT Listener.
type OptionsFn func(l *Listener)

// WithConfiguration sets the MQTT configuration into the MQTT Listener.
func WithConfiguration(cf Configuration) OptionsFn {
	return func(l *Listener) {
		l.conf = &cf
	}
}

// WithSessionStore sets the given SessionStore into the MQTT Listener.
func WithSessionStore(st SessionStore) OptionsFn {
	return func(l *Listener) {
		l.sessionStore = st
	}
}

// WithLogger sets the given Logger into the MQTT Listener.
func WithLogger(log *logger.Logger) OptionsFn {
	return func(l *Listener) {
		l.log = log
	}
}
