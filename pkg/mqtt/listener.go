/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt

import (
	"errors"
	"net"
	"sync"

	"github.com/gsalomao/maxmq/pkg/logger"
)

// Listener is responsible to implement the MQTT protocol conform the v3.1.1
// and v5.0 specifications.
type Listener struct {
	log         *logger.Logger
	listener    net.Listener
	connHandler ConnectionHandler
	running     bool
	mtx         sync.Mutex
}

// NewListener creates a new MQTT listener with the given options.
func NewListener(opts ...OptionsFn) (*Listener, error) {
	mqtt := &Listener{}

	for _, fn := range opts {
		fn(mqtt)
	}

	if mqtt.log == nil {
		return nil, errors.New("missing logger")
	}
	if mqtt.listener == nil {
		return nil, errors.New("missing TCP listener")
	}
	if mqtt.connHandler == nil {
		return nil, errors.New("missing connection handler")
	}

	return mqtt, nil
}

// Run starts the execution of the MQTT listener.
// Once called, it blocks waiting for connections until it's stopped by the
// Stop function.
func (mqtt *Listener) Run() error {
	mqtt.log.Info().Msg("MQTT listening on " + mqtt.listener.Addr().String())
	mqtt.setRunningState(true)

	for {
		conn, err := mqtt.listener.Accept()
		if err != nil {
			if !mqtt.isRunning() {
				break
			}

			mqtt.log.Warn().Msg("MQTT failed to accept TCP connection: " +
				err.Error())

			continue
		}

		addr := conn.RemoteAddr().String()
		mqtt.log.Trace().Msg("New TCP connection from " + addr)
		mqtt.connHandler.Handle(conn)
	}

	mqtt.log.Debug().Msg("MQTT listener stopped with success")
	return nil
}

// Stop stops the MQTT listener.
// Once called, it unblocks the Run function.
func (mqtt *Listener) Stop() {
	mqtt.log.Debug().Msg("Stopping MQTT listener")

	mqtt.setRunningState(false)
	mqtt.listener.Close()
}

func (mqtt *Listener) setRunningState(st bool) {
	mqtt.mtx.Lock()
	defer mqtt.mtx.Unlock()

	mqtt.running = st
}

func (mqtt *Listener) isRunning() bool {
	mqtt.mtx.Lock()
	defer mqtt.mtx.Unlock()

	return mqtt.running
}
