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

package mqtt

import (
	"errors"
	"net"
	"sync"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
)

// Listener is responsible to implement the MQTT protocol conform the v3.1, v3.1.1, and v5.0
// specifications.
type Listener struct {
	tcpLsn        net.Listener
	idGen         IDGenerator
	log           *logger.Logger
	conf          *handler.Configuration
	connectionMgr *connectionManager
	running       bool
	mtx           sync.Mutex
}

// NewListener creates a new MQTT Listener with the given options.
func NewListener(opts ...OptionsFn) (*Listener, error) {
	l := &Listener{}

	for _, fn := range opts {
		fn(l)
	}

	if l.log == nil {
		return nil, errors.New("missing logger")
	}
	if l.conf == nil {
		return nil, errors.New("missing configuration")
	}
	if l.idGen == nil {
		return nil, errors.New("missing ID generator")
	}

	mt := newMetrics(l.conf.MetricsEnabled, l.log)
	st := newSessionStore(l.idGen, l.log)
	cm := newConnectionManager(l.conf, st, mt, l.idGen, l.log)

	l.connectionMgr = cm
	return l, nil
}

// Listen starts the execution of the MQTT Listener.
// Once called, it blocks waiting for connections until it's stopped by the Stop function.
func (l *Listener) Listen() error {
	lsn, err := net.Listen("tcp" /*network*/, l.conf.TCPAddress)
	if err != nil {
		return err
	}

	l.connectionMgr.start()

	l.log.Info().Msg("Listening on " + lsn.Addr().String())
	l.tcpLsn = lsn
	l.setRunningState(true)

	for {
		l.log.Trace().Msg("Waiting for TCP connection")

		var tcpConn net.Conn
		tcpConn, err = l.tcpLsn.Accept()
		if err != nil {
			if !l.isRunning() {
				break
			}

			l.log.Warn().Msg("Failed to accept TCP connection: " + err.Error())
			continue
		}

		conn := l.connectionMgr.newConnection(tcpConn)
		l.log.Trace().Msg("New TCP connection")

		go func() { l.connectionMgr.handle(conn) }()
	}

	l.log.Debug().Msg("Listener stopped with success")
	return nil
}

// Stop stops the MQTT Listener.
// Once called, it unblocks the Run function.
func (l *Listener) Stop() {
	l.log.Debug().Msg("Stopping listener")

	l.connectionMgr.stop()
	l.setRunningState(false)
	_ = l.tcpLsn.Close()
}

func (l *Listener) setRunningState(st bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	l.running = st
}

func (l *Listener) isRunning() bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	return l.running
}
