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

// IDGenerator generates an identifier numbers.
type IDGenerator interface {
	// NextID generates the next identifier number.
	NextID() uint64
}

// Listener is responsible to implement the MQTT protocol conform the v3.1, v3.1.1, and v5.0 specifications.
type Listener struct {
	// Unexported fields
	tcpLsn        net.Listener
	idGen         IDGenerator
	log           *logger.Logger
	connectionMgr *connectionManager
	conf          handler.Configuration
	wg            sync.WaitGroup
	running       bool
	mtx           sync.Mutex
}

// NewListener creates a new MQTT Listener with the given options.
func NewListener(c handler.Configuration, g IDGenerator, l *logger.Logger) (*Listener, error) {
	if g == nil {
		return nil, errors.New("missing ID generator")
	}
	if l == nil {
		return nil, errors.New("missing logger")
	}

	log := l.WithPrefix("mqtt")
	mt := newMetrics(c.MetricsEnabled, log)
	st := newSessionStore(g, log)
	cm := newConnectionManager(&c, st, mt, g, log)

	return &Listener{
		conf:          c,
		idGen:         g,
		log:           log,
		connectionMgr: cm,
	}, nil
}

// Start starts the execution of the MQTT listener.
func (l *Listener) Start() error {
	lsn, err := net.Listen("tcp", l.conf.TCPAddress)
	if err != nil {
		return err
	}

	l.tcpLsn = lsn
	l.connectionMgr.start()
	l.setRunningState(true)
	l.wg.Add(1)

	l.log.Info().Msg("Listening on " + lsn.Addr().String())
	starting := make(chan bool)

	go func() {
		defer l.wg.Done()
		close(starting)

		for {
			var tcpConn net.Conn
			l.log.Trace().Msg("Waiting for TCP connection")

			tcpConn, err = lsn.Accept()
			if err != nil {
				if !l.isRunning() {
					break
				}

				l.log.Warn().Msg("Failed to accept TCP connection: " + err.Error())
				continue
			}

			conn := l.connectionMgr.newConnection(tcpConn)
			l.log.Trace().Msg("New TCP connection")

			l.wg.Add(1)
			go func() {
				defer l.wg.Done()
				l.connectionMgr.handle(conn)
			}()
		}
	}()

	<-starting
	l.log.Debug().Msg("Listener started with success")
	return nil
}

// Stop stops the MQTT listener. Once called, it unblocks the Run function.
func (l *Listener) Stop() {
	l.log.Debug().Msg("Stopping listener")

	l.setRunningState(false)
	_ = l.tcpLsn.Close()
	l.connectionMgr.stop()

	l.wg.Wait()
	l.log.Debug().Msg("Listener stopped with success")
}

func (l *Listener) Wait() {
	l.wg.Wait()
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
