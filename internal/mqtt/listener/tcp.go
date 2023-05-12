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

package listener

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/gsalomao/maxmq/internal/logger"
)

// TCPListener is a listener which listens for TCP connections.
type TCPListener struct {
	// Unexported fields
	log      *logger.Logger
	listener net.Listener
	address  string
	stop     atomic.Bool
	wg       sync.WaitGroup
}

// NewTCPListener creates a new instance of the TCPListener.
func NewTCPListener(address string, l *logger.Logger) *TCPListener {
	return &TCPListener{
		address: address,
		log:     l.WithPrefix("mqtt.tcp"),
	}
}

// Name returns the name of the listener.
func (l *TCPListener) Name() string {
	return "tcp"
}

// Listen starts listening for TCP connections without blocking the caller.
func (l *TCPListener) Listen() (c <-chan net.Conn, err error) {
	l.log.Debug().
		Str("Address", l.address).
		Msg("Starting TCP Listener")

	l.listener, err = net.Listen("tcp", l.address)
	if err != nil {
		l.log.Error().
			Str("Address", l.address).
			Msg("Failed to start TCP Listener: " + err.Error())
		return nil, fmt.Errorf("failed to start TCP Listener: %w", err)
	}

	connStream := make(chan net.Conn)
	starting := make(chan struct{})
	l.wg.Add(1)

	go func() {
		defer l.wg.Done()
		close(starting)
		l.acceptConnections(connStream)
	}()

	<-starting
	l.log.Debug().
		Str("Address", l.address).
		Msg("TCP Listener started with success")
	return connStream, nil
}

// Close closes the listener. Once called, it blocks the caller until the listener has stopped to accept new
// connections.
func (l *TCPListener) Close() error {
	l.log.Debug().
		Str("Address", l.address).
		Msg("Closing TCP Listener")

	l.stop.Store(true)
	if l.listener != nil {
		err := l.listener.Close()
		if err != nil {
			l.log.Error().
				Str("Address", l.address).
				Msg("Failed to close Listener: " + err.Error())
			return err
		}
	}

	l.wg.Wait()
	l.log.Debug().
		Str("Address", l.address).
		Msg("TCP Listener closed with success")
	return nil
}

func (l *TCPListener) acceptConnections(connStream chan<- net.Conn) {
	defer close(connStream)

	for {
		conn, err := l.listener.Accept()
		if err != nil {
			if l.stop.Load() {
				break
			}

			l.log.Warn().
				Str("Address", l.address).
				Msg("Failed to accept TCP connection: " + err.Error())
			continue
		}

		l.log.Debug().
			Str("Address", l.address).
			Msg("New TCP connection from " + conn.RemoteAddr().String())
		connStream <- conn
	}
}
