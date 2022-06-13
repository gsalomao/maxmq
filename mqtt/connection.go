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
	"errors"
	"io"
	"net"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
)

// Connection represents a network connection.
type Connection struct {
	netConn net.Conn
	client  client
}

// ConnectionHandler is responsible for handle connections.
type ConnectionHandler interface {
	// Handle handles the network connection.
	Handle(nc net.Conn) error
}

// ConnectionManager implements the ConnectionHandler interface.
type ConnectionManager struct {
	conf           Configuration
	log            *logger.Logger
	metrics        *metrics
	userProperties []packet.UserProperty
	reader         packet.Reader
	writer         packet.Writer
	sessionStore   SessionStore
}

// NewConnectionManager creates a new ConnectionManager.
func NewConnectionManager(
	cf Configuration,
	st SessionStore,
	lg *logger.Logger,
) ConnectionManager {
	cf.BufferSize = bufferSizeOrDefault(cf.BufferSize)
	cf.MaxPacketSize = maxPacketSizeOrDefault(cf.MaxPacketSize)
	cf.ConnectTimeout = connectTimeoutOrDefault(cf.ConnectTimeout)
	cf.MaximumQoS = maximumQosOrDefault(cf.MaximumQoS)
	cf.MaxTopicAlias = maxTopicAliasOrDefault(cf.MaxTopicAlias)
	cf.MaxInflightMessages = maxInflightMsgOrDefault(cf.MaxInflightMessages)
	cf.MaxClientIDLen = maxClientIDLenOrDefault(cf.MaxClientIDLen)

	userProps := make([]packet.UserProperty, 0, len(cf.UserProperties))
	for k, v := range cf.UserProperties {
		userProps = append(userProps,
			packet.UserProperty{Key: []byte(k), Value: []byte(v)})
	}

	rdOpts := packet.ReaderOptions{
		BufferSize:    cf.BufferSize,
		MaxPacketSize: cf.MaxPacketSize,
	}

	return ConnectionManager{
		conf:           cf,
		log:            lg,
		metrics:        newMetrics(cf.MetricsEnabled, lg),
		userProperties: userProps,
		reader:         packet.NewReader(rdOpts),
		writer:         packet.NewWriter(cf.BufferSize),
		sessionStore:   st,
	}
}

// Handle handles the network connection.
func (cm *ConnectionManager) Handle(nc net.Conn) error {
	conn := cm.newConnection(nc)
	defer conn.client.close(true)

	cm.metrics.recordConnection()
	cm.log.Debug().
		Uint16("Timeout", conn.client.keepAlive).
		Msg("MQTT Handling connection")

	deadline := time.Now()
	deadline = deadline.Add(time.Duration(conn.client.keepAlive) * time.Second)
	session := &conn.client.session

	for {
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Msg("MQTT Failed to set read deadline: " + err.Error())
			return errors.New("failed to set read deadline: " + err.Error())
		}

		cm.log.Trace().
			Uint16("Timeout", conn.client.keepAlive).
			Msg("MQTT Waiting packet")

		pkt, err := cm.reader.ReadPacket(conn.netConn, session.Version)
		if err != nil {
			if err == io.EOF {
				cm.log.Debug().Msg("MQTT Connection was closed")
				return nil
			}

			if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
				cm.log.Debug().
					Bytes("ClientID", session.ClientID).
					Bool("Connected", conn.client.connected).
					Uint16("Timeout", conn.client.keepAlive).
					Msg("MQTT Timeout - No packet received")
				return errors.New("timeout - no packet received")
			}

			cm.log.Info().Msg("MQTT Failed to read packet: " + err.Error())
			return errors.New("failed to read packet: " + err.Error())
		}

		cm.metrics.recordPacketReceived(pkt)
		cm.log.Debug().
			Uint8("PacketTypeID", uint8(pkt.Type())).
			Msg("MQTT Received packet")

		err = conn.client.handlePacket(pkt)
		if err != nil {
			cm.log.Warn().
				Bytes("ClientID", session.ClientID).
				Stringer("PacketType", pkt.Type()).
				Msg("MQTT Failed to process packet: " + err.Error())
			return errors.New("failed to process packet: " + err.Error())
		}

		if !conn.client.connected {
			return nil
		}
		deadline = nextConnectionDeadline(conn)
	}
}

func (cm *ConnectionManager) newConnection(nc net.Conn) Connection {
	return Connection{
		netConn: nc,
		client: newClient(clientOptions{
			log:            cm.log,
			metrics:        cm.metrics,
			conf:           &cm.conf,
			writer:         &cm.writer,
			userProperties: cm.userProperties,
			sessionStore:   cm.sessionStore,
			netConn:        nc,
			keepAlive:      uint16(cm.conf.ConnectTimeout),
		}),
	}
}
