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
	"math"
	"net"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"go.uber.org/multierr"
)

type connection struct {
	netConn net.Conn
	session Session
	closed  bool
}

func (c *connection) nextConnectionDeadline() time.Time {
	if c.session.KeepAlive > 0 {
		timeout := math.Ceil(float64(c.session.KeepAlive) * 1.5)
		return time.Now().Add(time.Duration(timeout) * time.Second)
	}

	// Zero value of time to disable the timeout
	return time.Time{}
}

type connectionManager struct {
	conf           *Configuration
	log            *logger.Logger
	metrics        *metrics
	reader         packet.Reader
	writer         packet.Writer
	sessionManager sessionManager
}

func newConnectionManager(
	cf *Configuration,
	st SessionStore,
	lg *logger.Logger,
) connectionManager {
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

	m := newMetrics(cf.MetricsEnabled, lg)
	return connectionManager{
		conf:           cf,
		log:            lg,
		metrics:        m,
		reader:         packet.NewReader(rdOpts),
		writer:         packet.NewWriter(cf.BufferSize),
		sessionManager: newSessionManager(cf, m, userProps, st, lg),
	}
}

func (cm *connectionManager) handle(nc net.Conn) error {
	conn := cm.createConnection(nc)
	defer cm.closeConnection(&conn, true)

	cm.metrics.recordConnection()
	cm.log.Debug().
		Int("Timeout", conn.session.KeepAlive).
		Msg("MQTT Handling connection")

	deadline := time.Now()
	deadline = deadline.Add(time.Duration(conn.session.KeepAlive) * time.Second)

	for {
		err := conn.netConn.SetReadDeadline(deadline)
		if err != nil {
			cm.log.Error().
				Msg("MQTT Failed to set read deadline: " + err.Error())
			return errors.New("failed to set read deadline: " + err.Error())
		}

		cm.log.Trace().
			Int("Timeout", conn.session.KeepAlive).
			Msg("MQTT Waiting packet")

		pkt, err := cm.readPacket(&conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		err = cm.handlePacket(&conn, pkt)
		if err != nil {
			return err
		}

		if !conn.session.connected {
			cm.closeConnection(&conn, false)
			return nil
		}

		deadline = conn.nextConnectionDeadline()
	}
}

func (cm *connectionManager) readPacket(conn *connection) (pkt packet.Packet,
	err error) {

	pkt, err = cm.reader.ReadPacket(conn.netConn, conn.session.Version)
	if err != nil {
		if err == io.EOF {
			cm.log.Debug().
				Bytes("ClientID", conn.session.ClientID).
				Msg("MQTT Network connection was closed")
			return
		}

		if errCon, ok := err.(net.Error); ok && errCon.Timeout() {
			cm.log.Debug().
				Bytes("ClientID", conn.session.ClientID).
				Bool("Connected", conn.session.connected).
				Int("Timeout", conn.session.KeepAlive).
				Msg("MQTT Timeout - No packet received")
			return nil, errors.New("timeout - no packet received")
		}

		cm.log.Info().Msg("MQTT Failed to read packet: " + err.Error())
		return nil, errors.New("failed to read packet: " + err.Error())
	}

	cm.metrics.recordPacketReceived(pkt)
	cm.log.Debug().
		Bytes("ClientID", conn.session.ClientID).
		Bool("Connected", conn.session.connected).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Msg("MQTT Received packet")
	return
}

func (cm *connectionManager) handlePacket(conn *connection,
	pkt packet.Packet) error {

	reply, err := cm.sessionManager.handlePacket(&conn.session, pkt)
	if err != nil {
		cm.log.Warn().
			Bytes("ClientID", conn.session.ClientID).
			Stringer("PacketType", pkt.Type()).
			Msg("MQTT Failed to handle packet: " + err.Error())
		err = errors.New("failed to handle packet: " + err.Error())
	}

	if reply != nil {
		errReply := cm.replyPacket(pkt, reply, conn)
		if errReply != nil {
			err = multierr.Combine(err,
				errors.New("failed to send packet: "+errReply.Error()))
		}
	}

	return err
}

func (cm *connectionManager) createConnection(nc net.Conn) connection {
	return connection{
		netConn: nc,
		session: newSession(cm.conf.ConnectTimeout),
	}
}

func (cm *connectionManager) closeConnection(conn *connection, force bool) {
	if conn.closed {
		return
	}

	cm.log.Trace().
		Bytes("ClientID", conn.session.ClientID).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	if tcp, ok := conn.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	_ = conn.netConn.Close()
	conn.closed = true

	err := cm.sessionManager.disconnectSession(&conn.session)
	if err != nil {
		cm.log.Error().
			Bytes("ClientID", conn.session.ClientID).
			Bool("Force", force).
			Msg("MQTT Error when disconnecting session on close connection: " +
				err.Error())
	}

	cm.metrics.recordDisconnection()
	cm.log.Debug().
		Bytes("ClientID", conn.session.ClientID).
		Bool("Force", force).
		Msg("MQTT Connection closed")
}

func (cm *connectionManager) replyPacket(pkt packet.Packet, reply packet.Packet,
	c *connection) error {

	cm.log.Trace().
		Bytes("ClientID", c.session.ClientID).
		Uint8("PacketTypeID", uint8(reply.Type())).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Sending packet")

	err := cm.writer.WritePacket(reply, c.netConn)
	if err != nil {
		cm.log.Warn().
			Bytes("ClientID", c.session.ClientID).
			Stringer("PacketType", reply.Type()).
			Uint8("Version", uint8(c.session.Version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		err = multierr.Combine(err,
			errors.New("failed to send packet: "+err.Error()))
	} else {
		cm.recordLatencyMetrics(pkt, reply)
		cm.metrics.recordPacketSent(reply)
		cm.log.Debug().
			Bytes("ClientID", c.session.ClientID).
			Uint8("PacketTypeID", uint8(reply.Type())).
			Uint8("Version", uint8(c.session.Version)).
			Msg("MQTT Packet sent")
	}

	return err
}

func (cm *connectionManager) recordLatencyMetrics(pkt packet.Packet,
	reply packet.Packet) {

	pktType := pkt.Type()
	replyType := reply.Type()
	latency := reply.Timestamp().Sub(pkt.Timestamp())

	if pktType == packet.CONNECT && replyType == packet.CONNACK {
		connAck := reply.(*packet.ConnAck)
		cm.metrics.recordConnectLatency(latency, int(connAck.ReasonCode))
	} else if pktType == packet.PINGREQ && replyType == packet.PINGRESP {
		cm.metrics.recordPingLatency(latency)
	} else if pktType == packet.SUBSCRIBE && replyType == packet.SUBACK {
		cm.metrics.recordSubscribeLatency(latency)
	} else if pktType == packet.UNSUBSCRIBE && replyType == packet.UNSUBACK {
		cm.metrics.recordUnsubscribeLatency(latency)
	}
}
