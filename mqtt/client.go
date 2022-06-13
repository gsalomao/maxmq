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
	"fmt"
	"net"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
	"go.uber.org/multierr"
)

// ClientID represents the MQTT Client ID.
type ClientID []byte

type client struct {
	log            *logger.Logger
	metrics        *metrics
	conf           *Configuration
	writer         *packet.Writer
	userProperties []packet.UserProperty
	sessionStore   SessionStore
	netConn        net.Conn
	session        Session
	keepAlive      uint16
	closed         bool
	connected      bool
}

type clientOptions struct {
	log            *logger.Logger
	metrics        *metrics
	conf           *Configuration
	writer         *packet.Writer
	userProperties []packet.UserProperty
	sessionStore   SessionStore
	netConn        net.Conn
	keepAlive      uint16
}

func newClient(opts clientOptions) client {
	return client{
		log:            opts.log,
		metrics:        opts.metrics,
		conf:           opts.conf,
		writer:         opts.writer,
		userProperties: opts.userProperties,
		sessionStore:   opts.sessionStore,
		netConn:        opts.netConn,
		keepAlive:      opts.keepAlive,
	}
}

func (c *client) handlePacket(pkt packet.Packet) error {
	if !c.connected && pkt.Type() != packet.CONNECT {
		return fmt.Errorf("received %v before CONNECT", pkt.Type())
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return c.handlePacketConnect(connPkt)
	case packet.PINGREQ:
		pingReqPkt, _ := pkt.(*packet.PingReq)
		return c.handlePacketPingReq(pingReqPkt)
	case packet.DISCONNECT:
		disconnectPkt, _ := pkt.(*packet.Disconnect)
		return c.handlePacketDisconnect(disconnectPkt)
	default:
		return errors.New("invalid packet type: " + pkt.Type().String())
	}
}

func (c *client) handlePacketConnect(pkt *packet.Connect) error {
	c.log.Trace().
		Bytes("ClientID", pkt.ClientID).
		Uint8("Version", uint8(pkt.Version)).
		Uint16("KeepAlive", pkt.KeepAlive).
		Msg("MQTT Handling CONNECT Packet")

	if err := c.checkPacketConnect(pkt); err != nil {
		return c.sendConnAckOnError(pkt.Version, err, pkt.Timestamp())
	}

	version := pkt.Version
	id, idCreated := getClientID(pkt, c.conf.ClientIDPrefix)
	exp := getSessionExpiryInterval(pkt, c.conf.MaxSessionExpiryInterval)

	session, sessionFound, err := c.findOrCreateSession(id, version, exp)
	if err != nil {
		return err
	}

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(version, code, pkt.KeepAlive, sessionFound, c.conf,
		pkt.Properties, c.userProperties)

	assignClientID(&connAck, session, id, idCreated)

	err = c.sendConnAck(&connAck, pkt.Timestamp())
	if err != nil {
		deleteErr := c.deleteSessionIfCreated(session, !sessionFound)
		if deleteErr != nil {
			return multierr.Combine(err, deleteErr)
		}
		return err
	}

	c.connected = true
	c.session = session
	c.keepAlive = pkt.KeepAlive

	if connAck.Properties != nil && connAck.Properties.ServerKeepAlive != nil {
		c.keepAlive = *connAck.Properties.ServerKeepAlive
	}

	c.log.Debug().
		Bytes("ClientID", c.session.ClientID).
		Uint16("KeepAlive", c.keepAlive).
		Msg("MQTT Client connected")

	return nil
}

func (c *client) handlePacketPingReq(pkt *packet.PingReq) error {
	c.log.Trace().
		Bytes("ClientID", c.session.ClientID).
		Uint8("Version", uint8(c.session.Version)).
		Uint16("KeepAlive", c.keepAlive).
		Msg("MQTT Handling PINGREQ Packet")

	return c.sendPingResp(pkt.Timestamp())
}

func (c *client) handlePacketDisconnect(pkt *packet.Disconnect) error {
	c.log.Trace().
		Bytes("ClientID", c.session.ClientID).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Handling DISCONNECT Packet")

	c.close(false)
	err := c.sessionStore.DeleteSession(c.session)
	if err == nil {
		c.log.Debug().
			Bytes("ClientID", c.session.ClientID).
			Msg("MQTT Session deleted with success")
	} else {
		c.log.Error().
			Bytes("ClientID", c.session.ClientID).
			Msg("MQTT Failed to delete session: " + err.Error())
	}

	c.metrics.recordDisconnectLatency(time.Since(pkt.Timestamp()))
	return nil
}

func (c *client) close(force bool) {
	if c.closed {
		return
	}

	if tcp, ok := c.netConn.(*net.TCPConn); ok && force {
		_ = tcp.SetLinger(0)
	}

	c.log.Debug().
		Bytes("ClientID", c.session.ClientID).
		Bool("Force", force).
		Msg("MQTT Closing connection")

	_ = c.netConn.Close()
	c.closed = true
	c.connected = false
	c.metrics.recordDisconnection()
}

func (c *client) checkPacketConnect(pkt *packet.Connect) *packet.Error {
	if err := c.checkKeepAlive(pkt); err != nil {
		return err
	}
	if err := c.checkClientID(pkt); err != nil {
		return err
	}

	return nil
}

func (c *client) checkKeepAlive(pkt *packet.Connect) *packet.Error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be
	// overwritten in the CONNACK Packet.
	if pkt.Version == packet.MQTT50 || c.conf.MaxKeepAlive == 0 {
		return nil
	}

	keepAlive := int(pkt.KeepAlive)
	if keepAlive == 0 || keepAlive > c.conf.MaxKeepAlive {
		// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
		// clients what Keep Alive value they should use. If an MQTT
		// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
		// MaxKeepAlive, the CONNACK Packet shall be sent with the reason code
		// "identifier rejected".
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (c *client) checkClientID(pkt *packet.Connect) *packet.Error {
	idLen := len(pkt.ClientID)

	if idLen == 0 && !c.conf.AllowEmptyClientID {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	if pkt.Version == packet.MQTT31 && idLen > 23 {
		return packet.ErrV3IdentifierRejected
	}

	if idLen > c.conf.MaxClientIDLen {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (c *client) findOrCreateSession(id ClientID, version packet.MQTTVersion,
	exp uint32) (s Session, found bool, err error) {

	s, err = c.sessionStore.GetSession(id)
	if err != nil {
		if err == ErrSessionNotFound {
			c.log.Debug().
				Bytes("ClientID", id).
				Uint8("Version", uint8(version)).
				Msg("MQTT Session not found")
		} else {
			c.log.Error().
				Bytes("ClientID", id).
				Uint8("Version", uint8(version)).
				Msg("MQTT Failed to get session")
			return
		}

		s = Session{ClientID: id, ExpiryInterval: exp}
	} else {
		found = true
		c.log.Debug().
			Bytes("ClientID", id).
			Msg("MQTT Session found")
	}

	s.Version = version
	s.ConnectedAt = time.Now().UnixMilli()

	err = c.sessionStore.SaveSession(s)
	if err != nil {
		c.log.Error().
			Bytes("ClientID", id).
			Uint8("Version", uint8(version)).
			Msg("MQTT Failed to save session: " + err.Error())
		return
	}

	c.log.Debug().
		Bytes("ClientID", id).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Uint8("Version", uint8(version)).
		Msg("MQTT Session saved with success")
	return
}

func (c *client) deleteSessionIfCreated(session Session, created bool) error {
	if created {
		err := c.sessionStore.DeleteSession(session)
		if err != nil {
			c.log.Error().
				Bytes("ClientID", session.ClientID).
				Uint8("Version", uint8(session.Version)).
				Msg("MQTT Failed to delete session on CONNACK error")
			return err
		}
	}
	return nil
}

func (c *client) sendConnAck(pkt *packet.ConnAck, start time.Time) error {
	c.log.Trace().
		Bytes("ClientID", c.session.ClientID).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Sending CONNACK Packet")

	tm, err := c.sendPacket(pkt)
	if err != nil {
		return err
	}

	latency := tm.Sub(start)
	c.metrics.recordConnectLatency(latency, int(pkt.ReasonCode))
	return nil
}

func (c *client) sendConnAckOnError(ver packet.MQTTVersion,
	err *packet.Error, start time.Time) error {

	code := err.ReasonCode
	connAck := newConnAck(ver, code, c.keepAlive, false, c.conf, nil, nil)

	errSendConnAck := c.sendConnAck(&connAck, start)
	return multierr.Combine(err, errSendConnAck)
}

func (c *client) sendPingResp(start time.Time) error {
	pkt := packet.NewPingResp()
	c.log.Trace().
		Bytes("ClientID", c.session.ClientID).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Uint16("KeepAlive", c.keepAlive).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Sending PINGRESP Packet")

	tm, err := c.sendPacket(&pkt)
	if err != nil {
		return err
	}

	latency := tm.Sub(start)
	c.metrics.recordPingLatency(latency)
	return nil
}

func (c *client) sendPacket(pkt packet.Packet) (time.Time, error) {
	c.log.Debug().
		Bytes("ClientID", c.session.ClientID).
		Uint8("PacketTypeID", uint8(pkt.Type())).
		Uint8("Version", uint8(c.session.Version)).
		Msg("MQTT Sending packet")

	err := c.writer.WritePacket(pkt, c.netConn)
	if err != nil {
		c.log.Error().
			Bytes("ClientID", c.session.ClientID).
			Stringer("PacketType", pkt.Type()).
			Uint8("Version", uint8(c.session.Version)).
			Msg("MQTT Failed to send packet: " + err.Error())
		return time.Now(), err
	}

	c.metrics.recordPacketSent(pkt)
	return time.Now(), nil
}
