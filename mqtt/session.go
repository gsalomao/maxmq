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
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
)

// ClientID represents the MQTT Client ID.
type ClientID []byte

// ErrSessionNotFound indicates that the session was not found in the store.
var ErrSessionNotFound = errors.New("session not found")

// SessionStore is responsible for manage sessions in the store.
type SessionStore interface {
	// GetSession gets the session from the store.
	GetSession(id ClientID) (*Session, error)

	// SaveSession saves the session into the store.
	SaveSession(s *Session) error

	// DeleteSession deletes the session from the store.
	DeleteSession(s *Session) error
}

// Session stores the MQTT session.
type Session struct {
	// ClientID represents the ID of the client owner of the session.
	ClientID ClientID

	// KeepAlive represents the MQTT keep alive of the session.
	KeepAlive int

	// ConnectedAt represents the timestamp of the last connection.
	ConnectedAt int64

	// ExpiryInterval represents the interval, in seconds, which the session
	// expires.
	ExpiryInterval uint32

	// Version represents the MQTT version.
	Version packet.MQTTVersion

	connected bool
	restored  bool
}

func newSession(keepAlive int) Session {
	return Session{KeepAlive: keepAlive}
}

type sessionManager struct {
	conf           *Configuration
	metrics        *metrics
	log            *logger.Logger
	store          SessionStore
	userProperties []packet.UserProperty
}

func newSessionManager(c *Configuration, m *metrics, p []packet.UserProperty,
	s SessionStore, l *logger.Logger) sessionManager {

	return sessionManager{
		conf:           c,
		metrics:        m,
		userProperties: p,
		store:          s,
		log:            l,
	}
}

func (m *sessionManager) handlePacket(s *Session,
	pkt packet.Packet) (reply packet.Packet, err error) {

	if !s.connected && pkt.Type() != packet.CONNECT {
		return nil, fmt.Errorf("received %v before CONNECT", pkt.Type())
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return m.handleConnect(s, connPkt)
	case packet.PINGREQ:
		return m.handlePingReq(s)
	case packet.DISCONNECT:
		disconnect := pkt.(*packet.Disconnect)
		return m.handleDisconnect(s, disconnect)
	default:
		return nil, errors.New("invalid packet type: " + pkt.Type().String())
	}
}

func (m *sessionManager) handleConnect(s *Session,
	pkt *packet.Connect) (packet.Packet, error) {

	m.log.Trace().
		Bytes("ClientID", pkt.ClientID).
		Uint8("Version", uint8(pkt.Version)).
		Uint16("KeepAlive", pkt.KeepAlive).
		Msg("MQTT Received CONNECT Packet")

	if pktErr := m.checkPacketConnect(pkt); pktErr != nil {
		connAck := newConnAck(pkt.Version, pktErr.ReasonCode, s.KeepAlive,
			false, m.conf, nil, nil)
		return &connAck, pktErr
	}

	version := pkt.Version
	id, idCreated := getClientID(pkt, m.conf.ClientIDPrefix)
	exp := getSessionExpiryInterval(pkt, m.conf.MaxSessionExpiryInterval)

	err := m.restoreSession(s, id, version, exp)
	if err != nil {
		return nil, err
	}

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(version, code, int(pkt.KeepAlive), s.restored,
		m.conf, pkt.Properties, m.userProperties)

	addAssignedClientID(&connAck, s, idCreated)
	s.connected = true
	s.KeepAlive = int(pkt.KeepAlive)

	if connAck.Properties != nil && connAck.Properties.ServerKeepAlive != nil {
		s.KeepAlive = int(*connAck.Properties.ServerKeepAlive)
	}

	m.log.Debug().
		Bytes("ClientID", s.ClientID).
		Int("KeepAlive", s.KeepAlive).
		Msg("MQTT Client connected")

	return &connAck, nil
}

func (m *sessionManager) handlePingReq(s *Session) (packet.Packet, error) {
	m.log.Trace().
		Bytes("ClientID", s.ClientID).
		Uint8("Version", uint8(s.Version)).
		Int("KeepAlive", s.KeepAlive).
		Msg("MQTT Received PINGREQ Packet")

	pkt := packet.NewPingResp()
	return &pkt, nil
}

func (m *sessionManager) handleDisconnect(s *Session,
	pkt *packet.Disconnect) (packet.Packet, error) {

	m.log.Trace().
		Bytes("ClientID", s.ClientID).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Received DISCONNECT Packet")

	m.disconnectSession(s)
	err := m.store.DeleteSession(s)
	if err == nil {
		m.log.Debug().
			Bytes("ClientID", s.ClientID).
			Msg("MQTT Session deleted with success")
	} else {
		m.log.Error().
			Bytes("ClientID", s.ClientID).
			Msg("MQTT Failed to delete session: " + err.Error())
	}

	latency := time.Since(pkt.Timestamp())
	m.metrics.recordDisconnectLatency(latency)

	return nil, nil
}

func (m *sessionManager) checkPacketConnect(pkt *packet.Connect) *packet.Error {
	if err := m.checkKeepAlive(pkt); err != nil {
		return err
	}
	if err := m.checkClientID(pkt); err != nil {
		return err
	}

	return nil
}

func (m *sessionManager) checkKeepAlive(pkt *packet.Connect) *packet.Error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be
	// overwritten in the CONNACK Packet.
	if pkt.Version == packet.MQTT50 || m.conf.MaxKeepAlive == 0 {
		return nil
	}

	keepAlive := int(pkt.KeepAlive)
	if keepAlive == 0 || keepAlive > m.conf.MaxKeepAlive {
		// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
		// clients what Keep Alive value they should use. If an MQTT
		// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
		// MaxKeepAlive, the CONNACK Packet shall be sent with the reason code
		// "identifier rejected".
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (m *sessionManager) checkClientID(pkt *packet.Connect) *packet.Error {
	idLen := len(pkt.ClientID)

	if idLen == 0 && !m.conf.AllowEmptyClientID {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	if pkt.Version == packet.MQTT31 && idLen > 23 {
		return packet.ErrV3IdentifierRejected
	}

	if idLen > m.conf.MaxClientIDLen {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (m *sessionManager) restoreSession(s *Session, id ClientID,
	version packet.MQTTVersion, exp uint32) error {

	session, err := m.store.GetSession(id)
	if err != nil {
		if err == ErrSessionNotFound {
			m.log.Debug().
				Bytes("ClientID", id).
				Uint8("Version", uint8(version)).
				Msg("MQTT Session not found")
		} else {
			m.log.Error().
				Bytes("ClientID", id).
				Uint8("Version", uint8(version)).
				Msg("MQTT Failed to get session: " + err.Error())
			return err
		}

		session = &Session{
			ClientID:       id,
			ExpiryInterval: exp,
		}
	} else {
		session.restored = true
		m.log.Debug().
			Bytes("ClientID", id).
			Msg("MQTT Session found")
	}

	s.ClientID = session.ClientID
	s.restored = session.restored
	s.Version = version
	s.ConnectedAt = time.Now().UnixMilli()

	err = m.store.SaveSession(s)
	if err != nil {
		m.log.Error().
			Bytes("ClientID", s.ClientID).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Failed to save session: " + err.Error())
		return err
	}

	m.log.Debug().
		Bytes("ClientID", s.ClientID).
		Int64("ConnectedAt", s.ConnectedAt).
		Uint32("ExpiryInterval", s.ExpiryInterval).
		Uint8("Version", uint8(s.Version)).
		Msg("MQTT Session saved with success")
	return nil
}

func (m *sessionManager) disconnectSession(s *Session) {
	if !s.connected {
		return
	}

	s.connected = false
	m.metrics.recordDisconnection()
}
