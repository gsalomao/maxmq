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

	"github.com/gsalomao/maxmq/pkg/logger"
	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
)

var errSessionNotFound = errors.New("session not found")

type sessionManager struct {
	deliverer      packetDeliverer
	pubSub         pubSub
	store          store
	idGen          IDGenerator
	conf           *Configuration
	metrics        *metrics
	log            *logger.Logger
	userProperties []packet.UserProperty
}

func newSessionManager(deliverer packetDeliverer, idGen IDGenerator,
	conf *Configuration, metrics *metrics, props []packet.UserProperty,
	log *logger.Logger) *sessionManager {

	sm := sessionManager{
		deliverer:      deliverer,
		store:          newStore(),
		idGen:          idGen,
		conf:           conf,
		metrics:        metrics,
		userProperties: props,
		log:            log,
	}

	sm.pubSub = newPubSub(&sm, idGen, metrics, log)
	return &sm
}

func (m *sessionManager) start() {
	m.log.Trace().Msg("MQTT Starting session manager")
	m.pubSub.start()
}

func (m *sessionManager) stop() {
	m.log.Trace().Msg("MQTT Stopping session manager")
	m.pubSub.stop()
	m.log.Debug().Msg("MQTT Session manager stopped with success")
}

func (m *sessionManager) handlePacket(session *Session,
	pkt packet.Packet) (*Session, []packet.Packet, error) {

	if pkt.Type() != packet.CONNECT && (session == nil || !session.connected) {
		return session, nil, fmt.Errorf("received %v before CONNECT",
			pkt.Type())
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return m.handleConnect(session, connPkt)
	case packet.PINGREQ:
		return m.handlePingReq(session)
	case packet.SUBSCRIBE:
		subPkt, _ := pkt.(*packet.Subscribe)
		return m.handleSubscribe(session, subPkt)
	case packet.UNSUBSCRIBE:
		unsubPkt, _ := pkt.(*packet.Unsubscribe)
		return m.handleUnsubscribe(session, unsubPkt)
	case packet.PUBLISH:
		pubPkt, _ := pkt.(*packet.Publish)
		return m.handlePublish(session, pubPkt)
	case packet.PUBACK:
		pubAckPkt, _ := pkt.(*packet.PubAck)
		return m.handlePubAck(session, pubAckPkt)
	case packet.DISCONNECT:
		disconnect := pkt.(*packet.Disconnect)
		return m.handleDisconnect(session, disconnect)
	default:
		return session, nil, fmt.Errorf("invalid packet type: %v",
			pkt.Type().String())
	}
}

func (m *sessionManager) handleConnect(session *Session,
	pkt *packet.Connect) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Bool("CleanSession", pkt.CleanSession).
		Bytes("ClientId", pkt.ClientID).
		Uint16("KeepAlive", pkt.KeepAlive).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Received CONNECT packet")

	if pktErr := m.checkPacketConnect(pkt); pktErr != nil {
		connAck := newConnAck(pkt.Version, pktErr.ReasonCode, 0,
			false, m.conf, nil, nil)
		return session, []packet.Packet{&connAck}, pktErr
	}

	var id ClientID
	var idCreated bool
	if len(pkt.ClientID) > 0 {
		id = ClientID(pkt.ClientID)
	} else {
		id = createClientID(m.conf.ClientIDPrefix)
		idCreated = true
	}

	exp := getSessionExpiryIntervalOnConnect(pkt,
		m.conf.MaxSessionExpiryInterval)

	session, err := m.readSession(id)
	if err == nil && pkt.CleanSession {
		m.cleanSession(session)
	}
	if session == nil {
		session = m.newSession(id)
		session.KeepAlive = m.conf.ConnectTimeout
	}

	session.Version = pkt.Version
	session.CleanSession = pkt.CleanSession
	session.ExpiryInterval = exp
	session.ConnectedAt = time.Now().UnixMilli()
	session.connected = true
	session.KeepAlive = getSessionKeepAlive(m.conf, int(pkt.KeepAlive))

	m.log.Info().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Client connected")

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(session.Version, code, int(pkt.KeepAlive),
		session.restored, m.conf, pkt.Properties, m.userProperties)

	addAssignedClientID(&connAck, session.Version, session.ClientID, idCreated)

	replies := make([]packet.Packet, 0, 1)
	replies = append(replies, &connAck)

	inflightMsg := session.inflightMessages.front()
	for inflightMsg != nil {
		replies = append(replies, inflightMsg.packet)
		inflightMsg.tries++
		inflightMsg.lastSent = time.Now().UnixMicro()
		inflightMsg = inflightMsg.next
	}

	m.saveSession(session)
	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			m.log.Trace().
				Str("ClientId", string(session.ClientID)).
				Uint64("SessionId", uint64(session.SessionID)).
				Bool("SessionPresent", connAck.SessionPresent).
				Uint8("Version", uint8(connAck.Version)).
				Msg("MQTT Sending CONNACK packet")
		} else if reply.Type() == packet.PUBLISH {
			pub := reply.(*packet.Publish)
			m.log.Trace().
				Str("ClientId", string(session.ClientID)).
				Uint16("PacketId", uint16(pub.PacketID)).
				Uint8("QoS", uint8(pub.QoS)).
				Uint8("Retain", pub.Retain).
				Uint64("SessionId", uint64(session.SessionID)).
				Str("TopicName", pub.TopicName).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Sending PUBLISH packet")
		}
	}

	return session, replies, nil
}

func (m *sessionManager) handlePingReq(session *Session) (*Session,
	[]packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received PINGREQ packet")

	reply := packet.NewPingResp()
	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Sending PINGRESP packet")

	return session, []packet.Packet{&reply}, nil
}

func (m *sessionManager) handleSubscribe(session *Session,
	pkt *packet.Subscribe) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Int("Topics", len(pkt.Topics)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Received SUBSCRIBE packet")

	replies := make([]packet.Packet, 0, 1)

	subscriptionID := getSubscriptionID(pkt.Properties)
	if subscriptionID > 0 && !m.conf.SubscriptionIDAvailable {
		m.log.Info().
			Str("ClientId", string(session.ClientID)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint64("SessionId", uint64(session.SessionID)).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Received SUBSCRIBE with subscription ID (not available)")

		disconnect := packet.NewDisconnect(session.Version,
			packet.ReasonCodeV5SubscriptionIDNotSupported, nil)
		replies = append(replies, &disconnect)
		return session, replies, packet.ErrV5SubscriptionIDNotSupported
	}

	codes := make([]packet.ReasonCode, 0, len(pkt.Topics))

	for _, topic := range pkt.Topics {
		subscription, err := m.pubSub.subscribe(session, topic, subscriptionID)
		if err != nil {
			codes = append(codes, packet.ReasonCodeV3Failure)
			continue
		}

		codes = append(codes, packet.ReasonCode(subscription.QoS))
		session.Subscriptions[subscription.TopicFilter] = subscription

		m.log.Info().
			Str("ClientId", string(session.ClientID)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", byte(topic.QoS)).
			Uint64("SessionId", uint64(session.SessionID)).
			Int("Subscriptions", len(session.Subscriptions)).
			Str("TopicFilter", topic.Name).
			Int("Topics", len(pkt.Topics)).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Client subscribed to topic")
	}

	m.saveSession(session)
	subAck := packet.NewSubAck(pkt.PacketID, pkt.Version, codes, nil)
	replies = append(replies, &subAck)
	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(subAck.PacketID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(subAck.Version)).
		Msg("MQTT Sending SUBACK packet")

	return session, replies, nil
}

func (m *sessionManager) handleUnsubscribe(session *Session,
	pkt *packet.Unsubscribe) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Int("Topics", len(pkt.Topics)).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Received UNSUBSCRIBE packet")

	var codes []packet.ReasonCode
	if pkt.Version == packet.MQTT50 {
		codes = make([]packet.ReasonCode, 0, len(pkt.Topics))
	}

	for _, topic := range pkt.Topics {
		code := packet.ReasonCodeV5UnspecifiedError
		err := m.pubSub.unsubscribe(session.ClientID, topic)
		if err == nil {
			code = packet.ReasonCodeV5Success
			delete(session.Subscriptions, topic)
			m.log.Info().
				Str("ClientId", string(session.ClientID)).
				Uint16("PacketId", uint16(pkt.PacketID)).
				Uint64("SessionId", uint64(session.SessionID)).
				Int("Subscriptions", len(session.Subscriptions)).
				Str("TopicFilter", topic).
				Int("Topics", len(pkt.Topics)).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Client unsubscribed to topic")
		} else if err == ErrSubscriptionNotFound {
			code = packet.ReasonCodeV5NoSubscriptionExisted
		}

		if pkt.Version == packet.MQTT50 {
			codes = append(codes, code)
		}
	}

	m.saveSession(session)
	replies := make([]packet.Packet, 0, 1)

	unsubAck := packet.NewUnsubAck(pkt.PacketID, pkt.Version, codes, nil)
	replies = append(replies, &unsubAck)
	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(unsubAck.PacketID)).
		Int("ReasonCodes", len(unsubAck.ReasonCodes)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(unsubAck.Version)).
		Msg("MQTT Sending UNSUBACK packet")

	return session, replies, nil
}

func (m *sessionManager) handlePublish(session *Session,
	pkt *packet.Publish) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint64("SessionId", uint64(session.SessionID)).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received PUBLISH packet")

	msg := m.pubSub.publish(pkt)
	m.log.Info().
		Str("ClientId", string(session.ClientID)).
		Uint8("DUP", msg.packet.Dup).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packet.PacketID)).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Uint64("SessionId", uint64(session.SessionID)).
		Str("TopicName", msg.packet.TopicName).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Client published a packet")

	if pkt.QoS > packet.QoS0 {
		replies := make([]packet.Packet, 0, 1)
		pubAck := packet.NewPubAck(pkt.PacketID, pkt.Version,
			packet.ReasonCodeV5Success, nil)

		replies = append(replies, &pubAck)
		m.log.Trace().
			Str("ClientId", string(session.ClientID)).
			Uint16("PacketId", uint16(pubAck.PacketID)).
			Uint8("Version", uint8(pubAck.Version)).
			Msg("MQTT Sending PUBACK packet")
		return session, replies, nil
	}

	return session, nil, nil
}

func (m *sessionManager) handlePubAck(session *Session,
	pkt *packet.PubAck) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received PUBACK packet")

	inflightMsg := session.inflightMessages.find(pkt.PacketID)
	if inflightMsg == nil {
		return session, nil, fmt.Errorf(
			"packet ID %v not found for client %s",
			pkt.PacketID, session.ClientID)
	}
	session.inflightMessages.remove(pkt.PacketID)
	m.saveSession(session)

	m.log.Info().
		Str("ClientId", string(session.ClientID)).
		Uint64("MessageId", uint64(inflightMsg.messageID)).
		Int("Messages", session.inflightMessages.len()).
		Uint16("PacketId", uint16(inflightMsg.packet.PacketID)).
		Uint8("QoS", uint8(inflightMsg.packet.QoS)).
		Uint8("Retain", inflightMsg.packet.Retain).
		Str("TopicName", inflightMsg.packet.TopicName).
		Uint8("Version", uint8(inflightMsg.packet.Version)).
		Msg("MQTT Message published to client")

	return session, nil, nil
}

func (m *sessionManager) handleDisconnect(session *Session,
	pkt *packet.Disconnect) (*Session, []packet.Packet, error) {

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received DISCONNECT packet")

	if session.Version == packet.MQTT50 && pkt.Properties != nil {
		interval := pkt.Properties.SessionExpiryInterval

		if interval != nil && *interval > 0 && session.ExpiryInterval == 0 {
			m.log.Debug().
				Str("ClientId", string(session.ClientID)).
				Uint8("Version", uint8(session.Version)).
				Uint32("SessionExpiryInterval", *interval).
				Uint64("SessionId", uint64(session.SessionID)).
				Msg("MQTT DISCONNECT with invalid Session Expiry Interval")

			replies := make([]packet.Packet, 0)
			disconnect := packet.NewDisconnect(session.Version,
				packet.ReasonCodeV5ProtocolError, nil)
			replies = append(replies, &disconnect)
			return session, replies, packet.ErrV5ProtocolError
		}

		if interval != nil && *interval != session.ExpiryInterval {
			session.ExpiryInterval = *interval
		}
	}

	m.disconnectSession(session)
	latency := time.Since(pkt.Timestamp())
	m.metrics.recordDisconnectLatency(latency)

	m.log.Info().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Int("Messages", session.inflightMessages.len()).
		Uint32("SessionExpiryInterval", session.ExpiryInterval).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Client disconnected")

	return session, nil, nil
}

func (m *sessionManager) publishMessage(session *Session, msg *message) error {
	pkt := msg.packet
	if pkt.QoS > packet.QoS0 {
		pkt = msg.packet.Clone()
		pkt.PacketID = session.nextClientID()
	}
	if pkt.Version != session.Version {
		pkt.Version = session.Version
	}

	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Uint64("MessageId", uint64(msg.id)).
		Int("Messages", session.inflightMessages.len()).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Uint64("SessionId", uint64(session.SessionID)).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Publishing message to client")

	var inflightMsg *inflightMessage
	if pkt.QoS > 0 {
		inflightMsg = &inflightMessage{packetID: pkt.PacketID,
			messageID: msg.id, packet: pkt}
		session.inflightMessages.add(inflightMsg)
		m.saveSession(session)
	}

	if session.connected {
		err := m.deliverer.deliverPacket(session.SessionID, pkt)
		if err != nil {
			return err
		}

		if inflightMsg != nil {
			inflightMsg.tries++
			inflightMsg.lastSent = time.Now().UnixMicro()
			m.saveSession(session)
		}

		if pkt.QoS == packet.QoS0 {
			m.log.Info().
				Str("ClientId", string(session.ClientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pkt.PacketID)).
				Uint8("QoS", uint8(pkt.QoS)).
				Uint8("Retain", pkt.Retain).
				Uint64("SessionId", uint64(session.SessionID)).
				Str("TopicName", pkt.TopicName).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Message published to client")
		} else {
			m.log.Debug().
				Str("ClientId", string(session.ClientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pkt.PacketID)).
				Uint8("QoS", uint8(pkt.QoS)).
				Uint8("Retain", pkt.Retain).
				Uint64("SessionId", uint64(session.SessionID)).
				Str("TopicName", pkt.TopicName).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Message delivered to client")
		}
	} else if pkt.QoS > packet.QoS0 {
		m.log.Info().
			Str("ClientId", string(session.ClientID)).
			Uint64("MessageId", uint64(msg.id)).
			Int("Messages", session.inflightMessages.len()).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", uint8(pkt.QoS)).
			Uint8("Retain", pkt.Retain).
			Uint64("SessionId", uint64(session.SessionID)).
			Str("TopicName", pkt.TopicName).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Publication postponed, client not connected")
	} else {
		m.log.Info().
			Str("ClientId", string(session.ClientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", uint8(pkt.QoS)).
			Uint8("Retain", pkt.Retain).
			Uint64("SessionId", uint64(session.SessionID)).
			Str("TopicName", pkt.TopicName).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Message dropped, client not connected")
	}

	return nil
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

func (m *sessionManager) newSession(id ClientID) *Session {
	nextId := m.idGen.NextID()
	session := &Session{
		ClientID:      id,
		SessionID:     SessionID(nextId),
		Subscriptions: make(map[string]Subscription),
	}
	m.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Msg("MQTT New session created")

	return session
}

func (m *sessionManager) readSession(id ClientID) (*Session, error) {
	m.log.Trace().
		Str("ClientId", string(id)).
		Msg("MQTT Reading session")

	session, err := m.store.readSession(id)
	if err == errSessionNotFound {
		m.log.Debug().
			Str("ClientId", string(id)).
			Msg("MQTT Session not found")
		return nil, err
	}

	session.restored = true
	m.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session read with success")

	return session, nil
}

func (m *sessionManager) saveSession(session *Session) {
	m.log.Trace().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Saving session")

	m.store.saveSession(session)

	m.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session saved with success")
}

func (m *sessionManager) deleteSession(session *Session) {
	m.log.Trace().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Deleting session")

	m.store.deleteSession(session)

	m.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("KeepAlive", session.KeepAlive).
		Int("Messages", session.inflightMessages.len()).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session deleted with success")
}

func (m *sessionManager) cleanSession(session *Session) {
	for _, sub := range session.Subscriptions {
		err := m.pubSub.unsubscribe(session.ClientID, sub.TopicFilter)
		if err != nil {
			m.log.Error().
				Str("ClientId", string(session.ClientID)).
				Bool("Connected", session.connected).
				Uint64("SessionId", uint64(session.SessionID)).
				Str("Topic", sub.TopicFilter).
				Uint8("Version", uint8(session.Version)).
				Msg("MQTT Failed to unsubscribe when cleaning session")
		}
	}
	session.clean()
}

func (m *sessionManager) disconnectSession(session *Session) {
	if !session.connected {
		return
	}

	session.connected = false

	if session.CleanSession && (session.Version != packet.MQTT50 ||
		session.ExpiryInterval == 0) {
		m.cleanSession(session)
		m.deleteSession(session)
	} else {
		m.saveSession(session)
	}
}
