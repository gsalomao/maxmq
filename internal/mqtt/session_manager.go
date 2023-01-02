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
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

var errSessionNotFound = errors.New("session not found")

type sessionManager struct {
	conf           *Configuration
	metrics        *metrics
	log            *logger.Logger
	sessions       map[ClientID]*Session
	deliverer      packetDeliverer
	idGen          IDGenerator
	mutex          sync.RWMutex
	pubSub         pubSub
	store          store
	userProperties []packet.UserProperty
}

func newSessionManager(deliverer packetDeliverer, idGen IDGenerator,
	conf *Configuration, metrics *metrics, props []packet.UserProperty,
	log *logger.Logger,
) *sessionManager {
	sm := sessionManager{
		conf:           conf,
		metrics:        metrics,
		log:            log,
		sessions:       make(map[ClientID]*Session),
		deliverer:      deliverer,
		idGen:          idGen,
		store:          newStore(),
		userProperties: props,
	}

	sm.pubSub = newPubSub(&sm, idGen, metrics, log)
	return &sm
}

func (sm *sessionManager) start() {
	sm.log.Trace().Msg("MQTT Starting session manager")
	sm.pubSub.start()
}

func (sm *sessionManager) stop() {
	sm.log.Trace().Msg("MQTT Stopping session manager")
	sm.pubSub.stop()
	sm.log.Debug().Msg("MQTT Session manager stopped with success")
}

func (sm *sessionManager) handlePacket(id ClientID,
	pkt packet.Packet) (*Session, []packet.Packet, error) {

	var session *Session
	if pkt.Type() != packet.CONNECT {
		if len(id) == 0 {
			return nil, nil, fmt.Errorf("received %v before CONNECT",
				pkt.Type())
		}

		var err error
		session, err = sm.readSession(id)
		if err != nil {
			return nil, nil, err
		}
	}

	switch pkt.Type() {
	case packet.CONNECT:
		connPkt, _ := pkt.(*packet.Connect)
		return sm.handleConnect(connPkt)
	case packet.PINGREQ:
		return sm.handlePingReq(session)
	case packet.SUBSCRIBE:
		subPkt, _ := pkt.(*packet.Subscribe)
		return sm.handleSubscribe(session, subPkt)
	case packet.UNSUBSCRIBE:
		unsubPkt, _ := pkt.(*packet.Unsubscribe)
		return sm.handleUnsubscribe(session, unsubPkt)
	case packet.PUBLISH:
		pubPkt, _ := pkt.(*packet.Publish)
		return sm.handlePublish(session, pubPkt)
	case packet.PUBACK:
		pubAckPkt, _ := pkt.(*packet.PubAck)
		return sm.handlePubAck(session, pubAckPkt)
	case packet.DISCONNECT:
		disconnect := pkt.(*packet.Disconnect)
		return sm.handleDisconnect(session, disconnect)
	default:
		return nil, nil, fmt.Errorf("invalid packet type: %v",
			pkt.Type().String())
	}
}

func (sm *sessionManager) handleConnect(pkt *packet.Connect) (*Session,
	[]packet.Packet, error) {

	sm.log.Trace().
		Bool("CleanSession", pkt.CleanSession).
		Bytes("ClientId", pkt.ClientID).
		Uint16("KeepAlive", pkt.KeepAlive).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Received CONNECT packet")

	if pktErr := sm.checkPacketConnect(pkt); pktErr != nil {
		connAck := newConnAck(pkt.Version, pktErr.ReasonCode, 0,
			false, sm.conf, nil, nil)
		return nil, []packet.Packet{&connAck}, pktErr
	}

	var id ClientID
	var idCreated bool
	if len(pkt.ClientID) > 0 {
		id = ClientID(pkt.ClientID)
	} else {
		id = createClientID(sm.conf.ClientIDPrefix)
		idCreated = true
	}

	exp := sessionExpiryIntervalOnConnect(pkt, sm.conf.MaxSessionExpiryInterval)

	session, err := sm.readSession(id)
	if err == nil && pkt.CleanSession {
		sm.cleanSession(session)
	}
	if session == nil {
		session = sm.newSession(id)
		session.KeepAlive = sm.conf.ConnectTimeout
	}

	session.Version = pkt.Version
	session.CleanSession = pkt.CleanSession
	session.ExpiryInterval = exp
	session.ConnectedAt = time.Now().UnixMilli()
	session.connected = true
	session.KeepAlive = sessionKeepAlive(sm.conf, int(pkt.KeepAlive))

	sm.log.Info().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Client connected")

	code := packet.ReasonCodeV3ConnectionAccepted
	connAck := newConnAck(session.Version, code, int(pkt.KeepAlive),
		session.restored, sm.conf, pkt.Properties, sm.userProperties)

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

	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			sm.log.Trace().
				Str("ClientId", string(session.ClientID)).
				Uint64("SessionId", uint64(session.SessionID)).
				Bool("SessionPresent", connAck.SessionPresent).
				Uint8("Version", uint8(connAck.Version)).
				Msg("MQTT Sending CONNACK packet")
		} else if reply.Type() == packet.PUBLISH {
			pub := reply.(*packet.Publish)
			sm.log.Trace().
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

	sm.saveSession(session)
	sm.mutex.Lock()
	sm.sessions[session.ClientID] = session
	sm.mutex.Unlock()

	return session, replies, nil
}

func (sm *sessionManager) handlePingReq(session *Session) (*Session,
	[]packet.Packet, error) {

	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received PINGREQ packet")

	reply := packet.NewPingResp()
	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Sending PINGRESP packet")

	return session, []packet.Packet{&reply}, nil
}

func (sm *sessionManager) handleSubscribe(session *Session,
	pkt *packet.Subscribe) (*Session, []packet.Packet, error) {

	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Int("Topics", len(pkt.Topics)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Received SUBSCRIBE packet")

	replies := make([]packet.Packet, 0, 1)

	subscriptionID := pkt.Properties.SubscriptionID()
	if subscriptionID > 0 && !sm.conf.SubscriptionIDAvailable {
		sm.log.Info().
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
		subscription, err := sm.pubSub.subscribe(session, topic, subscriptionID)
		if err != nil {
			codes = append(codes, packet.ReasonCodeV3Failure)
			continue
		}

		codes = append(codes, packet.ReasonCode(subscription.QoS))
		session.Subscriptions[subscription.TopicFilter] = subscription

		sm.log.Info().
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

	sm.saveSession(session)
	subAck := packet.NewSubAck(pkt.PacketID, pkt.Version, codes, nil)
	replies = append(replies, &subAck)
	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(subAck.PacketID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(subAck.Version)).
		Msg("MQTT Sending SUBACK packet")

	return session, replies, nil
}

func (sm *sessionManager) handleUnsubscribe(session *Session,
	pkt *packet.Unsubscribe) (*Session, []packet.Packet, error) {

	sm.log.Trace().
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
		err := sm.pubSub.unsubscribe(session.ClientID, topic)
		if err == nil {
			code = packet.ReasonCodeV5Success
			delete(session.Subscriptions, topic)
			sm.log.Info().
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

	sm.saveSession(session)
	replies := make([]packet.Packet, 0, 1)

	unsubAck := packet.NewUnsubAck(pkt.PacketID, pkt.Version, codes, nil)
	replies = append(replies, &unsubAck)
	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(unsubAck.PacketID)).
		Int("ReasonCodes", len(unsubAck.ReasonCodes)).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(unsubAck.Version)).
		Msg("MQTT Sending UNSUBACK packet")

	return session, replies, nil
}

func (sm *sessionManager) handlePublish(session *Session,
	pkt *packet.Publish) (*Session, []packet.Packet, error) {

	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint64("SessionId", uint64(session.SessionID)).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received PUBLISH packet")

	msg := sm.pubSub.publish(pkt)
	sm.log.Info().
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
		sm.log.Trace().
			Str("ClientId", string(session.ClientID)).
			Uint16("PacketId", uint16(pubAck.PacketID)).
			Uint8("Version", uint8(pubAck.Version)).
			Msg("MQTT Sending PUBACK packet")
		return session, replies, nil
	}

	return session, nil, nil
}

func (sm *sessionManager) handlePubAck(session *Session,
	pkt *packet.PubAck) (*Session, []packet.Packet, error) {

	sm.log.Trace().
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
	sm.saveSession(session)

	sm.log.Info().
		Str("ClientId", string(session.ClientID)).
		Int("InflightMessages", session.inflightMessages.len()).
		Uint64("MessageId", uint64(inflightMsg.messageID)).
		Uint16("PacketId", uint16(inflightMsg.packet.PacketID)).
		Uint8("QoS", uint8(inflightMsg.packet.QoS)).
		Uint8("Retain", inflightMsg.packet.Retain).
		Str("TopicName", inflightMsg.packet.TopicName).
		Uint8("Version", uint8(inflightMsg.packet.Version)).
		Msg("MQTT Message published to client")

	return session, nil, nil
}

func (sm *sessionManager) handleDisconnect(session *Session,
	pkt *packet.Disconnect) (*Session, []packet.Packet, error) {

	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Received DISCONNECT packet")

	if session.Version == packet.MQTT50 && pkt.Properties != nil {
		interval := pkt.Properties.SessionExpiryInterval

		if interval != nil && *interval > 0 && session.ExpiryInterval == 0 {
			sm.log.Debug().
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

	sm.disconnect(session)
	latency := time.Since(pkt.Timestamp())
	sm.metrics.recordDisconnectLatency(latency)

	sm.log.Info().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Int("InflightMessages", session.inflightMessages.len()).
		Uint32("SessionExpiryInterval", session.ExpiryInterval).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Client disconnected")

	return session, nil, nil
}

func (sm *sessionManager) publishMessage(id ClientID, msg *message) error {
	session, err := sm.readSession(id)
	if err != nil {
		return err
	}

	pkt := msg.packet
	if pkt.QoS > packet.QoS0 {
		pkt = msg.packet.Clone()
		pkt.PacketID = session.nextClientID()
	}
	if pkt.Version != session.Version {
		pkt.Version = session.Version
	}

	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int("InflightMessages", session.inflightMessages.len()).
		Uint64("MessageId", uint64(msg.id)).
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
		sm.saveSession(session)
	}

	if session.connected {
		err = sm.deliverer.deliverPacket(session.ClientID, pkt)
		if err != nil {
			return err
		}

		if inflightMsg != nil {
			inflightMsg.tries++
			inflightMsg.lastSent = time.Now().UnixMicro()
			sm.saveSession(session)
		}

		if pkt.QoS == packet.QoS0 {
			sm.log.Info().
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
			sm.log.Debug().
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
		sm.log.Info().
			Str("ClientId", string(session.ClientID)).
			Int("InflightMessages", session.inflightMessages.len()).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", uint8(pkt.QoS)).
			Uint8("Retain", pkt.Retain).
			Uint64("SessionId", uint64(session.SessionID)).
			Str("TopicName", pkt.TopicName).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Publication postponed, client not connected")
	} else {
		sm.log.Info().
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

func (sm *sessionManager) checkPacketConnect(pkt *packet.Connect) *packet.Error {
	if err := sm.checkKeepAlive(pkt); err != nil {
		return err
	}
	if err := sm.checkClientID(pkt); err != nil {
		return err
	}

	return nil
}

func (sm *sessionManager) checkKeepAlive(pkt *packet.Connect) *packet.Error {
	// For V5.0 clients, the Keep Alive sent in the CONNECT Packet can be
	// overwritten in the CONNACK Packet.
	if pkt.Version == packet.MQTT50 || sm.conf.MaxKeepAlive == 0 {
		return nil
	}

	keepAlive := int(pkt.KeepAlive)
	if keepAlive == 0 || keepAlive > sm.conf.MaxKeepAlive {
		// For MQTT v3.1 and v3.1.1, there is no mechanism to tell the
		// clients what Keep Alive value they should use. If an MQTT
		// v3.1 or v3.1.1 client specifies a Keep Alive time greater than
		// MaxKeepAlive, the CONNACK Packet shall be sent with the reason code
		// "identifier rejected".
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (sm *sessionManager) checkClientID(pkt *packet.Connect) *packet.Error {
	idLen := len(pkt.ClientID)

	if idLen == 0 && !sm.conf.AllowEmptyClientID {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	if pkt.Version == packet.MQTT31 && idLen > 23 {
		return packet.ErrV3IdentifierRejected
	}

	if idLen > sm.conf.MaxClientIDLen {
		if pkt.Version == packet.MQTT50 {
			return packet.ErrV5InvalidClientID
		}
		return packet.ErrV3IdentifierRejected
	}

	return nil
}

func (sm *sessionManager) newSession(id ClientID) *Session {
	nextId := sm.idGen.NextID()
	session := &Session{
		ClientID:      id,
		SessionID:     SessionID(nextId),
		Subscriptions: make(map[string]Subscription),
	}
	sm.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Uint64("SessionId", uint64(session.SessionID)).
		Msg("MQTT New session created")

	return session
}

func (sm *sessionManager) readSession(id ClientID) (*Session, error) {
	sm.log.Trace().
		Str("ClientId", string(id)).
		Msg("MQTT Reading session")

	session, err := sm.store.readSession(id)
	if err != nil {
		if err == errSessionNotFound {
			sm.log.Debug().
				Str("ClientId", string(id)).
				Msg("MQTT Session not found")
			return nil, err
		}

		sm.log.Error().
			Str("ClientId", string(id)).
			Msg("MQTT Failed to read session: " + err.Error())
		return nil, fmt.Errorf("failed to read session: %w", err)
	}

	session.restored = true
	sm.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session read with success")

	return session, nil
}

func (sm *sessionManager) saveSession(session *Session) {
	sm.log.Trace().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Saving session")

	sm.store.saveSession(session)

	sm.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session saved with success")
}

func (sm *sessionManager) deleteSession(session *Session) {
	sm.log.Trace().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Deleting session")

	sm.store.deleteSession(session)

	sm.log.Debug().
		Bool("CleanSession", session.CleanSession).
		Str("ClientId", string(session.ClientID)).
		Bool("Connected", session.connected).
		Int64("ConnectedAt", session.ConnectedAt).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Int("InflightMessages", session.inflightMessages.len()).
		Int("KeepAlive", session.KeepAlive).
		Uint64("SessionId", uint64(session.SessionID)).
		Int("Subscriptions", len(session.Subscriptions)).
		Uint8("Version", uint8(session.Version)).
		Msg("MQTT Session deleted with success")
}

func (sm *sessionManager) cleanSession(session *Session) {
	for _, sub := range session.Subscriptions {
		err := sm.pubSub.unsubscribe(session.ClientID, sub.TopicFilter)
		if err != nil {
			sm.log.Error().
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

func (sm *sessionManager) disconnectSession(id ClientID) {
	session, err := sm.readSession(id)
	if err != nil {
		sm.log.Warn().
			Str("ClientID", string(id)).
			Msg("MQTT Failed to disconnect session: " + err.Error())
		return
	}

	sm.disconnect(session)
}

func (sm *sessionManager) disconnect(session *Session) {
	if !session.connected {
		return
	}

	session.connected = false
	sm.mutex.Lock()
	delete(sm.sessions, session.ClientID)
	sm.mutex.Unlock()

	if session.CleanSession && (session.Version != packet.MQTT50 ||
		session.ExpiryInterval == 0) {
		sm.cleanSession(session)
		sm.deleteSession(session)
	} else {
		sm.saveSession(session)
	}
}
