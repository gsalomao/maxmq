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
	"fmt"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

var errSessionNotFound = errors.New("session not found")
var errPacketIDNotFound = errors.New("packet ID not found")

type packetHandler = func(id clientID, pkt packet.Packet) (*session,
	[]packet.Packet, error)

type sessionManager struct {
	conf           *Configuration
	metrics        *metrics
	log            *logger.Logger
	sessions       map[clientID]*session
	deliverer      packetDeliverer
	idGen          IDGenerator
	mutex          sync.RWMutex
	pubSub         pubSub
	store          store
	userProperties []packet.UserProperty
}

func newSessionManager(conf *Configuration, idGen IDGenerator, metrics *metrics,
	props []packet.UserProperty, log *logger.Logger) *sessionManager {

	sm := sessionManager{
		conf:           conf,
		metrics:        metrics,
		log:            log,
		sessions:       make(map[clientID]*session),
		idGen:          idGen,
		store:          newStore(),
		userProperties: props,
	}

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

func (sm *sessionManager) handlePacket(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	if pkt.Type() != packet.CONNECT && len(id) == 0 {
		return nil, nil, fmt.Errorf("received %v before CONNECT",
			pkt.Type())
	}

	handler, err := sm.packetHandler(pkt.Type())
	if err != nil {
		return nil, nil, err
	}

	return handler(id, pkt)
}

func (sm *sessionManager) packetHandler(t packet.Type) (packetHandler, error) {
	switch t {
	case packet.CONNECT:
		return sm.handleConnect, nil
	case packet.PINGREQ:
		return sm.handlePingReq, nil
	case packet.SUBSCRIBE:
		return sm.handleSubscribe, nil
	case packet.UNSUBSCRIBE:
		return sm.handleUnsubscribe, nil
	case packet.PUBLISH:
		return sm.handlePublish, nil
	case packet.PUBACK:
		return sm.handlePubAck, nil
	case packet.PUBREC:
		return sm.handlePubRec, nil
	case packet.PUBREL:
		return sm.handlePubRel, nil
	case packet.PUBCOMP:
		return sm.handlePubComp, nil
	case packet.DISCONNECT:
		return sm.handleDisconnect, nil
	default:
		return nil, errors.New("invalid packet type")
	}
}

func (sm *sessionManager) handleConnect(_ clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	connPkt := pkt.(*packet.Connect)
	sm.log.Trace().
		Bool("CleanSession", connPkt.CleanSession).
		Bytes("ClientId", connPkt.ClientID).
		Uint16("KeepAlive", connPkt.KeepAlive).
		Uint8("Version", uint8(connPkt.Version)).
		Msg("MQTT Received CONNECT packet")

	if pktErr := sm.checkPacketConnect(connPkt); pktErr != nil {
		connAck := newConnAck(connPkt.Version, pktErr.ReasonCode, 0,
			false, sm.conf, nil, nil)
		return nil, []packet.Packet{&connAck}, pktErr
	}

	var id clientID
	var idCreated bool
	if len(connPkt.ClientID) > 0 {
		id = clientID(connPkt.ClientID)
	} else {
		id = createClientID(sm.conf.ClientIDPrefix)
		idCreated = true
	}

	expiryInterval := sessionExpiryIntervalOnConnect(connPkt,
		sm.conf.MaxSessionExpiryInterval)

	s, err := sm.readSession(id)
	if err != nil && err != errSessionNotFound {
		return nil, nil, err
	}
	if s == nil {
		s = sm.newSession(id)
		s.keepAlive = sm.conf.ConnectTimeout
	} else if connPkt.CleanSession {
		sm.cleanSession(s)
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.version = connPkt.Version
	s.cleanSession = connPkt.CleanSession
	s.expiryInterval = expiryInterval
	s.connectedAt = time.Now().UnixMilli()
	s.connected = true
	s.keepAlive = sessionKeepAlive(sm.conf, int(connPkt.KeepAlive))

	sm.log.Info().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Client connected")

	code := packet.ReasonCodeV3ConnectionAccepted
	connAckPkt := newConnAck(s.version, code, int(connPkt.KeepAlive),
		s.restored, sm.conf, connPkt.Properties, sm.userProperties)

	addAssignedClientID(&connAckPkt, s.version, s.clientID, idCreated)

	replies := make([]packet.Packet, 0, 1)
	replies = append(replies, &connAckPkt)
	appendPendingInflightMessages(&replies, s)
	sm.saveSession(s)

	for _, reply := range replies {
		if reply.Type() == packet.CONNACK {
			sm.log.Trace().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Bool("SessionPresent", connAckPkt.SessionPresent).
				Uint8("Version", uint8(connAckPkt.Version)).
				Msg("MQTT Sending CONNACK packet")
		} else if reply.Type() == packet.PUBLISH {
			pub := reply.(*packet.Publish)
			if pub.Version != s.version {
				pub.Version = s.version
			}

			sm.log.Trace().
				Str("ClientId", string(s.clientID)).
				Int("InflightMessages", s.inflightMessages.Len()).
				Uint16("PacketId", uint16(pub.PacketID)).
				Uint8("QoS", uint8(pub.QoS)).
				Uint8("Retain", pub.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", pub.TopicName).
				Int("UnAckPubMessages", len(s.unAckPubMessages)).
				Uint8("Version", uint8(pub.Version)).
				Msg("MQTT Sending PUBLISH packet")
		}
	}

	return s, replies, nil
}

func (sm *sessionManager) handlePingReq(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pingReqPkt := pkt.(*packet.PingReq)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(pingReqPkt.Version)).
		Msg("MQTT Received PINGREQ packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	pingRespPkt := packet.PingResp{}
	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Sending PINGRESP packet")

	return s, []packet.Packet{&pingRespPkt}, nil
}

func (sm *sessionManager) handleSubscribe(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	subPkt := pkt.(*packet.Subscribe)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(subPkt.PacketID)).
		Int("Topics", len(subPkt.Topics)).
		Uint8("Version", uint8(subPkt.Version)).
		Msg("MQTT Received SUBSCRIBE packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	replies := make([]packet.Packet, 0, 1)

	subscriptionID := subPkt.Properties.SubscriptionID()
	if subscriptionID > 0 && !sm.conf.SubscriptionIDAvailable {
		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint16("PacketId", uint16(subPkt.PacketID)).
			Uint64("SessionId", uint64(s.sessionID)).
			Uint8("Version", uint8(subPkt.Version)).
			Msg("MQTT Received SUBSCRIBE with subscription ID (not available)")

		discPkt := packet.NewDisconnect(s.version,
			packet.ReasonCodeV5SubscriptionIDNotSupported, nil)
		replies = append(replies, &discPkt)
		return s, replies, packet.ErrV5SubscriptionIDNotSupported
	}

	codes := make([]packet.ReasonCode, 0, len(subPkt.Topics))

	for _, topic := range subPkt.Topics {
		var sub subscription

		sub, err = sm.pubSub.subscribe(s, topic, subscriptionID)
		if err != nil {
			codes = append(codes, packet.ReasonCodeV3Failure)
			continue
		}

		codes = append(codes, packet.ReasonCode(sub.qos))
		s.subscriptions[sub.topicFilter] = sub

		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint16("PacketId", uint16(subPkt.PacketID)).
			Uint8("QoS", byte(topic.QoS)).
			Uint64("SessionId", uint64(s.sessionID)).
			Int("Subscriptions", len(s.subscriptions)).
			Str("TopicFilter", topic.Name).
			Int("Topics", len(subPkt.Topics)).
			Uint8("Version", uint8(subPkt.Version)).
			Msg("MQTT Client subscribed to topic")
	}

	sm.saveSession(s)
	subAckPkt := packet.NewSubAck(subPkt.PacketID, subPkt.Version, codes, nil)
	replies = append(replies, &subAckPkt)
	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint16("PacketId", uint16(subAckPkt.PacketID)).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(subAckPkt.Version)).
		Msg("MQTT Sending SUBACK packet")

	return s, replies, nil
}

func (sm *sessionManager) handleUnsubscribe(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	unsubPkt := pkt.(*packet.Unsubscribe)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(unsubPkt.PacketID)).
		Int("Topics", len(unsubPkt.Topics)).
		Uint8("Version", uint8(unsubPkt.Version)).
		Msg("MQTT Received UNSUBSCRIBE packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	var codes []packet.ReasonCode
	if unsubPkt.Version == packet.MQTT50 {
		codes = make([]packet.ReasonCode, 0, len(unsubPkt.Topics))
	}

	for _, topic := range unsubPkt.Topics {
		code := packet.ReasonCodeV5UnspecifiedError
		err = sm.pubSub.unsubscribe(s.clientID, topic)
		if err == nil {
			code = packet.ReasonCodeV5Success
			delete(s.subscriptions, topic)
			sm.log.Info().
				Str("ClientId", string(s.clientID)).
				Uint16("PacketId", uint16(unsubPkt.PacketID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Int("Subscriptions", len(s.subscriptions)).
				Str("TopicFilter", topic).
				Int("Topics", len(unsubPkt.Topics)).
				Uint8("Version", uint8(unsubPkt.Version)).
				Msg("MQTT Client unsubscribed to topic")
		} else if err == errSubscriptionNotFound {
			code = packet.ReasonCodeV5NoSubscriptionExisted
		}

		if unsubPkt.Version == packet.MQTT50 {
			codes = append(codes, code)
		}
	}

	sm.saveSession(s)
	replies := make([]packet.Packet, 0, 1)

	unsubAckPkt := packet.NewUnsubAck(unsubPkt.PacketID, unsubPkt.Version,
		codes, nil)
	replies = append(replies, &unsubAckPkt)
	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint16("PacketId", uint16(unsubAckPkt.PacketID)).
		Int("ReasonCodes", len(unsubAckPkt.ReasonCodes)).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(unsubAckPkt.Version)).
		Msg("MQTT Sending UNSUBACK packet")

	return s, replies, nil
}

func (sm *sessionManager) handlePublish(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pubPkt := pkt.(*packet.Publish)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubPkt.PacketID)).
		Uint8("QoS", uint8(pubPkt.QoS)).
		Str("TopicName", pubPkt.TopicName).
		Uint8("Version", uint8(pubPkt.Version)).
		Msg("MQTT Received PUBLISH packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	msgID := sm.idGen.NextID()
	msg := &message{id: messageID(msgID), packetID: pubPkt.PacketID,
		packet: pubPkt}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if pubPkt.QoS < packet.QoS2 {
		sm.pubSub.publish(msg)
		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint8("DUP", msg.packet.Dup).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packet.PacketID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(s.version)).
			Msg("MQTT Client published a packet")

		if pubPkt.QoS == packet.QoS0 {
			return s, nil, nil
		}
	}

	replies := make([]packet.Packet, 0, 1)
	if pubPkt.QoS == packet.QoS1 {
		pubAckPkt := packet.NewPubAck(pubPkt.PacketID, pubPkt.Version,
			packet.ReasonCodeV5Success, nil)

		replies = append(replies, &pubAckPkt)
		sm.log.Trace().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pubAckPkt.PacketID)).
			Uint8("Version", uint8(pubAckPkt.Version)).
			Msg("MQTT Sending PUBACK packet")
	} else {
		sm.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pubPkt.PacketID)).
			Int("UnAckPubMessages", len(s.unAckPubMessages)).
			Uint8("Version", uint8(pubPkt.Version)).
			Msg("MQTT Received packet from client")

		unAckMsg, ok := s.unAckPubMessages[pubPkt.PacketID]
		if !ok || unAckMsg.packet == nil {
			sm.log.Trace().
				Str("ClientId", string(s.clientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pubPkt.PacketID)).
				Int("UnAckPubMessages", len(s.unAckPubMessages)).
				Uint8("Version", uint8(pubPkt.Version)).
				Msg("MQTT Adding message to unacknowledged messages")

			s.unAckPubMessages[pubPkt.PacketID] = msg
			sm.saveSession(s)
		}

		pubRecPkt := packet.NewPubRec(pubPkt.PacketID, pubPkt.Version,
			packet.ReasonCodeV5Success, nil)

		replies = append(replies, &pubRecPkt)
		sm.log.Trace().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pubRecPkt.PacketID)).
			Int("UnAckPubMessages", len(s.unAckPubMessages)).
			Uint8("Version", uint8(pubRecPkt.Version)).
			Msg("MQTT Sending PUBREC packet")
	}

	return s, replies, nil
}

func (sm *sessionManager) handlePubAck(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pubAckPkt := pkt.(*packet.PubAck)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubAckPkt.PacketID)).
		Uint8("Version", uint8(pubAckPkt.Version)).
		Msg("MQTT Received PUBACK packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	inflightMsg := s.findInflightMessage(pubAckPkt.PacketID)
	if inflightMsg == nil {
		sm.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint16("PacketId", uint16(pubAckPkt.PacketID)).
			Uint8("Version", uint8(s.version)).
			Msg("MQTT Received PUBACK with unknown packet ID")

		return s, nil, errPacketIDNotFound
	}
	s.inflightMessages.Remove(inflightMsg)
	sm.saveSession(s)

	msg := inflightMsg.Value.(*message)
	sm.log.Info().
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packet.PacketID)).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Str("TopicName", msg.packet.TopicName).
		Uint8("Version", uint8(msg.packet.Version)).
		Msg("MQTT Message published to client")

	return s, nil, nil
}

func (sm *sessionManager) handlePubRec(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pubRecPkt := pkt.(*packet.PubRec)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubRecPkt.PacketID)).
		Uint8("Version", uint8(pubRecPkt.Version)).
		Msg("MQTT Received PUBREC packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	replies := make([]packet.Packet, 0, 1)
	inflightMsg := s.findInflightMessage(pubRecPkt.PacketID)
	if inflightMsg == nil {
		sm.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint16("PacketId", uint16(pubRecPkt.PacketID)).
			Uint8("Version", uint8(s.version)).
			Msg("MQTT Received PUBREC with unknown packet ID")

		if s.version != packet.MQTT50 {
			return s, nil, errPacketIDNotFound
		}

		pubRelPkt := packet.NewPubRel(pubRecPkt.PacketID, s.version,
			packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubRelPkt)

		return s, replies, nil
	}

	msg := inflightMsg.Value.(*message)
	sm.log.Debug().
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packetID)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Client received the packet")

	msg.packet = nil
	sm.saveSession(s)

	pubRelPkt := packet.NewPubRel(pubRecPkt.PacketID, s.version,
		packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubRelPkt)

	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(pubRelPkt.PacketID)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(pubRelPkt.Version)).
		Msg("MQTT Sending PUBREL packet")

	return s, replies, nil
}

func (sm *sessionManager) handlePubRel(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pubRelPkt := pkt.(*packet.PubRel)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubRelPkt.PacketID)).
		Uint8("Version", uint8(pubRelPkt.Version)).
		Msg("MQTT Received PUBREL packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	replies := make([]packet.Packet, 0, 1)
	msg, ok := s.unAckPubMessages[pubRelPkt.PacketID]
	if !ok {
		sm.log.Warn().
			Str("ClientId", string(id)).
			Uint16("PacketId", uint16(pubRelPkt.PacketID)).
			Int("UnAckPubMessages", len(s.unAckPubMessages)).
			Uint8("Version", uint8(s.version)).
			Msg("MQTT Received PUBREL with unknown packet ID")

		if s.version != packet.MQTT50 {
			return s, nil, errPacketIDNotFound
		}

		pubCompPkt := packet.NewPubComp(pubRelPkt.PacketID, pubRelPkt.Version,
			packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubCompPkt)

		return s, replies, nil
	}

	if msg.packet != nil {
		msgToPub := &message{id: msg.id, packetID: msg.packetID,
			packet: msg.packet}

		sm.pubSub.publish(msgToPub)
		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msgToPub.id)).
			Uint16("PacketId", uint16(msgToPub.packet.PacketID)).
			Uint8("QoS", uint8(msgToPub.packet.QoS)).
			Uint8("Retain", msgToPub.packet.Retain).
			Str("TopicName", msgToPub.packet.TopicName).
			Int("UnAckPubMessages", len(s.unAckPubMessages)).
			Uint8("Version", uint8(msgToPub.packet.Version)).
			Msg("MQTT Client published a packet")

		msg.lastSent = time.Now().UnixMicro()
		msg.tries = 1
		msg.packet = nil
		sm.saveSession(s)
	}

	pubCompPkt := packet.NewPubComp(pubRelPkt.PacketID, pubRelPkt.Version,
		packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubCompPkt)

	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(pubCompPkt.PacketID)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("MQTT Sending PUBCOMP packet")

	return s, replies, nil
}

func (sm *sessionManager) handlePubComp(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	pubCompPkt := pkt.(*packet.PubComp)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubCompPkt.PacketID)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("MQTT Received PUBCOMP packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	inflightMsg := s.findInflightMessage(pubCompPkt.PacketID)
	if inflightMsg == nil {
		sm.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint16("PacketId", uint16(pubCompPkt.PacketID)).
			Uint8("Version", uint8(s.version)).
			Msg("MQTT Received PUBCOMP with unknown packet ID")
		return s, nil, errPacketIDNotFound
	}

	s.inflightMessages.Remove(inflightMsg)
	sm.saveSession(s)

	msg := inflightMsg.Value.(*message)
	sm.log.Info().
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packetID)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("MQTT Message published to client")

	return s, nil, nil
}

func (sm *sessionManager) handleDisconnect(id clientID,
	pkt packet.Packet) (*session, []packet.Packet, error) {

	discPkt := pkt.(*packet.Disconnect)
	sm.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(discPkt.Version)).
		Msg("MQTT Received DISCONNECT packet")

	s, err := sm.readSession(id)
	if err != nil {
		return nil, nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.version == packet.MQTT50 && discPkt.Properties != nil {
		interval := discPkt.Properties.SessionExpiryInterval

		if interval != nil && *interval > 0 && s.expiryInterval == 0 {
			sm.log.Debug().
				Str("ClientId", string(s.clientID)).
				Uint8("Version", uint8(s.version)).
				Uint32("SessionExpiryInterval", *interval).
				Uint64("SessionId", uint64(s.sessionID)).
				Msg("MQTT DISCONNECT with invalid Session Expiry Interval")

			replies := make([]packet.Packet, 0)
			discReplyPkt := packet.NewDisconnect(s.version,
				packet.ReasonCodeV5ProtocolError, nil)
			replies = append(replies, &discReplyPkt)
			return s, replies, packet.ErrV5ProtocolError
		}

		if interval != nil && *interval != s.expiryInterval {
			s.expiryInterval = *interval
		}
	}

	sm.disconnect(s)
	latency := time.Since(pkt.Timestamp())
	sm.metrics.recordDisconnectLatency(latency)

	sm.log.Info().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint32("SessionExpiryInterval", s.expiryInterval).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Client disconnected")

	return s, nil, nil
}

func (sm *sessionManager) publishMessage(id clientID, msg *message) error {
	s, err := sm.readSession(id)
	if err != nil {
		return err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	pkt := msg.packet
	if pkt.QoS > packet.QoS0 {
		pkt.PacketID = s.nextClientID()
		msg.packetID = pkt.PacketID
	}

	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(pkt.PacketID)).
		Uint8("QoS", uint8(pkt.QoS)).
		Uint8("Retain", pkt.Retain).
		Uint64("SessionId", uint64(s.sessionID)).
		Str("TopicName", pkt.TopicName).
		Uint8("Version", uint8(pkt.Version)).
		Msg("MQTT Publishing message to client")

	if pkt.QoS > packet.QoS0 {
		s.inflightMessages.PushBack(msg)
		sm.saveSession(s)
	}

	if s.connected {
		if pkt.Version != s.version {
			pkt.Version = s.version
		}

		err = sm.deliverer.deliverPacket(s.clientID, pkt)
		if err != nil {
			return err
		}

		if pkt.QoS > packet.QoS0 {
			msg.tries++
			msg.lastSent = time.Now().UnixMicro()
			sm.saveSession(s)

			sm.log.Debug().
				Str("ClientId", string(s.clientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pkt.PacketID)).
				Uint8("QoS", uint8(pkt.QoS)).
				Uint8("Retain", pkt.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", pkt.TopicName).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Message delivered to client")
		} else {
			sm.log.Info().
				Str("ClientId", string(s.clientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pkt.PacketID)).
				Uint8("QoS", uint8(pkt.QoS)).
				Uint8("Retain", pkt.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", pkt.TopicName).
				Uint8("Version", uint8(pkt.Version)).
				Msg("MQTT Message published to client")
		}
	} else if pkt.QoS > packet.QoS0 {
		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", uint8(pkt.QoS)).
			Uint8("Retain", pkt.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", pkt.TopicName).
			Uint8("Version", uint8(pkt.Version)).
			Msg("MQTT Publication postponed, client not connected")
	} else {
		sm.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pkt.PacketID)).
			Uint8("QoS", uint8(pkt.QoS)).
			Uint8("Retain", pkt.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
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

func (sm *sessionManager) newSession(id clientID) *session {
	nextId := sm.idGen.NextID()
	s := &session{
		clientID:         id,
		sessionID:        sessionID(nextId),
		subscriptions:    make(map[string]subscription),
		unAckPubMessages: make(map[packet.ID]*message),
	}

	sm.mutex.Lock()
	sm.sessions[s.clientID] = s
	sm.mutex.Unlock()

	sm.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("SessionId", uint64(s.sessionID)).
		Msg("MQTT New session created")

	return s
}

func (sm *sessionManager) readSession(id clientID) (*session, error) {
	sm.log.Trace().
		Str("ClientId", string(id)).
		Msg("MQTT Reading session")

	s, err := sm.store.readSession(id)
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

	s.restored = true
	sm.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Session read with success")

	return s, nil
}

func (sm *sessionManager) saveSession(s *session) {
	sm.log.Trace().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Saving session")

	sm.store.saveSession(s)

	sm.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Session saved with success")
}

func (sm *sessionManager) deleteSession(s *session) {
	sm.log.Trace().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Deleting session")

	sm.store.deleteSession(s)

	sm.log.Debug().
		Bool("CleanSession", s.cleanSession).
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int64("ConnectedAt", s.connectedAt).
		Uint32("ExpiryInterval", s.expiryInterval).
		Int("InflightMessages", s.inflightMessages.Len()).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Int("UnAckPubMessages", len(s.unAckPubMessages)).
		Uint8("Version", uint8(s.version)).
		Msg("MQTT Session deleted with success")
}

func (sm *sessionManager) cleanSession(s *session) {
	for _, sub := range s.subscriptions {
		err := sm.pubSub.unsubscribe(s.clientID, sub.topicFilter)
		if err != nil {
			sm.log.Error().
				Str("ClientId", string(s.clientID)).
				Bool("Connected", s.connected).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("Topic", sub.topicFilter).
				Uint8("Version", uint8(s.version)).
				Msg("MQTT Failed to unsubscribe when cleaning session")
		}
	}
	s.clean()
}

func (sm *sessionManager) disconnectSession(id clientID) {
	s, err := sm.readSession(id)
	if err != nil {
		sm.log.Warn().
			Str("ClientId", string(id)).
			Msg("MQTT Failed to disconnect session: " + err.Error())
		return
	}

	s.mutex.Lock()
	sm.disconnect(s)
	s.mutex.Unlock()
}

func (sm *sessionManager) disconnect(s *session) {
	if !s.connected {
		return
	}

	s.connected = false
	sm.mutex.Lock()
	delete(sm.sessions, s.clientID)
	sm.mutex.Unlock()

	if s.cleanSession && (s.version != packet.MQTT50 ||
		s.expiryInterval == 0) {
		sm.cleanSession(s)
		sm.deleteSession(s)
	} else {
		sm.saveSession(s)
	}
}
