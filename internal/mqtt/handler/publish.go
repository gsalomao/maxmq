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

package handler

import (
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// PublishHandler is responsible for handling PUBLISH packets.
type PublishHandler struct {
	// Unexported fields
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
	idGen           MessageIDGenerator
}

// NewPublishHandler creates a new PublishHandler.
func NewPublishHandler(
	st SessionStore,
	subMgr SubscriptionManager,
	gen MessageIDGenerator, l *logger.Logger,
) *PublishHandler {
	return &PublishHandler{log: l, sessionStore: st, subscriptionMgr: subMgr, idGen: gen}
}

// HandlePacket handles the given packet as PUBLISH packet.
func (h *PublishHandler) HandlePacket(
	id packet.ClientID,
	p packet.Packet,
) ([]packet.Packet, error) {
	pubPkt := p.(*packet.Publish)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubPkt.PacketID)).
		Uint8("QoS", uint8(pubPkt.QoS)).
		Str("TopicName", pubPkt.TopicName).
		Uint8("Version", uint8(pubPkt.Version)).
		Msg("MQTT Received PUBLISH packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubPkt.Version)).
			Msg("MQTT Failed to read session (PUBLISH): " + err.Error())
		return nil, err
	}

	msgID := h.idGen.NextID()
	msg := &Message{ID: MessageID(msgID), PacketID: pubPkt.PacketID, Packet: pubPkt}

	s.Mutex.RLock()
	defer s.Mutex.RUnlock()

	if pubPkt.QoS < packet.QoS2 {
		err = h.subscriptionMgr.Publish(msg)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("DUP", pubPkt.Dup).
				Uint64("MessageId", msgID).
				Uint16("PacketId", uint16(pubPkt.PacketID)).
				Uint8("QoS", byte(pubPkt.QoS)).
				Uint8("Retain", pubPkt.Retain).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("TopicName", pubPkt.TopicName).
				Uint8("Version", uint8(pubPkt.Version)).
				Msg("MQTT Failed to publish message (PUBLISH): " + err.Error())
			return nil, err
		}

		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Uint8("DUP", msg.Packet.Dup).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(msg.Packet.PacketID)).
			Uint8("QoS", uint8(msg.Packet.QoS)).
			Uint8("Retain", msg.Packet.Retain).
			Uint64("SessionId", uint64(s.SessionID)).
			Str("TopicName", msg.Packet.TopicName).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Client published a packet (PUBLISH)")

		if pubPkt.QoS == packet.QoS0 {
			return nil, nil
		}
	}

	replies := make([]packet.Packet, 0, 1)
	if pubPkt.QoS == packet.QoS1 {
		pubAck := packet.NewPubAck(
			pubPkt.PacketID,
			pubPkt.Version,
			packet.ReasonCodeV5Success,
			nil, /*props*/
		)
		replies = append(replies, &pubAck)
		h.log.Trace().
			Str("ClientId", string(s.ClientID)).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(pubAck.PacketID)).
			Uint8("Version", uint8(pubAck.Version)).
			Msg("MQTT Sending PUBACK packet")
	} else {
		h.log.Debug().
			Str("ClientId", string(s.ClientID)).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(pubPkt.PacketID)).
			Int("UnAckMessages", len(s.UnAckMessages)).
			Uint8("Version", uint8(pubPkt.Version)).
			Msg("MQTT Received packet from client (PUBLISH)")

		unAckMsg, ok := s.UnAckMessages[pubPkt.PacketID]
		if !ok || unAckMsg.Packet == nil {
			h.log.Trace().
				Str("ClientId", string(s.ClientID)).
				Uint64("MessageId", uint64(msg.ID)).
				Uint16("PacketId", uint16(pubPkt.PacketID)).
				Int("UnAckMessages", len(s.UnAckMessages)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Adding message to UnAck messages (PUBLISH)")

			s.UnAckMessages[pubPkt.PacketID] = msg
			err = h.sessionStore.SaveSession(s)
			if err != nil {
				h.log.Error().
					Str("ClientId", string(s.ClientID)).
					Uint64("SessionId", uint64(s.SessionID)).
					Uint8("Version", uint8(s.Version)).
					Msg("MQTT Failed to save session (PUBLISH): " + err.Error())
				return nil, err
			}
		}

		pubRec := packet.NewPubRec(
			pubPkt.PacketID,
			pubPkt.Version,
			packet.ReasonCodeV5Success,
			nil, /*props*/
		)
		replies = append(replies, &pubRec)
		h.log.Trace().
			Str("ClientId", string(s.ClientID)).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(pubRec.PacketID)).
			Int("UnAckMessages", len(s.UnAckMessages)).
			Uint8("Version", uint8(pubRec.Version)).
			Msg("MQTT Sending PUBREC packet")
	}

	return replies, nil
}
