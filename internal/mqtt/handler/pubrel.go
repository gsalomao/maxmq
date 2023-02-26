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
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// PubRelHandler is responsible for handling PUBREL packets.
type PubRelHandler struct {
	// Unexported fields
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewPubRelHandler creates a new PubRelHandler.
func NewPubRelHandler(st SessionStore, subMgr SubscriptionManager,
	l *logger.Logger) *PubRelHandler {

	return &PubRelHandler{
		log:             l,
		sessionStore:    st,
		subscriptionMgr: subMgr,
	}
}

// HandlePacket handles the given packet as a PUBREL packet.
func (h *PubRelHandler) HandlePacket(id packet.ClientID,
	pkt packet.Packet) ([]packet.Packet, error) {

	pubRelPkt := pkt.(*packet.PubRel)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubRelPkt.PacketID)).
		Uint8("Version", uint8(pubRelPkt.Version)).
		Msg("MQTT Received PUBREL packet")

	s, err := h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubRelPkt.Version)).
			Msg("MQTT Failed to read session (PUBREL): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	replies := make([]packet.Packet, 0, 1)
	msg, ok := s.UnAckMessages[pubRelPkt.PacketID]
	if !ok {
		h.log.Warn().
			Str("ClientId", string(id)).
			Uint16("PacketId", uint16(pubRelPkt.PacketID)).
			Int("UnAckMessages", len(s.UnAckMessages)).
			Uint8("Version", uint8(s.Version)).
			Msg("MQTT Received PUBREL with unknown packet ID")

		if s.Version != packet.MQTT50 {
			return nil, ErrPacketNotFound
		}

		pubCompPkt := packet.NewPubComp(pubRelPkt.PacketID, pubRelPkt.Version,
			packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubCompPkt)

		return replies, nil
	}

	if msg.Packet != nil {
		msgToPub := &Message{ID: msg.ID, PacketID: msg.PacketID,
			Packet: msg.Packet}

		err = h.subscriptionMgr.Publish(msgToPub)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("DUP", msgToPub.Packet.Dup).
				Uint64("MessageId", uint64(msg.ID)).
				Uint16("PacketId", uint16(msg.PacketID)).
				Uint8("QoS", byte(msg.Packet.QoS)).
				Uint8("Retain", msg.Packet.Retain).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("TopicName", msg.Packet.TopicName).
				Int("UnAckMessages", len(s.UnAckMessages)).
				Uint8("Version", uint8(msg.Packet.Version)).
				Msg("MQTT Failed to publish message (PUBREL): " + err.Error())
			return nil, err
		}

		h.log.Info().
			Str("ClientId", string(s.ClientID)).
			Uint8("DUP", msgToPub.Packet.Dup).
			Uint64("MessageId", uint64(msgToPub.ID)).
			Uint16("PacketId", uint16(msgToPub.Packet.PacketID)).
			Uint8("QoS", uint8(msgToPub.Packet.QoS)).
			Uint8("Retain", msgToPub.Packet.Retain).
			Uint64("SessionId", uint64(s.SessionID)).
			Str("TopicName", msgToPub.Packet.TopicName).
			Int("UnAckMessages", len(s.UnAckMessages)).
			Uint8("Version", uint8(msgToPub.Packet.Version)).
			Msg("MQTT Client published a packet (PUBREL)")

		msg.LastSent = time.Now().UnixMicro()
		msg.Tries = 1
		msg.Packet = nil

		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("MQTT Failed to save session (PUBREL): " + err.Error())
			return nil, err
		}
	}

	pubCompPkt := packet.NewPubComp(pubRelPkt.PacketID, pubRelPkt.Version,
		packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubCompPkt)

	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(pubCompPkt.PacketID)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("MQTT Sending PUBCOMP packet")

	return replies, nil
}
