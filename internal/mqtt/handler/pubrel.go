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

// PubRel is responsible for handling PUBREL packets.
type PubRel struct {
	// Unexported fields
	log             *logger.Logger
	sessionStore    SessionStore
	subscriptionMgr SubscriptionManager
}

// NewPubRel creates a new PubRel.
func NewPubRel(ss SessionStore, sm SubscriptionManager, l *logger.Logger) *PubRel {
	return &PubRel{log: l, sessionStore: ss, subscriptionMgr: sm}
}

// HandlePacket handles the given packet as PUBREL packet.
func (h *PubRel) HandlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubRel := p.(*packet.PubRel)
	pID := pubRel.PacketID
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pID)).
		Uint8("Version", uint8(pubRel.Version)).
		Msg("Received PUBREL packet")

	var s *Session
	s, err = h.sessionStore.ReadSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubRel.Version)).
			Msg("Failed to read session (PUBREL): " + err.Error())
		return nil, err
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	replies = make([]packet.Packet, 0, 1)
	msg, ok := s.UnAckMessages[pID]
	if !ok {
		h.log.Warn().
			Str("ClientId", string(id)).
			Uint16("PacketId", uint16(pID)).
			Int("UnAckMessages", len(s.UnAckMessages)).
			Uint8("Version", uint8(s.Version)).
			Msg("Received PUBREL with unknown packet ID")

		if s.Version != packet.MQTT50 {
			return nil, ErrPacketNotFound
		}

		pubComp := packet.NewPubComp(pID, pubRel.Version, packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubComp)
		return replies, nil
	}

	if msg.Packet != nil {
		msgToPub := &Message{ID: msg.ID, PacketID: msg.PacketID, Packet: msg.Packet}
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
				Msg("Failed to publish message (PUBREL): " + err.Error())
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
			Msg("Client published a packet (PUBREL)")

		msg.LastSent = time.Now().UnixMicro()
		msg.Tries = 1
		msg.Packet = nil

		err = h.sessionStore.SaveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint64("SessionId", uint64(s.SessionID)).
				Uint8("Version", uint8(s.Version)).
				Msg("Failed to save session (PUBREL): " + err.Error())
			return nil, err
		}
	}

	pubComp := packet.NewPubComp(pID, pubRel.Version, packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubComp)
	h.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(pubComp.PacketID)).
		Int("UnAckMessages", len(s.UnAckMessages)).
		Uint8("Version", uint8(pubComp.Version)).
		Msg("Sending PUBCOMP packet")
	return replies, nil
}
