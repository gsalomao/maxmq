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
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type pubRelHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
	pubSub       *pubSub
}

func newPubRelHandler(ss *sessionStore, ps *pubSub, l *logger.Logger) *pubRelHandler {
	return &pubRelHandler{
		log:          l.WithPrefix("mqtt.pubrel"),
		sessionStore: ss,
		pubSub:       ps,
	}
}

func (h *pubRelHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubRel := p.(*packet.PubRel)
	pID := pubRel.PacketID
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pID)).
		Uint8("Version", uint8(pubRel.Version)).
		Msg("Received PUBREL packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubRel.Version)).
			Msg("Failed to read session (PUBREL): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	replies = make([]packet.Packet, 0, 1)
	msg, ok := s.unAckMessages[pID]
	if !ok {
		h.log.Warn().
			Str("ClientId", string(id)).
			Uint16("PacketId", uint16(pID)).
			Int("UnAckMessages", len(s.unAckMessages)).
			Uint8("Version", uint8(s.version)).
			Msg("Received PUBREL with unknown packet ID")

		if s.version != packet.MQTT50 {
			return nil, errPacketNotFound
		}

		pubComp := packet.NewPubComp(pID, pubRel.Version, packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubComp)
		return replies, nil
	}

	if msg.packet != nil {
		msgToPub := &message{id: msg.id, packetID: msg.packetID, packet: msg.packet}
		err = h.pubSub.publish(msgToPub)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("DUP", msgToPub.packet.Dup).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(msg.packetID)).
				Uint8("QoS", byte(msg.packet.QoS)).
				Uint8("Retain", msg.packet.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", msg.packet.TopicName).
				Int("UnAckMessages", len(s.unAckMessages)).
				Uint8("Version", uint8(msg.packet.Version)).
				Msg("Failed to publish message (PUBREL): " + err.Error())
			return nil, err
		}

		h.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint8("DUP", msgToPub.packet.Dup).
			Uint64("MessageId", uint64(msgToPub.id)).
			Uint16("PacketId", uint16(msgToPub.packet.PacketID)).
			Uint8("QoS", uint8(msgToPub.packet.QoS)).
			Uint8("Retain", msgToPub.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msgToPub.packet.TopicName).
			Int("UnAckMessages", len(s.unAckMessages)).
			Uint8("Version", uint8(msgToPub.packet.Version)).
			Msg("Client published a packet (PUBREL)")

		msg.lastSent = time.Now().UnixMicro()
		msg.tries = 1
		msg.packet = nil

		err = h.sessionStore.saveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (PUBREL): " + err.Error())
			return nil, err
		}
	}

	pubComp := packet.NewPubComp(pID, pubRel.Version, packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubComp)
	h.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(pubComp.PacketID)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(pubComp.Version)).
		Msg("Sending PUBCOMP packet")
	return replies, nil
}
