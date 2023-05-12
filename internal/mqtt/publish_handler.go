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
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type publishHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
	pubSub       *pubSub
	idGen        IDGenerator
}

func newPublishHandler(ss *sessionStore, ps *pubSub, g IDGenerator, l *logger.Logger) *publishHandler {
	return &publishHandler{
		log:          l.WithPrefix("mqtt.publish"),
		sessionStore: ss,
		pubSub:       ps,
		idGen:        g,
	}
}

func (h *publishHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubPkt := p.(*packet.Publish)
	pID := pubPkt.PacketID
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pID)).
		Uint8("QoS", uint8(pubPkt.QoS)).
		Str("TopicName", pubPkt.TopicName).
		Uint8("Version", uint8(pubPkt.Version)).
		Msg("Received PUBLISH packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubPkt.Version)).
			Msg("Failed to read session (PUBLISH): " + err.Error())
		return nil, err
	}

	mID := h.idGen.NextID()
	msg := &message{id: messageID(mID), packetID: pID, packet: pubPkt}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if pubPkt.QoS < packet.QoS2 {
		err = h.pubSub.publish(msg)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(id)).
				Uint8("DUP", pubPkt.Dup).
				Uint64("MessageId", mID).
				Uint16("PacketId", uint16(pID)).
				Uint8("QoS", byte(pubPkt.QoS)).
				Uint8("Retain", pubPkt.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", pubPkt.TopicName).
				Uint8("Version", uint8(pubPkt.Version)).
				Msg("Failed to publish message (PUBLISH): " + err.Error())
			return nil, err
		}

		h.log.Info().
			Str("ClientId", string(s.clientID)).
			Uint8("DUP", msg.packet.Dup).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packet.PacketID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(s.version)).
			Msg("Client published a packet (PUBLISH)")

		if pubPkt.QoS == packet.QoS0 {
			return nil, nil
		}
	}

	replies = make([]packet.Packet, 0, 1)
	if pubPkt.QoS == packet.QoS1 {
		pubAck := packet.NewPubAck(pID, pubPkt.Version, packet.ReasonCodeV5Success, nil)
		replies = append(replies, &pubAck)
		h.log.Trace().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pubAck.PacketID)).
			Uint8("Version", uint8(pubAck.Version)).
			Msg("Sending PUBACK packet")
	} else {
		h.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pID)).
			Int("UnAckMessages", len(s.unAckMessages)).
			Uint8("Version", uint8(pubPkt.Version)).
			Msg("Received packet from client (PUBLISH)")

		unAckMsg, ok := s.unAckMessages[pID]
		if !ok || unAckMsg.packet == nil {
			h.log.Trace().
				Str("ClientId", string(s.clientID)).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(pID)).
				Int("UnAckMessages", len(s.unAckMessages)).
				Uint8("Version", uint8(s.version)).
				Msg("Adding message to UnAck messages (PUBLISH)")

			s.unAckMessages[pID] = msg
			err = h.sessionStore.saveSession(s)
			if err != nil {
				h.log.Error().
					Str("ClientId", string(s.clientID)).
					Uint64("SessionId", uint64(s.sessionID)).
					Uint8("Version", uint8(s.version)).
					Msg("Failed to save session (PUBLISH): " + err.Error())
				return nil, err
			}
		}

		pubRec := packet.NewPubRec(pID, pubPkt.Version, packet.ReasonCodeV5Success, nil)
		replies = append(replies, &pubRec)
		h.log.Trace().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(pubRec.PacketID)).
			Int("UnAckMessages", len(s.unAckMessages)).
			Uint8("Version", uint8(pubRec.Version)).
			Msg("Sending PUBREC packet")
	}

	return replies, nil
}
