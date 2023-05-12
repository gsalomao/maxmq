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

type pubRecHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
}

func newPubRecHandler(ss *sessionStore, l *logger.Logger) *pubRecHandler {
	return &pubRecHandler{
		log:          l.WithPrefix("mqtt.pubrec"),
		sessionStore: ss,
	}
}

func (h *pubRecHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubRec := p.(*packet.PubRec)
	pID := pubRec.PacketID
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pID)).
		Uint8("Version", uint8(pubRec.Version)).
		Msg("Received PUBREC packet")

	s, err := h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubRec.Version)).
			Msg("Failed to read session (PUBREC): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	replies = make([]packet.Packet, 0, 1)
	inflightMsg := s.findInflightMessage(pID)
	if inflightMsg == nil {
		h.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint16("PacketId", uint16(pID)).
			Uint8("Version", uint8(s.version)).
			Msg("Received PUBREC with unknown packet ID")

		if s.version != packet.MQTT50 {
			return nil, errPacketNotFound
		}

		pubRel := packet.NewPubRel(pID, s.version, packet.ReasonCodeV5PacketIDNotFound, nil)
		replies = append(replies, &pubRel)
		return replies, nil
	}

	msg := inflightMsg.Value.(*message)
	h.log.Debug().
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packetID)).
		Uint8("Version", uint8(s.version)).
		Msg("Client received the packet (PUBREC)")

	if msg.packet != nil {
		msg.packet = nil
		err = h.sessionStore.saveSession(s)
		if err != nil {
			h.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint64("SessionId", uint64(s.sessionID)).
				Uint8("Version", uint8(s.version)).
				Msg("Failed to save session (PUBREC): " + err.Error())
			return nil, err
		}
	}

	pubRel := packet.NewPubRel(pID, s.version, packet.ReasonCodeV5Success, nil)
	replies = append(replies, &pubRel)

	h.log.Trace().
		Str("ClientId", string(s.clientID)).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(pubRel.PacketID)).
		Int("UnAckMessages", len(s.unAckMessages)).
		Uint8("Version", uint8(pubRel.Version)).
		Msg("Sending PUBREL packet")
	return replies, nil
}
