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

type pubCompHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
}

func newPubCompHandler(ss *sessionStore, l *logger.Logger) *pubCompHandler {
	return &pubCompHandler{
		log:          l.WithPrefix("mqtt.pubcomp"),
		sessionStore: ss,
	}
}

func (h *pubCompHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pubCompPkt := p.(*packet.PubComp)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint16("PacketId", uint16(pubCompPkt.PacketID)).
		Uint8("Version", uint8(pubCompPkt.Version)).
		Msg("Received PUBCOMP packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pubCompPkt.Version)).
			Msg("Failed to read session (PUBCOMP): " + err.Error())
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	inflightMsg := s.findInflightMessage(pubCompPkt.PacketID)
	if inflightMsg == nil {
		h.log.Warn().
			Str("ClientId", string(id)).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint16("PacketId", uint16(pubCompPkt.PacketID)).
			Uint8("Version", uint8(s.version)).
			Msg("Received PUBCOMP with unknown packet ID")
		return nil, errPacketNotFound
	}

	s.inflightMessages.Remove(inflightMsg)
	err = h.sessionStore.saveSession(s)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(s.clientID)).
			Uint64("SessionId", uint64(s.sessionID)).
			Uint8("Version", uint8(s.version)).
			Msg("Failed to save session (PUBCOMP): " + err.Error())
		return nil, err
	}

	msg := inflightMsg.Value.(*message)
	h.log.Info().
		Str("ClientId", string(s.clientID)).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packetID)).
		Uint8("Version", uint8(s.version)).
		Msg("Message published to client (PUBCOMP)")
	return nil, nil
}
