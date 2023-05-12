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

type pingReqHandler struct {
	log          *logger.Logger
	sessionStore *sessionStore
}

func newPingReqHandler(ss *sessionStore, l *logger.Logger) *pingReqHandler {
	return &pingReqHandler{
		log:          l.WithPrefix("mqtt.pingreq"),
		sessionStore: ss,
	}
}

func (h *pingReqHandler) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	pingReq := p.(*packet.PingReq)
	h.log.Trace().
		Str("ClientId", string(id)).
		Uint8("Version", uint8(pingReq.Version)).
		Msg("Received PINGREQ packet")

	var s *session
	s, err = h.sessionStore.readSession(id)
	if err != nil {
		h.log.Error().
			Str("ClientId", string(id)).
			Uint8("Version", uint8(pingReq.Version)).
			Msg("Failed to read session (PINGREQ): " + err.Error())
		return nil, err
	}

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	replies = make([]packet.Packet, 0, 1)
	pingResp := &packet.PingResp{}
	replies = append(replies, pingResp)
	h.log.Trace().
		Str("ClientId", string(s.clientID)).
		Int("KeepAlive", s.keepAlive).
		Uint64("SessionId", uint64(s.sessionID)).
		Int("Subscriptions", len(s.subscriptions)).
		Uint8("Version", uint8(s.version)).
		Msg("Sending PINGRESP packet")
	return replies, nil
}
