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
	"sync/atomic"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// ClientID represents the MQTT Client ID.
type ClientID string

// SessionID represents the Session identification.
type SessionID uint64

type packetDeliverer interface {
	deliverPacket(id ClientID, pkt *packet.Publish) error
}

// Session stores the MQTT session.
type Session struct {
	SessionID SessionID

	// ClientID represents the ID of the client owner of the session.
	ClientID ClientID

	// KeepAlive represents the MQTT keep alive of the session.
	KeepAlive int

	// ConnectedAt represents the timestamp of the last connection.
	ConnectedAt int64

	// Subscriptions contains all subscriptions for the session.
	Subscriptions map[string]Subscription

	// ExpiryInterval represents the interval, in seconds, which the session
	// expires.
	ExpiryInterval uint32

	// Version represents the MQTT version.
	Version packet.MQTTVersion

	// CleanSession indicates if the session is temporary or not.
	CleanSession bool

	// Has unexported fields
	connected        bool
	restored         bool
	inflightMessages inflightMessagesList
	lastPacketID     uint32
}

func (s *Session) clean() {
	s.Subscriptions = make(map[string]Subscription)
	s.connected = false
	s.restored = false
}

func (s *Session) nextClientID() packet.ID {
	id := atomic.LoadUint32(&s.lastPacketID)
	if id == uint32(65535) || id == uint32(0) {
		atomic.StoreUint32(&s.lastPacketID, 1)
		return 1
	}

	return packet.ID(atomic.AddUint32(&s.lastPacketID, 1))
}
