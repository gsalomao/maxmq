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
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type clientID string
type sessionID uint64

type packetDeliverer interface {
	deliverPacket(id clientID, pkt *packet.Publish) error
}

type session struct {
	sessionID        sessionID
	clientID         clientID
	keepAlive        int
	connectedAt      int64
	subscriptions    map[string]Subscription
	expiryInterval   uint32
	version          packet.MQTTVersion
	cleanSession     bool
	connected        bool
	restored         bool
	inflightMessages list.List
	unAckPubMessages map[packet.ID]*message
	lastPacketID     uint32
	mutex            sync.RWMutex
}

func (s *session) clean() {
	s.subscriptions = make(map[string]Subscription)
	s.connected = false
	s.restored = false
}

func (s *session) nextClientID() packet.ID {
	id := atomic.LoadUint32(&s.lastPacketID)
	if id == uint32(65535) || id == uint32(0) {
		atomic.StoreUint32(&s.lastPacketID, 1)
		return 1
	}

	return packet.ID(atomic.AddUint32(&s.lastPacketID, 1))
}

func (s *session) findInflightMessage(id packet.ID) *list.Element {
	elem := s.inflightMessages.Front()

	for elem != nil {
		msg := elem.Value.(*message)
		if msg.packetID == id {
			return elem
		}

		elem = elem.Next()
	}

	return nil
}
