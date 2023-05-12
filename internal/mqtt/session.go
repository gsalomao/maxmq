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
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

var errSessionNotFound = errors.New("session not found")
var errPacketNotFound = errors.New("packet not found")

type sessionID uint64

type session struct {
	sessionID         sessionID
	clientID          packet.ClientID
	inflightMessages  list.List
	unAckMessages     map[packet.ID]*message
	subscriptions     map[string]*subscription
	mutex             sync.RWMutex
	keepAlive         int
	connectedAt       int64
	expiryInterval    uint32
	lastPacketID      uint32
	version           packet.Version
	cleanSession      bool
	connected         bool
	restored          bool
	clientIDGenerated bool
}

func (s *session) nextPacketID() packet.ID {
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
