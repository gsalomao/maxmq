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
	"container/list"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

// ErrSessionNotFound indicates that the session was not found.
var ErrSessionNotFound = errors.New("session not found")

// ErrPacketNotFound indicates that the packet with a given packet ID was not
// found.
var ErrPacketNotFound = errors.New("packet not found")

// SessionID represents the Session identifier.
type SessionID uint64

// SessionStore is responsible to handle the storage of the session.
type SessionStore interface {
	// NewSession creates a new Session for given client identifier.
	NewSession(id packet.ClientID) *Session

	// ReadSession reads the Session for the given client identifier.
	ReadSession(id packet.ClientID) (*Session, error)

	// SaveSession saves the given Session.
	SaveSession(s *Session) error

	// DeleteSession deletes the given Session.
	DeleteSession(s *Session) error
}

// Session represents the MQTT session for a specific MQTT client.
type Session struct {
	// SessionID is the session identifier.
	SessionID SessionID

	// ClientID is the MQTT client identifier.
	ClientID packet.ClientID

	// InflightMessages contains all the in-flight messages (messages being
	// sent to the client).
	InflightMessages list.List

	// UnAckMessages contains all unacknowledged QoS2 messages.
	UnAckMessages map[packet.ID]*Message

	// Subscriptions contains all the client's subscriptions.
	Subscriptions map[string]*Subscription

	// Mutex is the mutual exclusion lock to control concurrent access in the
	//session.
	Mutex sync.RWMutex

	// KeepAlive is a time interval, measured in seconds, that is permitted to
	// elapse between the point at which the client finishes transmitting one
	// Control Packet and the point it starts sending the next.
	KeepAlive int

	// ConnectedAt is the timestamp which the last connection was established.
	ConnectedAt int64

	// ExpiryInterval is the time, in seconds, which the broker must store the
	// session after the network connection is closed.
	ExpiryInterval uint32

	// LastPacketID is the last packet identified generated in the session.
	LastPacketID uint32

	// Version is the MQTT version.
	Version packet.Version

	// CleanSession indicates whether the session is temporary or not.
	CleanSession bool

	// Connected indicates whether the client is currently connected or not.
	Connected bool

	// Restored indicates whether the session was restored or not.
	Restored bool

	// ClientIDGenerated indicates whether the client identifier was generated
	// or given by the CONNECT packet.
	ClientIDGenerated bool
}

// NextPacketID generates a new packet identifier for the Session.
func (s *Session) NextPacketID() packet.ID {
	id := atomic.LoadUint32(&s.LastPacketID)
	if id == uint32(65535) || id == uint32(0) {
		atomic.StoreUint32(&s.LastPacketID, 1)
		return 1
	}

	return packet.ID(atomic.AddUint32(&s.LastPacketID, 1))
}

func (s *Session) findInflightMessage(id packet.ID) *list.Element {
	elem := s.InflightMessages.Front()

	for elem != nil {
		msg := elem.Value.(*Message)
		if msg.PacketID == id {
			return elem
		}

		elem = elem.Next()
	}

	return nil
}
