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

package store

import (
	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt"
)

// MemorySessionStore represents a store where the sessions are saved in
// memory.
type MemorySessionStore struct {
	log *logger.Logger
}

// NewMemorySessionStore creates a MemorySessionStore.
func NewMemorySessionStore(log *logger.Logger) *MemorySessionStore {
	return &MemorySessionStore{log: log}
}

// GetSession gets the session from in-memory session store.
func (s *MemorySessionStore) GetSession(id mqtt.ClientID) (mqtt.Session,
	error) {
	s.log.Debug().
		Bytes("ClientID", id).
		Msg("MQTT No session found in memory")

	return mqtt.Session{}, mqtt.ErrSessionNotFound
}

// SaveSession saves the session into the in-memory session store.
func (s *MemorySessionStore) SaveSession(id mqtt.ClientID,
	session mqtt.Session) error {

	s.log.Debug().
		Bytes("ClientID", id).
		Uint32("ExpiryInterval", session.ExpiryInterval).
		Msg("MQTT Session saved in memory")

	return nil
}
