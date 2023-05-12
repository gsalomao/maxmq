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
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSessionStoreNewSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)

	cID := packet.ClientID("a")
	sID := 10
	idGen.On("NextID").Return(sID)

	s := sm.newSession(cID)
	require.NotNil(t, s)
	assert.Equal(t, cID, s.clientID)
	assert.Equal(t, sessionID(sID), s.sessionID)
	idGen.AssertExpectations(t)
}

func TestSessionStoreReadSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)

	cID := packet.ClientID("a")
	s := &session{clientID: cID, sessionID: 10}
	sm.sessions[cID] = s

	ss, err := sm.readSession(cID)
	require.Nil(t, err)
	require.NotNil(t, ss)
	assert.Equal(t, s, ss)
}

func TestSessionStoreReadSessionNotFound(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)
	cID := packet.ClientID("a")

	ss, err := sm.readSession(cID)
	assert.Equal(t, errSessionNotFound, err)
	assert.Nil(t, ss)
}

func TestSessionStoreSaveSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)

	cID := packet.ClientID("a")
	s := &session{clientID: cID, sessionID: 10}

	err := sm.saveSession(s)
	assert.Nil(t, err)
}

func TestSessionStoreDeleteSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)

	cID := packet.ClientID("a")
	s := &session{clientID: cID, sessionID: 10}
	sm.sessions[cID] = s

	err := sm.deleteSession(s)
	assert.Nil(t, err)
}

func TestSessionStoreDeleteSessionError(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, log)

	cID := packet.ClientID("a")
	s := &session{clientID: cID, sessionID: 10}

	err := sm.deleteSession(s)
	assert.NotNil(t, err)
}
