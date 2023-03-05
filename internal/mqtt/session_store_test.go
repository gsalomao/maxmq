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

	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type sessionStoreMock struct {
	mock.Mock
}

func (m *sessionStoreMock) NewSession(id packet.ClientID) *handler.Session {
	args := m.Called(id)
	return args.Get(0).(*handler.Session)
}

func (m *sessionStoreMock) ReadSession(id packet.ClientID) (*handler.Session, error) {
	var s *handler.Session
	args := m.Called(id)
	if args.Get(0) != nil {
		s = args.Get(0).(*handler.Session)
	}
	return s, args.Error(1)
}

func (m *sessionStoreMock) SaveSession(s *handler.Session) error {
	args := m.Called(s)
	return args.Error(0)
}

func (m *sessionStoreMock) DeleteSession(s *handler.Session) error {
	args := m.Called(s)
	return args.Error(0)
}

func TestSessionStoreNewSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)

	cID := packet.ClientID("a")
	sID := 10
	idGen.On("NextID").Return(sID)

	s := sm.NewSession(cID)
	require.NotNil(t, s)
	assert.Equal(t, cID, s.ClientID)
	assert.Equal(t, handler.SessionID(sID), s.SessionID)
	idGen.AssertExpectations(t)
}

func TestSessionStoreReadSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)

	cID := packet.ClientID("a")
	s := &handler.Session{ClientID: cID, SessionID: 10}
	sm.sessions[cID] = s

	ss, err := sm.ReadSession(cID)
	require.Nil(t, err)
	require.NotNil(t, ss)
	assert.Equal(t, s, ss)
}

func TestSessionStoreReadSessionNotFound(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)
	cID := packet.ClientID("a")

	ss, err := sm.ReadSession(cID)
	assert.Equal(t, handler.ErrSessionNotFound, err)
	assert.Nil(t, ss)
}

func TestSessionStoreSaveSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)

	cID := packet.ClientID("a")
	s := &handler.Session{ClientID: cID, SessionID: 10}

	err := sm.SaveSession(s)
	assert.Nil(t, err)
}

func TestSessionStoreDeleteSession(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)

	cID := packet.ClientID("a")
	s := &handler.Session{ClientID: cID, SessionID: 10}
	sm.sessions[cID] = s

	err := sm.DeleteSession(s)
	assert.Nil(t, err)
}

func TestSessionStoreDeleteSessionError(t *testing.T) {
	idGen := &idGeneratorMock{}
	log := newLogger()
	sm := newSessionStore(idGen, &log)

	cID := packet.ClientID("a")
	s := &handler.Session{ClientID: cID, SessionID: 10}

	err := sm.DeleteSession(s)
	assert.NotNil(t, err)
}
