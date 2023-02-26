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
	"math"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type sessionStoreMock struct {
	mock.Mock
}

func (sm *sessionStoreMock) NewSession(id packet.ClientID) *Session {
	args := sm.Called(id)
	return args.Get(0).(*Session)
}

func (sm *sessionStoreMock) ReadSession(id packet.ClientID) (*Session,
	error) {

	var s *Session
	args := sm.Called(id)
	if args.Get(0) != nil {
		s = args.Get(0).(*Session)
	}
	return s, args.Error(1)
}

func (sm *sessionStoreMock) SaveSession(s *Session) error {
	args := sm.Called(s)
	return args.Error(0)
}

func (sm *sessionStoreMock) ResetSession(s *Session) error {
	args := sm.Called(s)
	return args.Error(0)
}

func (sm *sessionStoreMock) DeleteSession(s *Session) error {
	args := sm.Called(s)
	return args.Error(0)
}

func TestSessionNextPacketID(t *testing.T) {
	s := Session{}

	for i := 0; i < math.MaxUint16; i++ {
		id := s.NextPacketID()
		assert.Equal(t, packet.ID(i+1), id)
	}

	id := s.NextPacketID()
	assert.Equal(t, packet.ID(1), id)
}
