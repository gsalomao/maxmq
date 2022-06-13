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

import "github.com/stretchr/testify/mock"

type sessionStoreMock struct {
	mock.Mock
}

func (s *sessionStoreMock) GetSession(id ClientID) (Session, error) {
	args := s.Called(id)
	ss := args.Get(0)
	if ss == nil {
		return Session{}, args.Error(1)
	}
	return ss.(Session), args.Error(1)
}

func (s *sessionStoreMock) SaveSession(session Session) error {
	args := s.Called(session)
	return args.Error(0)
}

func (s *sessionStoreMock) DeleteSession(session Session) error {
	args := s.Called(session)
	return args.Error(0)
}
