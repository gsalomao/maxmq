/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mocks

import (
	"net"

	"github.com/stretchr/testify/mock"
)

// ConnectionHandlerMock is responsible to mock the mqtt.ConnectionHandler.
type ConnectionHandlerMock struct {
	mock.Mock
}

// Handle handles the new opened TCP connection.
func (m *ConnectionHandlerMock) Handle(conn net.Conn) {
	ret := m.Called(conn)

	if fn, ok := ret.Get(0).(func()); ok {
		fn()
	}
}
