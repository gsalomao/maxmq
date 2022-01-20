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
	"time"

	"github.com/stretchr/testify/mock"
)

// NetConnMock is responsible to mock the net.Conn.
type NetConnMock struct {
	mock.Mock
	net.TCPConn
}

// Read reads data from the connection.
func (c *NetConnMock) Read(b []byte) (n int, err error) {
	ret := c.Called()
	n, _ = ret.Get(0).(int)
	err, _ = ret.Get(1).(error)
	return
}

// Write writes data to the connection.
func (c *NetConnMock) Write(b []byte) (n int, err error) {
	ret := c.Called()
	n, _ = ret.Get(0).(int)
	err, _ = ret.Get(1).(error)
	return
}

// Close closes the connection.
func (c *NetConnMock) Close() error {
	ret := c.Called()
	err, _ := ret.Get(0).(error)
	return err
}

// LocalAddr returns the local network address.
func (c *NetConnMock) LocalAddr() net.Addr {
	ret := c.Called()
	addr, _ := ret.Get(0).(net.Addr)
	return addr
}

// RemoteAddr returns the remote network address.
func (c *NetConnMock) RemoteAddr() net.Addr {
	ret := c.Called()
	addr, _ := ret.Get(0).(net.Addr)
	return addr
}

// SetDeadline sets the read and write deadlines associated
// with the connection.
func (c *NetConnMock) SetDeadline(t time.Time) error {
	ret := c.Called(t)
	err, _ := ret.Get(0).(error)
	return err
}

// SetReadDeadline sets the deadline for future Read calls
// and any currently-blocked Read call.
func (c *NetConnMock) SetReadDeadline(t time.Time) error {
	ret := c.Called(t)
	err, _ := ret.Get(0).(error)
	return err
}

// SetWriteDeadline sets the deadline for future Write calls
// and any currently-blocked Write call.
func (c *NetConnMock) SetWriteDeadline(t time.Time) error {
	ret := c.Called(t)
	err, _ := ret.Get(0).(error)
	return err
}
