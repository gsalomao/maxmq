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
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisconnectHandlerHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewDisconnectHandler(st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{
				ClientID:      id,
				Version:       tc,
				Connected:     true,
				Subscriptions: make(map[string]*Subscription),
			}

			sub := &Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			s.Subscriptions["test"] = sub

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &discPkt)
			assert.Nil(t, err)
			assert.Empty(t, replies)
			assert.False(t, s.Connected)
			assert.NotEmpty(t, s.Subscriptions)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestDisconnectHandlerHandlePacketCleanSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewDisconnectHandler(st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{
				ClientID:      id,
				Version:       tc,
				Connected:     true,
				CleanSession:  true,
				Subscriptions: make(map[string]*Subscription),
			}

			sub := &Subscription{ID: 1, TopicFilter: "test", ClientID: id}
			s.Subscriptions["test"] = sub

			st.On("ReadSession", id).Return(s, nil)
			st.On("DeleteSession", s).Return(nil)
			subMgr.On("Unsubscribe", id, sub.TopicFilter).Return(nil)

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &discPkt)
			assert.Nil(t, err)
			require.Empty(t, replies)
			assert.False(t, s.Connected)
			assert.Empty(t, s.Subscriptions)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestDisconnectHandlerHandlePacketV5ExpiryInterval(t *testing.T) {
	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewDisconnectHandler(st, subMgr, &log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50, Connected: true, ExpiryInterval: 600}

	st.On("ReadSession", id).Return(s, nil)
	st.On("SaveSession", s).Return(nil)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, props)
	replies, err := h.HandlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(300), s.ExpiryInterval)
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}

func TestDisconnectHandlerHandlePacketV5CleanSessionExpInterval(t *testing.T) {
	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewDisconnectHandler(st, subMgr, &log)

	id := packet.ClientID("a")
	s := &Session{
		ClientID:       id,
		Version:        packet.MQTT50,
		Connected:      true,
		CleanSession:   true,
		ExpiryInterval: 600,
	}

	st.On("ReadSession", id).Return(s, nil)
	st.On("SaveSession", s).Return(nil)

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, nil /*props*/)
	replies, err := h.HandlePacket(id, &discPkt)
	assert.Nil(t, err)
	assert.Empty(t, replies)
	assert.Equal(t, uint32(600), s.ExpiryInterval)
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}

func TestDisconnectHandlerHandlePacketV5InvalidExpiryInterval(t *testing.T) {
	st := &sessionStoreMock{}
	subMgr := &subscriptionMgrMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewDisconnectHandler(st, subMgr, &log)

	id := packet.ClientID("a")
	s := &Session{ClientID: id, Version: packet.MQTT50, Connected: true}

	st.On("ReadSession", id).Return(s, nil)

	props := &packet.Properties{}
	props.SessionExpiryInterval = new(uint32)
	*props.SessionExpiryInterval = 300

	discPkt := packet.NewDisconnect(packet.MQTT50, packet.ReasonCodeV5Success, props)
	replies, err := h.HandlePacket(id, &discPkt)
	require.NotNil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	assert.Equal(t, packet.DISCONNECT, reply.Type())

	discReplyPkt := reply.(*packet.Disconnect)
	assert.Equal(t, packet.MQTT50, discReplyPkt.Version)
	assert.Equal(t, packet.ReasonCodeV5ProtocolError, discReplyPkt.ReasonCode)
	st.AssertExpectations(t)
	subMgr.AssertExpectations(t)
}

func TestDisconnectHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewDisconnectHandler(st, subMgr, &log)

			id := packet.ClientID("a")
			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)

			discPkt := packet.NewDisconnect(tc, packet.ReasonCodeV5Success, nil /*props*/)
			replies, err := h.HandlePacket(id, &discPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestDisconnectHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewDisconnectHandler(st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc, Connected: true}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))

			discPkt := packet.NewDisconnect(
				packet.MQTT50,
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &discPkt)
			assert.Nil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}

func TestDisconnectHandlerHandlePacketDeleteSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			subMgr := &subscriptionMgrMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewDisconnectHandler(st, subMgr, &log)

			id := packet.ClientID("a")
			s := &Session{
				ClientID:     id,
				Version:      tc,
				Connected:    true,
				CleanSession: true,
			}

			st.On("ReadSession", id).Return(s, nil)
			st.On("DeleteSession", s).Return(errors.New("failed"))

			discPkt := packet.NewDisconnect(
				packet.MQTT311,
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &discPkt)
			assert.Nil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
			subMgr.AssertExpectations(t)
		})
	}
}
