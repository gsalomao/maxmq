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
	"fmt"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestConnectHandlerHandlePacketNewSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id}

			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)
			st.On("NewSession", id).Return(s)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)
			assert.Equal(t, s.ClientID, connAckPkt.ClientID)
			assert.Equal(t, s.KeepAlive, connAckPkt.KeepAlive)
			assert.Equal(t, tc, connAckPkt.Version)

			assert.Equal(t, tc, s.Version)
			assert.True(t, s.Connected)
			assert.NotZero(t, s.ConnectedAt)
			assert.Equal(t, int(connPkt.KeepAlive), s.KeepAlive)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketExistingSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			id := packet.ClientID("a")
			connectedAt := time.Now().Add(-1 * time.Minute).Unix()
			s := &Session{
				ClientID:       id,
				SessionID:      1,
				Version:        tc,
				ConnectedAt:    connectedAt,
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Subscriptions:  make(map[string]*Subscription),
			}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.Equal(t, tc, s.Version)
			assert.True(t, s.Connected)
			assert.NotZero(t, s.ConnectedAt)
			assert.NotEqual(t, connectedAt, s.ConnectedAt)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketExistingWithCleanSession(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			id := packet.ClientID("a")
			s := &Session{
				ClientID:       id,
				SessionID:      1,
				Version:        tc,
				ConnectedAt:    time.Now().Add(-1 * time.Minute).Unix(),
				ExpiryInterval: conf.MaxSessionExpiryInterval,
				Subscriptions:  make(map[string]*Subscription),
			}

			st.On("ReadSession", id).Return(s, nil)
			st.On("DeleteSession", s).Return(nil)

			s = &Session{ClientID: id}
			st.On("NewSession", id).Return(s)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte(id), Version: tc, CleanSession: true}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.False(t, connAckPkt.SessionPresent)

			assert.Equal(t, tc, s.Version)
			assert.True(t, s.Connected)
			assert.NotZero(t, s.ConnectedAt)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketWithInflightMessages(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			inflightMsgList := make([]*Message, 0)
			for i := 0; i < 10; i++ {
				qos := packet.QoS(i%2 + 1)

				topic := fmt.Sprintf("data/%v", i)
				newPub := packet.NewPublish(
					packet.ID(i+1),
					tc,
					topic,
					qos,
					0,   /*dup*/
					0,   /*retain*/
					nil, /*payload*/
					nil, /*props*/
				)
				msg := Message{
					ID:       MessageID(newPub.PacketID),
					PacketID: newPub.PacketID,
					Packet:   &newPub,
				}

				s.InflightMessages.PushBack(&msg)
				inflightMsgList = append(inflightMsgList, &msg)
			}

			connPkt := &packet.Connect{ClientID: []byte{'a'}, Version: tc}
			replies, err := h.HandlePacket("", connPkt)
			require.Nil(t, err)
			require.Len(t, replies, 11)

			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())

			for i := 0; i < 10; i++ {
				reply = replies[i+1]
				require.Equal(t, packet.PUBLISH, reply.Type())

				pubPkt := reply.(*packet.Publish)
				assert.Equal(t, inflightMsgList[i].Packet, pubPkt)
				assert.Equal(t, 1, inflightMsgList[i].Tries)
				assert.NotZero(t, inflightMsgList[i].LastSent)

				qos := packet.QoS(i%2 + 1)
				assert.Equal(t, qos, pubPkt.QoS)
			}
		})
	}
}

func TestConnectHandlerHandlePacketWithInflightMessagesNoPacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			for i := 0; i < 10; i++ {
				msg := Message{ID: MessageID(i + 1), PacketID: packet.ID(i + 1)}
				s.InflightMessages.PushBack(&msg)
			}

			connPkt := &packet.Connect{ClientID: []byte{'a'}, Version: tc}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			assert.Equal(t, packet.CONNACK, reply.Type())
		})
	}
}

func TestConnectHandlerHandlePacketClientIDTooBig(t *testing.T) {
	testCases := []struct {
		version  packet.Version
		id       []byte
		maxIDLen int
		code     packet.ReasonCode
	}{
		{version: packet.MQTT31, id: []byte("012345678901234567890123"),
			maxIDLen: 65535, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT311, id: []byte("0123456789012345678901234567890"),
			maxIDLen: 30, code: packet.ReasonCodeV3IdentifierRejected},
		{version: packet.MQTT50, id: []byte("0123456789012345678901234567890"),
			maxIDLen: 30, code: packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxClientIDLen = tc.maxIDLen

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			connPkt := &packet.Connect{ClientID: tc.id, Version: tc.version}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.version, connAckPkt.Version)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlerHandlePacketAllowEmptyClientID(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.AllowEmptyClientID = true

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("NewSession", mock.Anything).Return(s)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{Version: tc}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

			assert.True(t, s.Connected)
			assert.Greater(t, len(s.ClientID), 0)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketDenyEmptyClientID(t *testing.T) {
	testCases := []struct {
		version packet.Version
		code    packet.ReasonCode
	}{
		{packet.MQTT31, packet.ReasonCodeV3IdentifierRejected},
		{packet.MQTT311, packet.ReasonCodeV3IdentifierRejected},
		{packet.MQTT50, packet.ReasonCodeV5InvalidClientID},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.AllowEmptyClientID = false

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			connPkt := &packet.Connect{Version: tc.version}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlerHandlePacketV5AssignClientID(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.AllowEmptyClientID = true

	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewConnectHandler(&conf, st, &log)

	s := &Session{}
	st.On("NewSession", mock.Anything).Return(s)
	st.On("SaveSession", s).Return(nil)

	connPkt := &packet.Connect{Version: packet.MQTT50}
	replies, err := h.HandlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	assert.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.AssignedClientID)
	assert.Len(t, connAckPkt.Properties.AssignedClientID, 20)

	assert.True(t, s.Connected)
	assignedID := packet.ClientID(connAckPkt.Properties.AssignedClientID)
	assert.Equal(t, assignedID, s.ClientID)
	st.AssertExpectations(t)
}

func TestConnectHandlerHandlePacketV5AssignClientIDWithPrefix(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.AllowEmptyClientID = true
	conf.ClientIDPrefix = []byte("MAXMQ-")

	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewConnectHandler(&conf, st, &log)

	s := &Session{}
	st.On("NewSession", mock.Anything).Return(s)
	st.On("SaveSession", s).Return(nil)

	connPkt := &packet.Connect{Version: packet.MQTT50}
	replies, err := h.HandlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := replies[0].(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
	require.NotNil(t, connAckPkt.Properties)
	require.NotNil(t, connAckPkt.Properties.AssignedClientID)

	assignedID := connAckPkt.Properties.AssignedClientID
	assert.Len(t, connAckPkt.Properties.AssignedClientID, 26)
	assert.Equal(t, conf.ClientIDPrefix, assignedID[:len(conf.ClientIDPrefix)])

	assert.True(t, s.Connected)
	assert.Equal(t, packet.ClientID(assignedID), s.ClientID)
}

func TestConnectHandlerHandlePacketV5MaxSessionExpiryInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
	}{
		{0, 100},
		{100, 100},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxSessionExpiryInterval = tc.maxInterval

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{
				ClientID:   []byte("a"),
				Version:    packet.MQTT50,
				Properties: props,
			}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.Nil(t, connAckPkt.Properties)

			assert.True(t, s.Connected)
			assert.Equal(t, tc.interval, s.ExpiryInterval)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketV5AboveMaxSessionExpInterval(t *testing.T) {
	testCases := []struct {
		interval    uint32
		maxInterval uint32
		resp        uint32
	}{
		{101, 100, 100},
		{2000, 1000, 1000},
		{100000, 80000, 80000},
		{50000000, 32000000, 32000000},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.interval), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxSessionExpiryInterval = tc.maxInterval

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			props := &packet.Properties{SessionExpiryInterval: &tc.interval}
			connPkt := &packet.Connect{
				ClientID:   []byte("a"),
				Version:    packet.MQTT50,
				Properties: props,
			}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			require.NotNil(t, connAckPkt.Properties)

			props = connAckPkt.Properties
			require.NotNil(t, props.SessionExpiryInterval)
			assert.Equal(t, tc.resp, *props.SessionExpiryInterval)

			assert.True(t, s.Connected)
			assert.Equal(t, tc.resp, s.ExpiryInterval)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketV3MaxKeepAliveRejected(t *testing.T) {
	testCases := []struct {
		keepAlive    uint16
		maxKeepAlive uint16
	}{
		{0, 100},
		{101, 100},
		{501, 500},
		{65535, 65534},
	}

	for _, tc := range testCases {
		testName := fmt.Sprintf("%v-%v", tc.keepAlive, tc.maxKeepAlive)
		t.Run(testName, func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxKeepAlive = int(tc.maxKeepAlive)

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			connPkt := &packet.Connect{
				ClientID:  []byte("a"),
				Version:   packet.MQTT311,
				KeepAlive: tc.keepAlive,
			}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV3IdentifierRejected, connAckPkt.ReasonCode)
		})
	}
}

func TestConnectHandlerHandlePacketV3MaxKeepAliveAccepted(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.MaxKeepAlive = 100

	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewConnectHandler(&conf, st, &log)

	s := &Session{}
	st.On("ReadSession", mock.Anything).Return(s, nil)
	st.On("SaveSession", s).Return(nil)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT311, KeepAlive: 100}
	replies, err := h.HandlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV3ConnectionAccepted, connAckPkt.ReasonCode)

	assert.True(t, s.Connected)
	assert.Equal(t, conf.MaxKeepAlive, s.KeepAlive)
	st.AssertExpectations(t)
}

func TestConnectHandlerHandlePacketV5MaxKeepAliveAccepted(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.MaxKeepAlive = 100

	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewConnectHandler(&conf, st, &log)

	s := &Session{}
	st.On("ReadSession", mock.Anything).Return(s, nil)
	st.On("SaveSession", s).Return(nil)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50, KeepAlive: 200}
	replies, err := h.HandlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	assert.True(t, s.Connected)
	assert.Equal(t, conf.MaxKeepAlive, s.KeepAlive)

	require.NotNil(t, connAckPkt.Properties)
	assert.NotNil(t, connAckPkt.Properties.ServerKeepAlive)
	assert.Equal(t, conf.MaxKeepAlive, int(*connAckPkt.Properties.ServerKeepAlive))
	st.AssertExpectations(t)
}

func TestConnectHandlerHandlePacketV5MaxInflightMessages(t *testing.T) {
	testCases := []struct {
		maxInflight uint16
		resp        uint16
	}{
		{0, 0},
		{255, 255},
		{65534, 65534},
		{65535, 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxInflight), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxInflightMessages = int(tc.maxInflight)

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if tc.resp > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.ReceiveMaximum)
				assert.Equal(t, tc.resp, *connAckPkt.Properties.ReceiveMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5MaxPacketSize(t *testing.T) {
	testCases := []struct {
		maxSize uint32
		resp    uint32
	}{
		{0, 0},
		{255, 255},
		{65535, 65535},
		{16777215, 16777215},
		{268435455, 268435455},
		{268435456, 0},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxSize), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxPacketSize = int(tc.maxSize)

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if tc.resp > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.MaximumPacketSize)
				assert.Equal(t, tc.resp, *connAckPkt.Properties.MaximumPacketSize)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5MaximumQoS(t *testing.T) {
	testCases := []packet.QoS{
		packet.QoS0,
		packet.QoS1,
		packet.QoS2,
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaximumQoS = int(tc)

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if tc < packet.QoS2 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.MaximumQoS)
				assert.Equal(t, byte(tc), *connAckPkt.Properties.MaximumQoS)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5TopicAliasMaximum(t *testing.T) {
	testCases := []struct {
		maxAlias uint16
	}{
		{0},
		{255},
		{65535},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.maxAlias), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.MaxTopicAlias = int(tc.maxAlias)

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if tc.maxAlias > 0 {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.TopicAliasMaximum)
				assert.Equal(t, tc.maxAlias, *connAckPkt.Properties.TopicAliasMaximum)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5RetainAvailable(t *testing.T) {
	testCases := []struct {
		available bool
	}{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.RetainAvailable = tc.available

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.RetainAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.RetainAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5WildcardSubsAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.WildcardSubscriptionAvailable = tc.available

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.WildcardSubscriptionAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.WildcardSubscriptionAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5SubscriptionIDAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.SubscriptionIDAvailable = tc.available

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.SubscriptionIDAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.SubscriptionIDAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5SharedSubscriptionAvailable(t *testing.T) {
	testCases := []struct{ available bool }{
		{false},
		{true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.available), func(t *testing.T) {
			conf := newDefaultConfiguration()
			conf.SharedSubscriptionAvailable = tc.available

			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
			replies, err := h.HandlePacket("", connPkt)
			assert.Nil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)
			assert.True(t, s.Connected)
			st.AssertExpectations(t)

			if !tc.available {
				require.NotNil(t, connAckPkt.Properties)
				require.NotNil(t, connAckPkt.Properties.SharedSubscriptionAvailable)
				assert.Equal(t, byte(0), *connAckPkt.Properties.SharedSubscriptionAvailable)
			} else {
				assert.Nil(t, connAckPkt.Properties)
			}
		})
	}
}

func TestConnectHandlerHandlePacketV5UserProperty(t *testing.T) {
	conf := newDefaultConfiguration()
	conf.UserProperties = map[string]string{"k1": "v1"}

	st := &sessionStoreMock{}
	log := logger.New(&bytes.Buffer{}, nil)
	h := NewConnectHandler(&conf, st, &log)

	s := &Session{}
	st.On("ReadSession", mock.Anything).Return(s, nil)
	st.On("SaveSession", s).Return(nil)

	connPkt := &packet.Connect{ClientID: []byte("a"), Version: packet.MQTT50}
	replies, err := h.HandlePacket("", connPkt)
	assert.Nil(t, err)
	require.Len(t, replies, 1)

	reply := replies[0]
	require.Equal(t, packet.CONNACK, reply.Type())

	connAckPkt := reply.(*packet.ConnAck)
	assert.Equal(t, packet.ReasonCodeV5Success, connAckPkt.ReasonCode)

	assert.True(t, s.Connected)
	require.NotNil(t, connAckPkt.Properties)
	require.Len(t, connAckPkt.Properties.UserProperties, 1)
	assert.Equal(t, []byte("k1"), connAckPkt.Properties.UserProperties[0].Key)
	assert.Equal(t, []byte("v1"), connAckPkt.Properties.UserProperties[0].Value)
}

func TestConnectHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []struct {
		version packet.Version
		code    packet.ReasonCode
	}{
		{packet.MQTT31, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT311, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT50, packet.ReasonCodeV5ServerUnavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			st.On("ReadSession", mock.Anything).Return(nil, errors.New("failed"))

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: tc.version}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketDeleteSessionError(t *testing.T) {
	testCases := []struct {
		version packet.Version
		code    packet.ReasonCode
	}{
		{packet.MQTT31, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT311, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT50, packet.ReasonCodeV5ServerUnavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("DeleteSession", s).Return(errors.New("failed"))

			connPkt := &packet.Connect{
				ClientID:     []byte("a"),
				Version:      tc.version,
				CleanSession: true,
			}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
			st.AssertExpectations(t)
		})
	}
}

func TestConnectHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []struct {
		version packet.Version
		code    packet.ReasonCode
	}{
		{packet.MQTT31, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT311, packet.ReasonCodeV3ServerUnavailable},
		{packet.MQTT50, packet.ReasonCodeV5ServerUnavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.version.String(), func(t *testing.T) {
			conf := newDefaultConfiguration()
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewConnectHandler(&conf, st, &log)

			s := &Session{}
			st.On("ReadSession", mock.Anything).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))

			connPkt := &packet.Connect{ClientID: []byte("a"), Version: tc.version}
			replies, err := h.HandlePacket("", connPkt)
			assert.NotNil(t, err)
			require.Len(t, replies, 1)

			reply := replies[0]
			require.Equal(t, packet.CONNACK, reply.Type())

			connAckPkt := reply.(*packet.ConnAck)
			assert.Equal(t, tc.code, connAckPkt.ReasonCode)
			st.AssertExpectations(t)
		})
	}
}
