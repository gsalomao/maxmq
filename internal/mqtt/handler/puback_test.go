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
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPubAckHandlerHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubAckHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			pubPkt := packet.NewPublish(
				2,      /*id*/
				tc,     /*version*/
				"data", /*topic*/
				packet.QoS1,
				0,   /*dup*/
				0,   /*retain*/
				nil, /*payload*/
				nil, /*props*/
			)

			msg := &Message{ID: 1, PacketID: pubPkt.PacketID, Packet: &pubPkt, Tries: 1,
				LastSent: time.Now().UnixMicro()}

			s.InflightMessages.PushBack(&Message{ID: 1, PacketID: 1})
			s.InflightMessages.PushBack(msg)

			pubAckPkt := packet.NewPubAck(
				pubPkt.PacketID,
				pubPkt.Version,
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubAckPkt)
			require.Nil(t, err)
			assert.Empty(t, replies)
			assert.Equal(t, 1, s.InflightMessages.Len())
			st.AssertExpectations(t)
		})
	}
}

func TestPubAckHandlerHandlePacketUnknownMessage(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubAckHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			st.On("ReadSession", id).Return(s, nil)

			pubAckPkt := packet.NewPubAck(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubAckPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}

func TestPubAckHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubAckHandler(st, &log)
			id := packet.ClientID("a")

			st.On("ReadSession", id).Return(nil, ErrSessionNotFound)

			pubAckPkt := packet.NewPubAck(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubAckPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}

func TestPubAckHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubAckHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))

			pubPkt := packet.NewPublish(
				1,      /*id*/
				tc,     /*version*/
				"data", /*topic*/
				packet.QoS1,
				0,   /*dup*/
				0,   /*retain*/
				nil, /*payload*/
				nil, /*props*/
			)

			msg := &Message{
				ID:       1,
				PacketID: pubPkt.PacketID,
				Packet:   &pubPkt,
				Tries:    1,
				LastSent: time.Now().UnixMicro(),
			}
			s.InflightMessages.PushBack(msg)

			pubAckPkt := packet.NewPubAck(
				1,  /*id*/
				tc, /*version*/
				packet.ReasonCodeV5Success,
				nil, /*props*/
			)
			replies, err := h.HandlePacket(id, &pubAckPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}
