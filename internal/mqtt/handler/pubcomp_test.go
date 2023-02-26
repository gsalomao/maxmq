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

func TestPubCompHandlerHandlePacket(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubCompHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			msg := &Message{PacketID: 1, Tries: 1,
				LastSent: time.Now().UnixMicro()}
			s.InflightMessages.PushBack(msg)

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			pubCompPkt := packet.NewPubComp(msg.PacketID, tc,
				packet.ReasonCodeV5Success, nil)

			replies, err := h.HandlePacket(id, &pubCompPkt)
			require.Nil(t, err)
			require.Empty(t, replies)
			require.Equal(t, 0, s.InflightMessages.Len())
			st.AssertExpectations(t)
		})
	}
}

func TestPubCompHandlerHandlePacketPacketNotFound(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubCompHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			st.On("ReadSession", id).Return(s, nil)

			pubCompPkt := packet.NewPubComp(2, tc,
				packet.ReasonCodeV5Success, nil)

			replies, err := h.HandlePacket(id, &pubCompPkt)
			require.NotNil(t, err)
			require.Empty(t, replies)
			st.AssertExpectations(t)
		})
	}
}

func TestPubCompHandlerHandlePacketReadSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubCompHandler(st, &log)

			id := packet.ClientID("a")

			st.On("ReadSession", id).
				Return(nil, ErrSessionNotFound)

			pubCompPkt := packet.NewPubComp(1, tc, packet.ReasonCodeV5Success,
				nil)

			replies, err := h.HandlePacket(id, &pubCompPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}

func TestPubCompHandlerHandlePacketSaveSessionError(t *testing.T) {
	testCases := []packet.Version{
		packet.MQTT31,
		packet.MQTT311,
		packet.MQTT50,
	}

	for _, tc := range testCases {
		t.Run(tc.String(), func(t *testing.T) {
			st := &sessionStoreMock{}
			log := logger.New(&bytes.Buffer{}, nil)
			h := NewPubCompHandler(st, &log)

			id := packet.ClientID("a")
			s := &Session{ClientID: id, Version: tc}

			msg := &Message{PacketID: 1, Tries: 1,
				LastSent: time.Now().UnixMicro()}
			s.InflightMessages.PushBack(msg)

			st.On("ReadSession", id).Return(s, nil)
			st.On("SaveSession", s).Return(errors.New("failed"))

			pubCompPkt := packet.NewPubComp(1, tc, packet.ReasonCodeV5Success,
				nil)

			replies, err := h.HandlePacket(id, &pubCompPkt)
			assert.NotNil(t, err)
			assert.Empty(t, replies)
		})
	}
}
