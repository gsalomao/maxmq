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

package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketNewPacket(t *testing.T) {
	testCases := []struct {
		pktType      Type
		flags        byte
		remainingLen int
	}{
		{CONNECT, 0, 0},
		{PINGREQ, 0, 0},
		{SUBSCRIBE, 2, 0},
		{PUBLISH, 0, 0},
		{PUBACK, 0, 2},
		{PUBREC, 0, 2},
		{PUBREL, 2, 2},
		{PUBCOMP, 0, 2},
	}

	for _, tc := range testCases {
		t.Run(tc.pktType.String(), func(t *testing.T) {
			opts := options{
				packetType:      tc.pktType,
				controlFlags:    tc.flags,
				version:         MQTT311,
				remainingLength: tc.remainingLen,
			}
			pkt, err := newPacket(opts)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			assert.Equal(t, tc.pktType, pkt.Type())
		})
	}
}

func TestPacketNewPacketInvalid(t *testing.T) {
	opts := options{packetType: RESERVED, controlFlags: 0}
	pkt, err := newPacket(opts)
	assert.NotNil(t, err)
	assert.Nil(t, pkt)
}

func TestPacketPacketTypeToString(t *testing.T) {
	testCases := []struct {
		name    string
		pktType Type
	}{
		{"CONNECT", CONNECT},
		{"CONNACK", CONNACK},
		{"PUBLISH", PUBLISH},
		{"PUBACK", PUBACK},
		{"PUBREC", PUBREC},
		{"PUBREL", PUBREL},
		{"PUBCOMP", PUBCOMP},
		{"SUBSCRIBE", SUBSCRIBE},
		{"SUBACK", SUBACK},
		{"UNSUBSCRIBE", UNSUBSCRIBE},
		{"UNSUBACK", UNSUBACK},
		{"PINGREQ", PINGREQ},
		{"PINGRESP", PINGRESP},
		{"DISCONNECT", DISCONNECT},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.name, tc.pktType.String())
		})
	}
}

func TestPacketPacketTypeToStringInvalid(t *testing.T) {
	tp := RESERVED
	assert.Equal(t, "UNKNOWN", tp.String())
}

func TestPacketMQTTVersionToString(t *testing.T) {
	testCases := []struct {
		name    string
		version Version
	}{
		{"3.1", MQTT31},
		{"3.1.1", MQTT311},
		{"5.0", MQTT50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.name, tc.version.String())
		})
	}
}
