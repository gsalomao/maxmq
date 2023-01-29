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
		{pktType: CONNECT, flags: 0},
		{pktType: PINGREQ, flags: 0},
		{pktType: SUBSCRIBE, flags: 2},
		{pktType: PUBLISH, flags: 0},
		{pktType: PUBACK, flags: 0, remainingLen: 2},
		{pktType: PUBREC, flags: 0, remainingLen: 2},
		{pktType: PUBREL, flags: 2, remainingLen: 2},
		{pktType: PUBCOMP, flags: 0, remainingLen: 2},
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
		{name: "CONNECT", pktType: CONNECT},
		{name: "CONNACK", pktType: CONNACK},
		{name: "PUBLISH", pktType: PUBLISH},
		{name: "PUBACK", pktType: PUBACK},
		{name: "PUBREC", pktType: PUBREC},
		{name: "PUBREL", pktType: PUBREL},
		{name: "PUBCOMP", pktType: PUBCOMP},
		{name: "SUBSCRIBE", pktType: SUBSCRIBE},
		{name: "SUBACK", pktType: SUBACK},
		{name: "UNSUBSCRIBE", pktType: UNSUBSCRIBE},
		{name: "UNSUBACK", pktType: UNSUBACK},
		{name: "PINGREQ", pktType: PINGREQ},
		{name: "PINGRESP", pktType: PINGRESP},
		{name: "DISCONNECT", pktType: DISCONNECT},
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
		version MQTTVersion
	}{
		{name: "3.1", version: MQTT31},
		{name: "3.1.1", version: MQTT311},
		{name: "5.0", version: MQTT50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.name, tc.version.String())
		})
	}
}
