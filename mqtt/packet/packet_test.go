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

package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacket_NewPacket(t *testing.T) {
	testCases := []struct {
		tp    Type
		flags byte
		name  string
	}{
		{tp: CONNECT, flags: 0, name: "CONNECT"},
		{tp: PINGREQ, flags: 0, name: "PINGREQ"},
	}

	for _, test := range testCases {
		t.Run(
			test.name, func(t *testing.T) {
				fh := fixedHeader{
					packetType:   test.tp,
					controlFlags: test.flags,
				}

				pkt, err := newPacket(fh)
				require.Nil(t, err)
				require.NotNil(t, pkt)

				assert.Equal(t, pkt.Type().String(), test.name)
			},
		)
	}
}

func TestPacket_NewPacketInvalid(t *testing.T) {
	fh := fixedHeader{
		packetType:   RESERVED,
		controlFlags: 0,
	}

	pkt, err := newPacket(fh)
	assert.NotNil(t, err)
	assert.Nil(t, pkt)
}

func TestPacket_PacketTypeToString(t *testing.T) {
	testCases := []struct {
		tp   Type
		name string
	}{
		{tp: CONNECT, name: "CONNECT"},
		{tp: CONNACK, name: "CONNACK"},
		{tp: PUBLISH, name: "PUBLISH"},
		{tp: PUBACK, name: "PUBACK"},
		{tp: PUBREC, name: "PUBREC"},
		{tp: PUBREL, name: "PUBREL"},
		{tp: PUBCOMP, name: "PUBCOMP"},
		{tp: SUBSCRIBE, name: "SUBSCRIBE"},
		{tp: SUBACK, name: "SUBACK"},
		{tp: UNSUBSCRIBE, name: "UNSUBSCRIBE"},
		{tp: UNSUBACK, name: "UNSUBACK"},
		{tp: PINGREQ, name: "PINGREQ"},
		{tp: PINGRESP, name: "PINGRESP"},
		{tp: DISCONNECT, name: "DISCONNECT"},
	}

	for _, test := range testCases {
		t.Run(
			test.name, func(t *testing.T) {
				assert.Equal(t, test.name, test.tp.String())
			},
		)
	}
}

func TestPacket_PacketTypeToStringInvalid(t *testing.T) {
	tp := RESERVED
	assert.Equal(t, "UNKNOWN", tp.String())
}

func TestPacket_MQTTVersionToString(t *testing.T) {
	testCases := []struct {
		ver  MQTTVersion
		name string
	}{
		{ver: MQTT31, name: "3.1"},
		{ver: MQTT311, name: "3.1.1"},
		{ver: MQTT50, name: "5.0"},
	}

	for _, test := range testCases {
		t.Run(
			test.name, func(t *testing.T) {
				assert.Equal(t, test.name, test.ver.String())
			},
		)
	}
}
