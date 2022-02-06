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

package packet_test

import (
	"testing"

	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacket_NewPacket(t *testing.T) {
	testCases := []struct {
		tp    packet.PacketType
		flags byte
		name  string
	}{
		{tp: packet.CONNECT, flags: 0, name: "CONNECT"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			fh := packet.FixedHeader{
				PacketType:   test.tp,
				ControlFlags: test.flags,
			}

			pkt, err := packet.NewPacket(fh)
			require.Nil(t, err)
			require.NotNil(t, pkt)

			assert.Equal(t, pkt.Type().String(), test.name)
		})
	}
}

func TestPacket_NewPacketInvalid(t *testing.T) {
	fh := packet.FixedHeader{
		PacketType:   packet.RESERVED,
		ControlFlags: 0,
	}

	pkt, err := packet.NewPacket(fh)
	assert.NotNil(t, err)
	assert.Nil(t, pkt)
}

func TestPacket_PacketTypeToString(t *testing.T) {
	testCases := []struct {
		tp   packet.PacketType
		name string
	}{
		{tp: packet.CONNECT, name: "CONNECT"},
		{tp: packet.CONNACK, name: "CONNACK"},
		{tp: packet.PUBLISH, name: "PUBLISH"},
		{tp: packet.PUBACK, name: "PUBACK"},
		{tp: packet.PUBREC, name: "PUBREC"},
		{tp: packet.PUBREL, name: "PUBREL"},
		{tp: packet.PUBCOMP, name: "PUBCOMP"},
		{tp: packet.SUBSCRIBE, name: "SUBSCRIBE"},
		{tp: packet.SUBACK, name: "SUBACK"},
		{tp: packet.UNSUBSCRIBE, name: "UNSUBSCRIBE"},
		{tp: packet.UNSUBACK, name: "UNSUBACK"},
		{tp: packet.PINGREQ, name: "PINGREQ"},
		{tp: packet.PINGRESP, name: "PINGRESP"},
		{tp: packet.DISCONNECT, name: "DISCONNECT"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.name, test.tp.String())
		})
	}
}

func TestPacket_PacketTypeToStringInvalid(t *testing.T) {
	tp := packet.RESERVED
	assert.Equal(t, "", tp.String())
}

func TestPacket_MQTTVersionToString(t *testing.T) {
	testCases := []struct {
		ver  packet.MQTTVersion
		name string
	}{
		{ver: packet.MQTT_V3_1, name: "3.1"},
		{ver: packet.MQTT_V3_1_1, name: "3.1.1"},
		{ver: packet.MQTT_V5_0, name: "5.0"},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.name, test.ver.String())
		})
	}
}
