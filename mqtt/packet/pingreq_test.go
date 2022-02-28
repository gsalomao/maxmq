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
	"bufio"
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingReq_InvalidPacketType(t *testing.T) {
	fh := fixedHeader{
		packetType: DISCONNECT, // invalid
	}

	pkt, err := newPacketPingReq(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPingReq_InvalidControlFlags(t *testing.T) {
	fh := fixedHeader{
		packetType:   PINGREQ,
		controlFlags: 1, // invalid
	}

	pkt, err := newPacketPingReq(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPingReq_InvalidRemainLength(t *testing.T) {
	fh := fixedHeader{
		packetType:      PINGREQ,
		remainingLength: 1,
	}

	pkt, err := newPacketPingReq(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestPingReq_PackUnsupported(t *testing.T) {
	fh := fixedHeader{
		packetType:      PINGREQ,
		remainingLength: 0,
	}

	pkt, err := newPacketPingReq(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	err = pkt.Pack(wr)
	require.NotNil(t, err)
}

func TestPingReq_Unpack(t *testing.T) {
	fh := fixedHeader{
		packetType:      PINGREQ,
		remainingLength: 0,
	}

	pkt, err := newPacketPingReq(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, PINGREQ, pkt.Type())

	var msg []byte
	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)
}

func BenchmarkPingReq_Unpack(b *testing.B) {
	var msg []byte
	fh := fixedHeader{
		packetType:      PINGREQ,
		remainingLength: 0,
	}
	pkt, _ := newPacketPingReq(fh)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		err := pkt.Unpack(bytes.NewBuffer(msg))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPingReq_Size(t *testing.T) {
	fh := fixedHeader{
		packetType:      PINGREQ,
		remainingLength: 0,
		size:            2,
	}

	pkt, err := newPacketPingReq(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)

	assert.Equal(t, 2, pkt.Size())
}
