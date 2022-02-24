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

func TestDisconnect_InvalidPacketType(t *testing.T) {
	fh := fixedHeader{
		packetType: CONNECT, // invalid
	}

	pkt, err := newPacketDisconnect(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestDisconnect_InvalidControlFlags(t *testing.T) {
	fh := fixedHeader{
		packetType:   DISCONNECT,
		controlFlags: 1, // invalid
	}

	pkt, err := newPacketDisconnect(fh)
	require.NotNil(t, err)
	require.Nil(t, pkt)
}

func TestDisconnect_PackV3(t *testing.T) {
	pkt := NewDisconnect(MQTT311, ReasonCodeV3ConnectionAccepted, nil)
	require.Equal(t, DISCONNECT, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xE0, 0}
	assert.Equal(t, msg, buf.Bytes())
}

func TestDisconnect_PackV3NoProperties(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewDisconnect(MQTT311, ReasonCodeV3ConnectionAccepted, props)
	require.Equal(t, DISCONNECT, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xE0, 0}
	assert.Equal(t, msg, buf.Bytes())
}

func TestDisconnect_PackV5(t *testing.T) {
	pkt := NewDisconnect(MQTT50, ReasonCodeV5Success, nil)
	require.Equal(t, DISCONNECT, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xE0, 2, 0, 0}
	assert.Equal(t, msg, buf.Bytes())
}

func TestDisconnect_PackV5Properties(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewDisconnect(MQTT50, ReasonCodeV5UnspecifiedError, props)
	require.Equal(t, DISCONNECT, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xE0, 7, 0x80, 5, 17, 0, 0, 0, 30}
	assert.Equal(t, msg, buf.Bytes())
}

func TestDisconnect_PackV5PropertiesInvalid(t *testing.T) {
	props := &Properties{ServerKeepAlive: new(uint16)}
	*props.ServerKeepAlive = 60

	pkt := NewDisconnect(MQTT50, ReasonCodeV5Success, props)
	require.Equal(t, DISCONNECT, pkt.Type())

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	require.NotNil(t, err)
}

func TestDisconnect_UnpackV3(t *testing.T) {
	fh := fixedHeader{
		packetType:      DISCONNECT,
		remainingLength: 0,
	}

	pkt, err := newPacketDisconnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, DISCONNECT, pkt.Type())

	var msg []byte
	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)

	discPkg, ok := pkt.(*Disconnect)
	require.True(t, ok)
	assert.Equal(t, MQTT311, discPkg.Version)
}

func TestDisconnect_UnpackV5(t *testing.T) {
	msg := []byte{0x81, 0}
	fh := fixedHeader{
		packetType:      DISCONNECT,
		remainingLength: len(msg),
	}

	pkt, err := newPacketDisconnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, DISCONNECT, pkt.Type())

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)

	discPkg, ok := pkt.(*Disconnect)
	require.True(t, ok)
	assert.Equal(t, MQTT50, discPkg.Version)
	assert.Equal(t, ReasonCodeV5MalformedPacket, discPkg.ReasonCode)
	require.NotNil(t, discPkg.Properties)
}

func TestDisconnect_UnpackV5NoReasonCode(t *testing.T) {
	fh := fixedHeader{
		packetType:      DISCONNECT,
		remainingLength: 1,
	}

	pkt, err := newPacketDisconnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, DISCONNECT, pkt.Type())

	var msg []byte
	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
}

func TestDisconnect_UnpackV5Properties(t *testing.T) {
	msg := []byte{0, 5, 17, 0, 0, 0, 30}
	fh := fixedHeader{
		packetType:      DISCONNECT,
		remainingLength: len(msg),
	}

	pkt, err := newPacketDisconnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, DISCONNECT, pkt.Type())

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.Nil(t, err)

	discPkg, ok := pkt.(*Disconnect)
	require.True(t, ok)
	assert.Equal(t, MQTT50, discPkg.Version)
	assert.Equal(t, ReasonCodeV5Success, discPkg.ReasonCode)
	require.NotNil(t, discPkg.Properties)
	require.NotNil(t, discPkg.Properties.SessionExpiryInterval)
	assert.Equal(t, uint32(30), *discPkg.Properties.SessionExpiryInterval)
}

func TestDisconnect_UnpackV5PropertiesInvalid(t *testing.T) {
	msg := []byte{0, 5, 17, 0, 0}
	fh := fixedHeader{
		packetType:      DISCONNECT,
		remainingLength: len(msg),
	}

	pkt, err := newPacketDisconnect(fh)
	require.Nil(t, err)
	require.NotNil(t, pkt)
	require.Equal(t, DISCONNECT, pkt.Type())

	err = pkt.Unpack(bytes.NewBuffer(msg))
	require.NotNil(t, err)
}