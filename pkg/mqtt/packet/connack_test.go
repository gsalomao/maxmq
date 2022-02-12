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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnAck_Pack(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReturnCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)
	require.Equal(t, CONNACK, pkt.Type())

	out := new(bytes.Buffer)
	err := pkt.Pack(out)
	assert.Nil(t, err)

	msg := []byte{
		0x20, 2, // fixed header
		0, 0, // variable header
	}
	assert.Equal(t, msg, out.Bytes())
}

func TestConnAck_PackReturnCode(t *testing.T) {
	testCases := []struct {
		ver  MQTTVersion
		code ReturnCode
		data byte
	}{
		{ver: MQTT31, code: ReturnCodeV3ConnectionAccepted, data: 0},
		{ver: MQTT311, code: ReturnCodeV3NotAuthorized, data: 5},
		{ver: MQTT50, code: ReturnCodeV5Success, data: 0},
		{ver: MQTT50, code: ReturnCodeV5UnspecifiedError, data: 0x80},
	}

	for _, test := range testCases {
		pkt := NewConnAck(test.ver, test.code, false, nil)
		out := new(bytes.Buffer)

		err := pkt.Pack(out)
		assert.Nil(t, err)

		msg := []byte{
			0x20, 2, // fixed header
			0, test.data, // variable header
		}
		assert.Equal(t, msg, out.Bytes())
	}
}

func TestConnAck_PackSessionPresent(t *testing.T) {
	testCases := []struct {
		ver  MQTTVersion
		val  bool
		data byte
	}{
		{ver: MQTT31, val: false, data: 0},
		{ver: MQTT31, val: true, data: 0},
		{ver: MQTT311, val: false, data: 0},
		{ver: MQTT311, val: true, data: 1},
		{ver: MQTT50, val: false, data: 0},
		{ver: MQTT50, val: true, data: 1},
	}

	for _, test := range testCases {
		pkt := NewConnAck(test.ver, ReturnCodeV3ConnectionAccepted, test.val,
			nil)
		out := new(bytes.Buffer)

		err := pkt.Pack(out)
		assert.Nil(t, err)

		msg := []byte{
			0x20, 2, // fixed header
			test.data, 0, // variable header
		}
		assert.Equal(t, msg, out.Bytes())
	}
}

func TestConnAck_PackProperties(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewConnAck(MQTT50, ReturnCodeV5Success, false, props)
	require.NotNil(t, pkt)

	out := new(bytes.Buffer)
	err := pkt.Pack(out)
	assert.Nil(t, err)

	msg := []byte{
		0x20, 2, // fixed header
		0, 0, // variable header
		5,               // property length
		17, 0, 0, 0, 30, // SessionExpiryInterval
	}
	assert.Equal(t, msg, out.Bytes())
}

func TestConnAck_PackV3WithoutProperties(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewConnAck(MQTT311, ReturnCodeV3ConnectionAccepted, false, props)
	require.NotNil(t, pkt)

	out := new(bytes.Buffer)
	err := pkt.Pack(out)
	assert.Nil(t, err)

	msg := []byte{
		0x20, 2, // fixed header
		0, 0, // variable header
	}
	assert.Equal(t, msg, out.Bytes())
}

func TestConnAck_UnpackUnsupported(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReturnCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)

	buf := new(bytes.Buffer)
	err := pkt.Unpack(buf)
	require.NotNil(t, err)
}
