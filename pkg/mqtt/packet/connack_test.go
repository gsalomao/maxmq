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
	"bufio"
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnAck_Write(t *testing.T) {
	tests := []struct {
		ver  MQTTVersion
		code ReasonCode
		msg  []byte
	}{
		{MQTT31, ReasonCodeV3ConnectionAccepted, []byte{0x20, 2, 0, 0}},
		{MQTT311, ReasonCodeV3NotAuthorized, []byte{0x20, 2, 0, 5}},
		{MQTT50, ReasonCodeV5Success, []byte{0x20, 3, 0, 0, 0}},
		{MQTT50, ReasonCodeV5UnspecifiedError, []byte{0x20, 3, 0, 0x80, 0}},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v-%v", tt.ver, tt.code), func(t *testing.T) {
			pkt := NewConnAck(tt.ver, tt.code, false, nil)
			assert.Equal(t, CONNACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, tt.msg, buf.Bytes())
		})
	}
}

func BenchmarkConnAck_WriteV3(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnAck_WriteV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewConnAck(MQTT50, ReasonCodeV5Success, false, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestConnAck_WriteSessionPresent(t *testing.T) {
	testCases := []struct {
		ver MQTTVersion
		val bool
		msg []byte
	}{
		{MQTT31, false, []byte{0x20, 2, 0, 0}},
		{MQTT31, true, []byte{0x20, 2, 0, 0}},
		{MQTT311, false, []byte{0x20, 2, 0, 0}},
		{MQTT311, true, []byte{0x20, 2, 1, 0}},
		{MQTT50, false, []byte{0x20, 3, 0, 0, 0}},
		{MQTT50, true, []byte{0x20, 3, 1, 0, 0}},
	}

	for _, tt := range testCases {
		pkt := NewConnAck(tt.ver, ReasonCodeV3ConnectionAccepted, tt.val,
			nil)
		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		assert.Nil(t, err)

		err = wr.Flush()
		assert.Nil(t, err)

		assert.Equal(t, tt.msg, buf.Bytes())
	}
}

func TestConnAck_WriteV5Properties(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewConnAck(MQTT50, ReasonCodeV5Success, false, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{
		0x20, 8, // fixed header
		0, 0, // variable header
		5,               // property length
		17, 0, 0, 0, 30, // SessionExpiryInterval
	}
	assert.Equal(t, msg, buf.Bytes())
}

func TestConnAck_WriteV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewConnAck(MQTT50, ReasonCodeV5Success, false, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestConnAck_WriteV3PropertiesIgnored(t *testing.T) {
	props := &Properties{SessionExpiryInterval: new(uint32)}
	*props.SessionExpiryInterval = 30

	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{
		0x20, 2, // fixed header
		0, 0, // variable header
	}
	assert.Equal(t, msg, buf.Bytes())
}

func TestConnAck_ReadUnsupported(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Read(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestConnAck_Size(t *testing.T) {
	t.Run("Unknown", func(t *testing.T) {
		pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
		require.NotNil(t, pkt)
		assert.Equal(t, 0, pkt.Size())
	})

	t.Run("V3", func(t *testing.T) {
		pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 4, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		pkt := NewConnAck(MQTT50, ReasonCodeV5Success, false, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 5, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		props := &Properties{SessionExpiryInterval: new(uint32)}
		*props.SessionExpiryInterval = 30

		pkt := NewConnAck(MQTT50, ReasonCodeV5Success, false, props)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 10, pkt.Size())
	})
}

func TestConnAck_Timestamp(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
