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

func TestSubAck_Pack(t *testing.T) {
	tests := []struct {
		id  ID
		ver MQTTVersion
		cds []ReasonCode
		msg []byte
	}{
		{
			id:  1,
			ver: MQTT31,
			cds: []ReasonCode{ReasonCodeV3GrantedQoS0},
			msg: []byte{0x90, 3, 0, 1, 0},
		},
		{
			id:  2,
			ver: MQTT311,
			cds: []ReasonCode{ReasonCodeV3GrantedQoS0, ReasonCodeV3GrantedQoS2},
			msg: []byte{0x90, 4, 0, 2, 0, 2},
		},
		{
			id:  3,
			ver: MQTT50,
			cds: []ReasonCode{ReasonCodeV3GrantedQoS1},
			msg: []byte{0x90, 4, 0, 3, 0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v-%v", tt.ver, tt.id), func(t *testing.T) {
			pkt := NewSubAck(tt.id, tt.ver, tt.cds, nil)
			assert.Equal(t, SUBACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Pack(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, tt.msg, buf.Bytes())
		})
	}
}

func BenchmarkSubAck_PackV3(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Pack(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubAck_PackV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewSubAck(4, MQTT50, []ReasonCode{0, 1, 2}, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Pack(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubAck_PackV5Properties(t *testing.T) {
	props := &Properties{}
	props.ReasonString = []byte("abc")

	pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0x90, 12, 0, 5, 6, 31, 0, 3, 'a', 'b', 'c', 1, 0, 2}
	assert.Equal(t, msg, buf.Bytes())
}

func TestSubAck_PackV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Pack(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestSubAck_UnpackUnsupported(t *testing.T) {
	pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Unpack(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestSubAck_Size(t *testing.T) {
	t.Run("Unknown", func(t *testing.T) {
		pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)
		require.NotNil(t, pkt)
		assert.Equal(t, 0, pkt.Size())
	})

	t.Run("V3", func(t *testing.T) {
		pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 7, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		pkt := NewSubAck(4, MQTT50, []ReasonCode{0, 1, 2}, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 8, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		props := &Properties{}
		props.ReasonString = []byte("abc")

		pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, props)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Pack(wr)
		require.Nil(t, err)

		assert.Equal(t, 14, pkt.Size())
	})
}

func TestSubAck_Timestamp(t *testing.T) {
	pkt := NewSubAck(4, MQTT50, []ReasonCode{0}, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
