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

func TestUnsubAckWrite(t *testing.T) {
	tests := []struct {
		id      ID
		version MQTTVersion
		codes   []ReasonCode
		msg     []byte
	}{
		{
			id:      1,
			version: MQTT31,
			codes:   nil,
			msg:     []byte{0xB0, 2, 0, 1},
		},
		{
			id:      2,
			version: MQTT311,
			codes:   nil,
			msg:     []byte{0xB0, 2, 0, 2},
		},
		{
			id:      3,
			version: MQTT50,
			codes:   []ReasonCode{ReasonCodeV5Success},
			msg:     []byte{0xB0, 4, 0, 3, 0, 0},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%v-%v", tt.version, tt.id), func(t *testing.T) {
			pkt := NewUnsubAck(tt.id, tt.version, tt.codes, nil)
			assert.Equal(t, UNSUBACK, pkt.Type())

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

func BenchmarkUnsubAckWriteV3(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewUnsubAck(4, MQTT311, nil, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnsubAckWriteV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewUnsubAck(4, MQTT50, []ReasonCode{0, 17, 128}, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestUnsubAckWriteV5Properties(t *testing.T) {
	props := &Properties{}
	props.ReasonString = []byte("abc")

	pkt := NewUnsubAck(5, MQTT50, []ReasonCode{0, 17, 128}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0xB0, 12, 0, 5, 6, 31, 0, 3, 'a', 'b', 'c', 0, 17, 128}
	assert.Equal(t, msg, buf.Bytes())
}

func TestUnsubAckWriteV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewUnsubAck(5, MQTT50, []ReasonCode{0, 17, 128}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestUnsubAckReadUnsupported(t *testing.T) {
	pkt := NewUnsubAck(4, MQTT311, nil, nil)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Read(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestUnsubAckSize(t *testing.T) {
	t.Run("Unknown", func(t *testing.T) {
		pkt := NewUnsubAck(4, MQTT311, nil, nil)
		require.NotNil(t, pkt)
		assert.Equal(t, 0, pkt.Size())
	})

	t.Run("V3", func(t *testing.T) {
		pkt := NewUnsubAck(4, MQTT311, nil, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 4, pkt.Size())
	})

	t.Run("V5", func(t *testing.T) {
		pkt := NewUnsubAck(4, MQTT50, []ReasonCode{0, 17, 128}, nil)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 8, pkt.Size())
	})

	t.Run("V5-Properties", func(t *testing.T) {
		props := &Properties{}
		props.ReasonString = []byte("abc")

		pkt := NewUnsubAck(5, MQTT50, []ReasonCode{0, 128}, props)
		require.NotNil(t, pkt)

		buf := &bytes.Buffer{}
		wr := bufio.NewWriter(buf)

		err := pkt.Write(wr)
		require.Nil(t, err)

		assert.Equal(t, 13, pkt.Size())
	})
}

func TestUnsubAckTimestamp(t *testing.T) {
	pkt := NewUnsubAck(4, MQTT50, []ReasonCode{0}, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
