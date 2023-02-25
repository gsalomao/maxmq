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
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubAckWrite(t *testing.T) {
	testCases := []struct {
		name    string
		id      ID
		version Version
		codes   []ReasonCode
		msg     []byte
	}{
		{name: "V3.1", id: 1, version: MQTT31,
			codes: []ReasonCode{ReasonCodeV3GrantedQoS0},
			msg:   []byte{0x90, 3, 0, 1, 0}},
		{name: "V3.1.1", id: 2, version: MQTT311,
			codes: []ReasonCode{ReasonCodeV3GrantedQoS0,
				ReasonCodeV3GrantedQoS2},
			msg: []byte{0x90, 4, 0, 2, 0, 2}},
		{name: "V5.0", id: 3, version: MQTT50,
			codes: []ReasonCode{ReasonCodeV5GrantedQoS1},
			msg:   []byte{0x90, 4, 0, 3, 0, 1}},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewSubAck(test.id, test.version, test.codes, nil)
			assert.Equal(t, SUBACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, test.msg, buf.Bytes())
		})
	}
}

func BenchmarkSubAckWriteV3(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSubAckWriteV5(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewSubAck(4, MQTT50, []ReasonCode{0, 1, 2}, nil)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		buf.Reset()

		err := pkt.Write(wr)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSubAckWriteV5Properties(t *testing.T) {
	props := &Properties{}
	props.ReasonString = []byte("abc")

	pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.Nil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)

	msg := []byte{0x90, 12, 0, 5, 6, 31, 0, 3, 'a', 'b', 'c', 1, 0, 2}
	assert.Equal(t, msg, buf.Bytes())
}

func TestSubAckWriteV5InvalidProperty(t *testing.T) {
	props := &Properties{TopicAlias: new(uint16)}
	*props.TopicAlias = 10

	pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestSubAckWriteFailure(t *testing.T) {
	pkt := NewSubAck(5, MQTT50, []ReasonCode{1, 0, 2}, nil)
	require.NotNil(t, pkt)

	conn, _ := net.Pipe()
	w := bufio.NewWriterSize(conn, 1)
	_ = conn.Close()

	err := pkt.Write(w)
	assert.NotNil(t, err)
}

func TestSubAckReadUnsupported(t *testing.T) {
	pkt := NewSubAck(4, MQTT311, []ReasonCode{0, 1, 2}, nil)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Read(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestSubAckSize(t *testing.T) {
	testCases := []struct {
		name    string
		version Version
		codes   []ReasonCode
		props   *Properties
		size    int
	}{
		{name: "V3.1", version: MQTT31, size: 7},
		{name: "V3.1.1", version: MQTT311, size: 7},
		{name: "V5.0-Success", version: MQTT50, codes: []ReasonCode{0, 1, 2},
			size: 8},
		{name: "V5.0-Properties", version: MQTT50, codes: []ReasonCode{0, 1, 2},
			props: &Properties{ReasonString: []byte{'a'}}, size: 12},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewSubAck(1, test.version, []ReasonCode{0, 1, 2},
				test.props)
			require.NotNil(t, pkt)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)

			assert.Equal(t, test.size, pkt.Size())
		})
	}
}

func TestSubAckTimestamp(t *testing.T) {
	pkt := NewSubAck(4, MQTT50, []ReasonCode{0}, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
