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
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnsubAckWrite(t *testing.T) {
	testCases := []struct {
		id      ID
		version Version
		codes   []ReasonCode
		msg     []byte
	}{
		{id: 1, version: MQTT31, codes: nil, msg: []byte{0xB0, 2, 0, 1}},
		{id: 2, version: MQTT311, codes: []ReasonCode{ReasonCodeV5Success}, msg: []byte{0xB0, 2, 0, 2}},
		{id: 3, version: MQTT50, codes: []ReasonCode{ReasonCodeV5Success},
			msg: []byte{0xB0, 4, 0, 3, 0, 0}},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprint(tc.id), func(t *testing.T) {
			pkt := NewUnsubAck(tc.id, tc.version, tc.codes, nil /*props*/)
			assert.Equal(t, UNSUBACK, pkt.Type())

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			assert.Nil(t, err)

			err = wr.Flush()
			assert.Nil(t, err)

			assert.Equal(t, tc.msg, buf.Bytes())
		})
	}
}

func BenchmarkUnsubAckWriteV3(b *testing.B) {
	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)
	pkt := NewUnsubAck(4 /*id*/, MQTT311, nil /*code*/, nil /*props*/)

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
	pkt := NewUnsubAck(4 /*id*/, MQTT50, []ReasonCode{0, 17, 128}, nil /*props*/)

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

	pkt := NewUnsubAck(5 /*id*/, MQTT50, []ReasonCode{0, 17, 128}, props)
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

	pkt := NewUnsubAck(5 /*id*/, MQTT50, []ReasonCode{0, 17, 128}, props)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	wr := bufio.NewWriter(buf)

	err := pkt.Write(wr)
	assert.NotNil(t, err)

	err = wr.Flush()
	assert.Nil(t, err)
	assert.Empty(t, buf)
}

func TestUnsubAckWriteFailure(t *testing.T) {
	pkt := NewUnsubAck(5 /*id*/, MQTT50, []ReasonCode{1, 0, 2}, nil /*props*/)
	require.NotNil(t, pkt)

	conn, _ := net.Pipe()
	w := bufio.NewWriterSize(conn, 1)
	_ = conn.Close()

	err := pkt.Write(w)
	assert.NotNil(t, err)
}

func TestUnsubAckReadUnsupported(t *testing.T) {
	pkt := NewUnsubAck(4 /*id*/, MQTT311, nil /*code*/, nil /*props*/)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Read(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestUnsubAckSize(t *testing.T) {
	testCases := []struct {
		name    string
		version Version
		codes   []ReasonCode
		props   *Properties
		size    int
	}{
		{name: "V3.1", version: MQTT31, size: 4},
		{name: "V3.1.1", version: MQTT311, size: 4},
		{name: "V5.0-Success", version: MQTT50, codes: []ReasonCode{0, 17, 128}, size: 8},
		{name: "V5.0-Properties", version: MQTT50, codes: []ReasonCode{0, 128},
			props: &Properties{ReasonString: []byte{'a'}}, size: 11},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pkt := NewUnsubAck(1 /*id*/, tc.version, tc.codes, tc.props)
			require.NotNil(t, pkt)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)

			assert.Equal(t, tc.size, pkt.Size())
		})
	}
}

func TestUnsubAckTimestamp(t *testing.T) {
	pkt := NewUnsubAck(4 /*id*/, MQTT50, []ReasonCode{0}, nil /*props*/)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
