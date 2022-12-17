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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnAckWrite(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		code    ReasonCode
		msg     []byte
	}{
		{name: "V3.1", version: MQTT31, code: ReasonCodeV3ConnectionAccepted,
			msg: []byte{0x20, 2, 0, 0}},
		{name: "V3.1.1", version: MQTT311, code: ReasonCodeV3NotAuthorized,
			msg: []byte{0x20, 2, 0, 5}},
		{name: "V5.0-Success", version: MQTT50, code: ReasonCodeV5Success,
			msg: []byte{0x20, 3, 0, 0, 0}},
		{name: "V5.0-Error", version: MQTT50,
			code: ReasonCodeV5UnspecifiedError,
			msg:  []byte{0x20, 3, 0, 0x80, 0}},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewConnAck(test.version, test.code, false,
				nil)
			assert.Equal(t, CONNACK, pkt.Type())

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

func BenchmarkConnAckWriteV3(b *testing.B) {
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

func BenchmarkConnAckWriteV5(b *testing.B) {
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

func TestConnAckWriteSessionPresent(t *testing.T) {
	testCases := []struct {
		version MQTTVersion
		value   bool
		msg     []byte
	}{
		{version: MQTT31, value: false, msg: []byte{0x20, 2, 0, 0}},
		{version: MQTT31, value: true, msg: []byte{0x20, 2, 0, 0}},
		{version: MQTT311, value: false, msg: []byte{0x20, 2, 0, 0}},
		{version: MQTT311, value: true, msg: []byte{0x20, 2, 1, 0}},
		{version: MQTT50, value: false, msg: []byte{0x20, 3, 0, 0, 0}},
		{version: MQTT50, value: true, msg: []byte{0x20, 3, 1, 0, 0}},
	}

	for _, test := range testCases {
		t.Run(test.version.String(), func(t *testing.T) {
			pkt := NewConnAck(test.version, ReasonCodeV3ConnectionAccepted,
				test.value, nil)
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

func TestConnAckWriteV5Properties(t *testing.T) {
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

func TestConnAckWriteV5InvalidProperty(t *testing.T) {
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

func TestConnAckWriteV3PropertiesIgnored(t *testing.T) {
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

func TestConnAckWriteFailure(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)

	conn, _ := net.Pipe()
	w := bufio.NewWriterSize(conn, 1)
	_ = conn.Close()

	err := pkt.Write(w)
	assert.NotNil(t, err)
}

func TestConnAckReadUnsupported(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)

	buf := &bytes.Buffer{}
	err := pkt.Read(bufio.NewReader(buf))
	require.NotNil(t, err)
}

func TestConnAckSize(t *testing.T) {
	testCases := []struct {
		name    string
		version MQTTVersion
		code    ReasonCode
		props   *Properties
		size    int
	}{
		{name: "V3.1", version: MQTT31, code: ReasonCodeV3ConnectionAccepted,
			size: 4},
		{name: "V3.1.1", version: MQTT311, code: ReasonCodeV3ConnectionAccepted,
			size: 4},
		{name: "V5.0-Success", version: MQTT50, code: ReasonCodeV5Success,
			size: 5},
		{name: "V5.0-Properties", version: MQTT50,
			props: &Properties{ReasonString: []byte{'a'}},
			code:  ReasonCodeV5Success, size: 9},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			pkt := NewConnAck(test.version, test.code, false, test.props)
			require.NotNil(t, pkt)

			buf := &bytes.Buffer{}
			wr := bufio.NewWriter(buf)

			err := pkt.Write(wr)
			require.Nil(t, err)

			assert.Equal(t, test.size, pkt.Size())
		})
	}
}

func TestConnAckTimestamp(t *testing.T) {
	pkt := NewConnAck(MQTT311, ReasonCodeV3ConnectionAccepted, false, nil)
	require.NotNil(t, pkt)
	assert.NotNil(t, pkt.Timestamp())
}
