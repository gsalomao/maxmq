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
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacketReadPacket(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer close(done)

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		rd := NewReader(opts)

		pkt, err := rd.ReadPacket(sConn, 0)
		assert.Nil(t, err)
		assert.NotNil(t, pkt)
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)
	<-done
}

func BenchmarkReaderReadPacketConnect(b *testing.B) {
	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}
	r := bytes.NewReader(msg)
	opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
	rd := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := rd.ReadPacket(r, 0)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderReadPacketDisconnect(b *testing.B) {
	msg := []byte{0xE0, 0}
	r := bytes.NewReader(msg)
	opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
	rd := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := rd.ReadPacket(r, MQTT311)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderReadPacketPingReq(b *testing.B) {
	msg := []byte{0xC0, 0}
	r := bytes.NewReader(msg)
	opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
	rd := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := rd.ReadPacket(r, MQTT31)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPacketReadPacketBiggerThanMaxPacketSize(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer close(done)

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 2}
		rd := NewReader(opts)

		_, err := rd.ReadPacket(sConn, MQTT50)
		require.NotNil(t, err)
	}()

	msg := []byte{
		0x10, 13, // fixed header
		0, 4, 'M', 'Q', 'T', 'T', 4, 0, 0, 0, // variable header
		0, 1, 'a', // client ID
	}

	_, err := conn.Write(msg)
	require.Nil(t, err)
	<-done
}

func TestPacketReadPacketError(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer close(done)

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		rd := NewReader(opts)

		_, err := rd.ReadPacket(sConn, MQTT311)
		require.NotNil(t, err)
	}()

	_ = conn.Close()
	<-done
}

func TestPacketReadPacketInvalid(t *testing.T) {
	testCases := []struct {
		pkt []byte
		msg string
	}{
		{[]byte{0xFF, 1}, "invalid packet type"},
		{[]byte{0x10}, "failed to read variable integer"},
		{[]byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF}, "invalid variable integer"},
		{[]byte{0x10, 3}, "failed to read remaining bytes"},
		{[]byte{0x10, 0}, "failed to read protocol name"},
	}

	for _, tc := range testCases {
		t.Run(tc.msg, func(t *testing.T) {
			conn, sConn := net.Pipe()

			done := make(chan bool)
			go func() {
				defer close(done)

				_ = sConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
				rd := NewReader(opts)

				_, err := rd.ReadPacket(sConn, MQTT311)
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), tc.msg)
			}()

			n, err := conn.Write(tc.pkt)
			assert.Nil(t, err)
			assert.Equal(t, len(tc.pkt), n)
			<-done
		})
	}
}
