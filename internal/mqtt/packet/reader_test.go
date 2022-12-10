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
		defer func() {
			done <- true
		}()

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		reader := NewReader(opts)

		pkt, err := reader.ReadPacket(sConn, 0)
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
	reader := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := reader.ReadPacket(r, 0)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderReadPacketDisconnect(b *testing.B) {
	msg := []byte{0xE0, 0}
	r := bytes.NewReader(msg)
	opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
	reader := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := reader.ReadPacket(r, MQTT311)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReaderReadPacketPingReq(b *testing.B) {
	msg := []byte{0xC0, 0}
	r := bytes.NewReader(msg)
	opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
	reader := NewReader(opts)

	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		r.Reset(msg)
		_, err := reader.ReadPacket(r, MQTT31)

		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestPacketReadPacketBiggerThanMaxPacketSize(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 2}
		reader := NewReader(opts)

		_, err := reader.ReadPacket(sConn, MQTT50)
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
		defer func() {
			done <- true
		}()

		opts := ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		reader := NewReader(opts)

		_, err := reader.ReadPacket(sConn, MQTT311)
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
		{
			pkt: []byte{0xFF, 1},
			msg: "invalid packet type",
		},
		{
			pkt: []byte{0x10},
			msg: "failed to read variable integer",
		},
		{
			pkt: []byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF},
			msg: "invalid variable integer",
		},
		{
			pkt: []byte{0x10, 3},
			msg: "failed to read remaining bytes",
		},
		{
			pkt: []byte{0x10, 0},
			msg: "failed to read protocol name",
		},
	}

	for _, test := range testCases {
		t.Run(test.msg, func(t *testing.T) {
			conn, sConn := net.Pipe()

			done := make(chan bool)
			go func() {
				defer func() {
					done <- true
				}()

				_ = sConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
				opts := ReaderOptions{BufferSize: 1024,
					MaxPacketSize: 65536}
				reader := NewReader(opts)

				_, err := reader.ReadPacket(sConn, MQTT311)
				require.NotNil(t, err)
				assert.Contains(t, err.Error(), test.msg)
			}()

			n, err := conn.Write(test.pkt)
			assert.Nil(t, err)
			assert.Equal(t, len(test.pkt), n)
			<-done
		})
	}
}
