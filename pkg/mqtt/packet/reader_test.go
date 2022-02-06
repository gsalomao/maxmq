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

package packet_test

import (
	"net"
	"testing"
	"time"

	"github.com/gsalomao/maxmq/pkg/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPacket_ReadPacket(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		opts := packet.ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		reader := packet.NewReader(sConn, opts)

		pkt, err := reader.ReadPacket()
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

func TestPacket_ReadPacketBiggerThanMaxPacketSize(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		opts := packet.ReaderOptions{BufferSize: 1024, MaxPacketSize: 2}
		reader := packet.NewReader(sConn, opts)

		_, err := reader.ReadPacket()
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

func TestPacket_ReadPacketError(t *testing.T) {
	conn, sConn := net.Pipe()

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		opts := packet.ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
		reader := packet.NewReader(sConn, opts)

		_, err := reader.ReadPacket()
		require.NotNil(t, err)
	}()

	conn.Close()
	<-done
}

func TestPacket_ReadPacketInvalid(t *testing.T) {
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
			msg: "invalid variable integer",
		},
		{
			pkt: []byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF},
			msg: "invalid variable integer",
		},
		{
			pkt: []byte{0x10, 3},
			msg: "missing data",
		},
		{
			pkt: []byte{0x10, 0},
			msg: "cannot decode protocol name",
		},
	}

	for _, test := range testCases {
		conn, sConn := net.Pipe()

		done := make(chan bool)
		go func() {
			defer func() {
				done <- true
			}()

			_ = sConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			opts := packet.ReaderOptions{BufferSize: 1024, MaxPacketSize: 65536}
			reader := packet.NewReader(sConn, opts)

			_, err := reader.ReadPacket()
			require.NotNil(t, err)
			assert.Contains(t, err.Error(), test.msg)
		}()

		n, err := conn.Write(test.pkt)
		assert.Nil(t, err)
		assert.Equal(t, len(test.pkt), n)
		<-done
	}
}
