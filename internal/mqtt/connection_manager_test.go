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

package mqtt

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type packetHandlerMock struct {
	mock.Mock
}

func (h *packetHandlerMock) handlePacket(id packet.ClientID, p packet.Packet) (replies []packet.Packet, err error) {
	args := h.Called(id, p)
	if len(args) > 0 && args.Get(0) != nil {
		replies = args.Get(0).([]packet.Packet)
	}
	if len(args) > 1 {
		err = args.Error(1)
	}
	return replies, err
}

type packetReaderMock struct {
	mock.Mock
}

func (r *packetReaderMock) ReadPacket(rd io.Reader, v packet.Version) (p packet.Packet, err error) {
	args := r.Called(rd, v)
	if len(args) > 0 && args.Get(0) != nil {
		p = args.Get(0).(packet.Packet)
	}
	if len(args) > 1 {
		err = args.Error(1)
	}
	return p, err
}

type packetWriterMock struct {
	mock.Mock
}

func (w *packetWriterMock) WritePacket(wr io.Writer, p packet.Packet) error {
	args := w.Called(wr, p)
	if len(args) > 0 {
		return args.Error(0)
	}
	return nil
}

func newConfiguration() Config {
	return Config{
		TCPAddress:                    ":1883",
		ConnectTimeout:                5,
		BufferSize:                    1024,
		DefaultVersion:                4,
		MaxPacketSize:                 268435456,
		MaxKeepAlive:                  0,
		MaxSessionExpiryInterval:      0,
		MaxInflightMessages:           0,
		MaxInflightRetries:            0,
		MaximumQoS:                    2,
		MaxTopicAlias:                 0,
		RetainAvailable:               true,
		WildcardSubscriptionAvailable: true,
		SubscriptionIDAvailable:       true,
		SharedSubscriptionAvailable:   true,
		MaxClientIDLen:                65535,
		AllowEmptyClientID:            true,
		UserProperties:                map[string]string{},
		MetricsEnabled:                true,
	}
}

func TestConnectionManagerDefaultConfig(t *testing.T) {
	conf := newConfiguration()
	conf.BufferSize = 0
	conf.MaxPacketSize = 0
	conf.ConnectTimeout = 0
	conf.DefaultVersion = 0
	conf.MaximumQoS = 3
	conf.MaxTopicAlias = 1000000
	conf.MaxInflightMessages = 1000000
	conf.MaxInflightRetries = 1000000
	conf.MaxClientIDLen = 0

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	assert.Equal(t, 1024, cm.conf.BufferSize)
	assert.Equal(t, 268435456, cm.conf.MaxPacketSize)
	assert.Equal(t, 5, cm.conf.ConnectTimeout)
	assert.Equal(t, 4, cm.conf.DefaultVersion)
	assert.Equal(t, 2, cm.conf.MaximumQoS)
	assert.Equal(t, 0, cm.conf.MaxTopicAlias)
	assert.Equal(t, 0, cm.conf.MaxInflightMessages)
	assert.Equal(t, 0, cm.conf.MaxInflightRetries)
	assert.Equal(t, 23, cm.conf.MaxClientIDLen)
}

func TestConnectionManagerServeConnectionPacketConnect(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	rd.On("ReadPacket", c2, packet.MQTT311).Return(p).Once()
	hd.On("handlePacket", packet.ClientID(""), p).Return(replies)
	wr.On("WritePacket", c2, replies[0])
	rd.On("ReadPacket", c2, packet.MQTT311).Return(nil, io.EOF)

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
}

func TestConnectionManagerServeConnectionPacketConnectCleanSession(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311, CleanSession: true}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	rd.On("ReadPacket", c2, packet.MQTT311).Return(p).Once()
	hd.On("handlePacket", packet.ClientID(""), p).Return(replies)
	wr.On("WritePacket", c2, replies[0])
	rd.On("ReadPacket", c2, packet.MQTT311).Return(nil, io.EOF)

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
}

func TestConnectionManagerServeConnectionPacketDisconnectReceived(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	p := &packet.Disconnect{}
	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	cID := packet.ClientID("client-0")
	rd.On("ReadPacket", c2, packet.MQTT311).Return(p)
	hd.On("handlePacket", cID, p)

	c := cm.newConnection(c2)
	c.clientID = cID
	c.setState(stateIdle)
	c.hasSession = true
	cm.connections.Value[cID] = c
	cm.serveConnection(c)

	assert.False(t, c.hasSession)
	assert.Equal(t, stateClosed, c.state())
	assert.NotContains(t, cm.connections.Value, cID)
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
}

func TestConnectionManagerServeConnectionPacketDisconnectReplied(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	p := &packet.Subscribe{}
	replies := []packet.Packet{&packet.Disconnect{}}
	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	cID := packet.ClientID("client-0")
	rd.On("ReadPacket", c2, packet.MQTT311).Return(p)
	hd.On("handlePacket", cID, p).Return(replies)
	wr.On("WritePacket", c2, replies[0])

	c := cm.newConnection(c2)
	c.clientID = cID
	c.setState(stateIdle)
	c.hasSession = true
	cm.connections.Value[cID] = c
	cm.serveConnection(c)

	assert.False(t, c.hasSession)
	assert.Equal(t, stateClosed, c.state())
	assert.NotContains(t, cm.connections.Value, cID)
	rd.AssertExpectations(t)
	wr.AssertExpectations(t)
	hd.AssertExpectations(t)
}

func TestConnectionManagerServeConnection(t *testing.T) {
	testCases := []struct {
		p     packet.Packet
		reply packet.Packet
	}{
		{&packet.PingReq{}, &packet.PingResp{}},
		{&packet.Subscribe{}, &packet.SubAck{}},
		{&packet.Unsubscribe{}, &packet.UnsubAck{}},
		{&packet.Publish{}, &packet.PubAck{}},
		{&packet.Publish{}, &packet.PubRec{}},
		{&packet.PubRel{}, &packet.PubComp{}},
	}

	for _, tc := range testCases {
		name := fmt.Sprintf("%v-%v", tc.p.Type().String(), tc.reply.Type().String())
		t.Run(name, func(t *testing.T) {
			conf := newConfiguration()
			gen := newIDGeneratorMock()
			log := newLogger()
			ss := newSessionStore(gen, log)
			mt := newMetrics(conf.MetricsEnabled, log)

			cm := newConnectionManager(conf, ss, mt, gen, log)
			hd := &packetHandlerMock{}
			rd := &packetReaderMock{}
			wr := &packetWriterMock{}

			c1, c2 := net.Pipe()
			defer func() { _ = c1.Close() }()

			replies := []packet.Packet{tc.reply}
			cm.handlers[tc.p.Type()] = hd
			cm.reader = rd
			cm.writer = wr

			cID := packet.ClientID("client-0")
			rd.On("ReadPacket", c2, packet.MQTT311).Return(tc.p).Once()
			hd.On("handlePacket", cID, tc.p).Return(replies)
			wr.On("WritePacket", c2, replies[0])
			rd.On("ReadPacket", c2, packet.MQTT311).Return(nil, io.EOF)

			c := cm.newConnection(c2)
			c.clientID = cID
			c.setState(stateIdle)
			c.hasSession = true
			cm.connections.Value[cID] = c
			cm.serveConnection(c)

			rd.AssertExpectations(t)
			wr.AssertExpectations(t)
			hd.AssertExpectations(t)
		})
	}
}

func TestConnectionManagerServeConnectionReadFailure(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	rd := &packetReaderMock{}
	cm.reader = rd
	rd.On("ReadPacket", c2, packet.MQTT311).Return(nil, errors.New("failed"))

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	assert.Equal(t, stateClosed, c.state())
	assert.Empty(t, cm.pendingConnections.Value)
	rd.AssertExpectations(t)
}

func TestConnectionManagerServeConnectionSetDeadlineFailure(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)

	c1, c2 := net.Pipe()
	_ = c1.Close()

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)

	// It is expected to run and return immediately
	cm.serveConnection(c)

	assert.Equal(t, stateClosed, c.state())
}

func TestConnectionManagerServeConnectionReadTimeout(t *testing.T) {
	conf := newConfiguration()
	conf.ConnectTimeout = 1

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	assert.Equal(t, stateClosed, c.state())
}

func TestConnectionManagerServeConnectionWritePacketFailure(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	rd.On("ReadPacket", c2, packet.MQTT311).Return(p)
	hd.On("handlePacket", packet.ClientID(""), p).Return(replies)
	wr.On("WritePacket", c2, replies[0]).Return(errors.New("failed"))

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	assert.Equal(t, stateClosed, c.state())
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
}

func TestConnectionManagerServeConnectionInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	rd := &packetReaderMock{}
	cm.reader = rd

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	p := &packet.PingResp{}
	rd.On("ReadPacket", c2, packet.MQTT311).Return(p)

	c := cm.newConnection(c2)
	cm.addPendingConnection(c)
	cm.serveConnection(c)

	assert.Equal(t, stateClosed, c.state())
	rd.AssertExpectations(t)
}

func TestConnectionManagerDeliverMessage(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	wr := &packetWriterMock{}
	cm.writer = wr

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	cID := packet.ClientID("client-0")
	c := cm.newConnection(c2)
	c.clientID = cID
	c.setState(stateIdle)
	c.hasSession = true
	cm.connections.Value[cID] = c

	p := packet.NewPublish(10, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	wr.On("WritePacket", c2, &p)

	err := cm.deliverPacket(cID, &p)
	assert.Nil(t, err)
	wr.AssertExpectations(t)
}

func TestConnectionManagerDeliverMessageConnectionNotFound(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	p := packet.NewPublish(10, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)

	err := cm.deliverPacket("client-b", &p)
	assert.NotNil(t, err)
}

func TestConnectionManagerDeliverMessageWriteFailure(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	gen := newIDGeneratorMock()
	log := newLogger()
	ss := newSessionStore(gen, log)
	mt := newMetrics(conf.MetricsEnabled, log)

	cm := newConnectionManager(conf, ss, mt, gen, log)
	wr := &packetWriterMock{}
	cm.writer = wr

	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()

	cID := packet.ClientID("client-0")
	c := cm.newConnection(c2)
	c.clientID = cID
	c.setState(stateIdle)
	c.hasSession = true
	cm.connections.Value[cID] = c

	p := packet.NewPublish(10, packet.MQTT311, "t", packet.QoS0, 0, 0, nil, nil)
	wr.On("WritePacket", c2, &p).Return(errors.New("failed"))

	err := cm.deliverPacket(cID, &p)
	assert.NotNil(t, err)
	wr.AssertExpectations(t)
}
