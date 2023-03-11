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

	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type packetDelivererMock struct {
	mock.Mock
}

func (p *packetDelivererMock) deliverPacket(id packet.ClientID, pkt *packet.Publish) error {
	args := p.Called(id, pkt)
	return args.Error(0)
}

type packetHandlerMock struct {
	mock.Mock
}

func (h *packetHandlerMock) HandlePacket(
	id packet.ClientID,
	p packet.Packet,
) ([]packet.Packet, error) {
	args := h.Called(id, p)
	var replies []packet.Packet
	if args.Get(0) != nil {
		replies = args.Get(0).([]packet.Packet)
	}
	return replies, args.Error(1)
}

type packetReaderMock struct {
	mock.Mock
}

func (r *packetReaderMock) ReadPacket(rd io.Reader, v packet.Version) (packet.Packet, error) {
	args := r.Called(rd, v)
	var pkt packet.Packet
	if args.Get(0) != nil {
		pkt = args.Get(0).(packet.Packet)
	}
	return pkt, args.Error(1)
}

type packetWriterMock struct {
	mock.Mock
}

func (w *packetWriterMock) WritePacket(wr io.Writer, p packet.Packet) error {
	args := w.Called(wr, p)
	return args.Error(0)
}

func newConfiguration() handler.Configuration {
	return handler.Configuration{
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

func TestConnectionManagerNewDefaultValues(t *testing.T) {
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

	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)

	cm := newConnectionManager(&conf, st, mt, idGen, &log)
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

func TestConnectionManagerHandleConnect(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	conn, sConn := net.Pipe()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil).Once()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(nil, io.EOF)
	hd.On("HandlePacket", packet.ClientID(""), p).Return(replies, nil)
	wr.On("WritePacket", sConn, replies[0]).Return(nil)

	sc := &handler.Session{ClientID: cID, Connected: true}
	sd := &handler.Session{ClientID: cID, Connected: false}
	st.On("ReadSession", cID).Return(sc, nil)
	st.On("SaveSession", sd).Return(nil)

	c := cm.newConnection(sConn)
	cm.handle(c)

	_ = conn.Close()
	assert.Empty(t, cm.connections)
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestConnectionManagerHandleConnectCleanSession(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311, CleanSession: true}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	conn, sConn := net.Pipe()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil).Once()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(nil, io.EOF)
	hd.On("HandlePacket", packet.ClientID(""), p).Return(replies, nil)
	wr.On("WritePacket", sConn, replies[0]).Return(nil)

	s := &handler.Session{ClientID: cID, Connected: true, CleanSession: true}
	st.On("ReadSession", cID).Return(s, nil)
	st.On("DeleteSession", s).Return(nil)

	c := cm.newConnection(sConn)
	cm.handle(c)

	_ = conn.Close()
	assert.Empty(t, cm.connections)
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestConnectionManagerHandleDisconnectReceived(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	conn, sConn := net.Pipe()
	cID := packet.ClientID("client-0")
	c := cm.newConnection(sConn)
	c.clientID = cID
	c.connected = true
	c.hasSession = true
	cm.connections[cID] = &c

	p := &packet.Disconnect{}
	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil)
	hd.On("HandlePacket", cID, p).Return(nil, nil)

	cm.handle(c)
	_ = conn.Close()

	assert.Empty(t, cm.connections)
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestConnectionManagerHandleDisconnectReplied(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	conn, sConn := net.Pipe()
	cID := packet.ClientID("client-0")
	c := cm.newConnection(sConn)
	c.clientID = cID
	c.connected = true
	c.hasSession = true
	cm.connections[cID] = &c

	p := &packet.Subscribe{}
	replies := []packet.Packet{&packet.Disconnect{}}
	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil)
	hd.On("HandlePacket", cID, p).Return(replies, nil)
	wr.On("WritePacket", sConn, replies[0]).Return(nil)

	cm.handle(c)
	_ = conn.Close()

	assert.Empty(t, cm.connections)
	rd.AssertExpectations(t)
	wr.AssertExpectations(t)
	hd.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestConnectionManagerHandle(t *testing.T) {
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
		name := fmt.Sprintf("%v-%v", tc.p.Type().String(),
			tc.reply.Type().String())

		t.Run(name, func(t *testing.T) {
			conf := newConfiguration()
			st := &sessionStoreMock{}
			log := newLogger()
			idGen := &idGeneratorMock{}
			mt := newMetrics(conf.MetricsEnabled, &log)
			cm := newConnectionManager(&conf, st, mt, idGen, &log)

			hd := &packetHandlerMock{}
			rd := &packetReaderMock{}
			wr := &packetWriterMock{}

			conn, sConn := net.Pipe()
			cID := packet.ClientID("client-0")
			c := cm.newConnection(sConn)
			c.clientID = cID
			c.connected = true
			c.hasSession = true
			cm.connections[cID] = &c

			replies := []packet.Packet{tc.reply}
			cm.handlers[tc.p.Type()] = hd
			cm.reader = rd
			cm.writer = wr

			rd.On("ReadPacket", sConn, packet.MQTT311).Return(tc.p, nil).Once()
			rd.On("ReadPacket", sConn, packet.MQTT311).Return(nil, io.EOF)
			hd.On("HandlePacket", cID, tc.p).Return(replies, nil)
			wr.On("WritePacket", sConn, replies[0]).Return(nil)

			s := &handler.Session{ClientID: cID, Connected: true}
			st.On("ReadSession", cID).Return(s, nil)
			st.On("SaveSession", s).Return(nil)

			cm.handle(c)
			_ = conn.Close()

			assert.Empty(t, cm.connections)
			rd.AssertExpectations(t)
			wr.AssertExpectations(t)
			hd.AssertExpectations(t)
			st.AssertExpectations(t)
		})
	}

}

func TestConnectionManagerHandleSetDeadlineFailure(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	conn, sConn := net.Pipe()
	_ = conn.Close()

	c := cm.newConnection(sConn)
	cm.handle(c) // It is expected to run and return immediately
}

func TestConnectionManagerHandleReadFailure(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	conn, sConn := net.Pipe()
	c := cm.newConnection(sConn)

	rd := &packetReaderMock{}
	cm.reader = rd
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(nil, errors.New("failed"))

	cm.handle(c)
	_ = conn.Close()
	rd.AssertExpectations(t)
}

func TestConnectionManagerHandleReadTimeout(t *testing.T) {
	conf := newConfiguration()
	conf.ConnectTimeout = 1

	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	conn, sConn := net.Pipe()
	c := cm.newConnection(sConn)

	cm.handle(c)
	_ = conn.Close()
}

func TestConnectionManagerHandleWritePacketFailure(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	hd := &packetHandlerMock{}
	rd := &packetReaderMock{}
	wr := &packetWriterMock{}

	cID := packet.ClientID("client-0")
	p := &packet.Connect{ClientID: []byte(cID), Version: packet.MQTT311}
	replies := []packet.Packet{&packet.ConnAck{ClientID: cID, Version: packet.MQTT311}}

	cm.handlers[p.Type()] = hd
	cm.reader = rd
	cm.writer = wr

	conn, sConn := net.Pipe()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil)
	hd.On("HandlePacket", packet.ClientID(""), p).Return(replies, nil)
	wr.On("WritePacket", sConn, replies[0]).Return(errors.New("failed"))

	sc := &handler.Session{ClientID: cID, Connected: true}
	sd := &handler.Session{ClientID: cID, Connected: false}
	st.On("ReadSession", cID).Return(sc, nil)
	st.On("SaveSession", sd).Return(nil)

	c := cm.newConnection(sConn)
	cm.handle(c)

	_ = conn.Close()
	rd.AssertExpectations(t)
	hd.AssertExpectations(t)
	wr.AssertExpectations(t)
	st.AssertExpectations(t)
}

func TestConnectionManagerHandleInvalidPacket(t *testing.T) {
	conf := newConfiguration()
	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	rd := &packetReaderMock{}
	cm.reader = rd

	p := &packet.PingResp{}
	conn, sConn := net.Pipe()
	rd.On("ReadPacket", sConn, packet.MQTT311).Return(p, nil)

	c := cm.newConnection(sConn)
	cm.handle(c)

	_ = conn.Close()
	assert.Empty(t, cm.connections)
	rd.AssertExpectations(t)
}

func TestConnectionManagerDeliverMessageWriteFailure(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	conn, sConn := net.Pipe()
	c := cm.newConnection(sConn)
	cm.connections["client-a"] = &c

	wr := &packetWriterMock{}
	cm.writer = wr

	p := packet.NewPublish(
		10, /*id*/
		packet.MQTT311,
		"data", /*topic*/
		packet.QoS0,
		0,   /*dup*/
		0,   /*retain*/
		nil, /*payload*/
		nil, /*props*/
	)

	wr.On("WritePacket", sConn, &p).Return(errors.New("failed"))

	err := cm.deliverPacket("client-a", &p)
	assert.NotNil(t, err)
	_ = conn.Close()
	wr.AssertExpectations(t)
}

func TestConnectionManagerDeliverMessage(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	conn, sConn := net.Pipe()
	c := cm.newConnection(sConn)
	cm.connections["client-a"] = &c

	wr := &packetWriterMock{}
	cm.writer = wr

	p := packet.NewPublish(
		10, /*id*/
		packet.MQTT311,
		"data", /*topic*/
		packet.QoS0,
		0,   /*dup*/
		0,   /*retain*/
		nil, /*payload*/
		nil, /*props*/
	)

	wr.On("WritePacket", sConn, &p).Return(nil)

	err := cm.deliverPacket("client-a", &p)
	assert.Nil(t, err)
	_ = conn.Close()
	wr.AssertExpectations(t)
}

func TestConnectionManagerDeliverMessageConnectionNotFound(t *testing.T) {
	conf := newConfiguration()
	conf.MetricsEnabled = false

	st := &sessionStoreMock{}
	log := newLogger()
	idGen := &idGeneratorMock{}
	mt := newMetrics(conf.MetricsEnabled, &log)
	cm := newConnectionManager(&conf, st, mt, idGen, &log)

	p := packet.NewPublish(
		10, /*id*/
		packet.MQTT311,
		"data", /*topic*/
		packet.QoS0,
		0,   /*dup*/
		0,   /*retain*/
		nil, /*payload*/
		nil, /*props*/
	)

	err := cm.deliverPacket("client-b", &p)
	assert.NotNil(t, err)
}
