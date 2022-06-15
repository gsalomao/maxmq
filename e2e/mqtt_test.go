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

//go:build e2e
// +build e2e

package e2e

import (
	"testing"
	"time"

	"github.com/gsalomao/maxmq/broker"
	"github.com/gsalomao/maxmq/mocks"
	"github.com/gsalomao/maxmq/mqtt"
	"github.com/gsalomao/maxmq/mqtt/store"
	"github.com/stretchr/testify/assert"

	paho "github.com/eclipse/paho.mqtt.golang"
)

func newBroker() *broker.Broker {
	conf := mqtt.Configuration{TCPAddress: ":1883"}
	logStub := mocks.NewLoggerStub()
	st := store.NewMemoryStore()

	l, _ := mqtt.NewListener(
		mqtt.WithConfiguration(conf),
		mqtt.WithSessionStore(st),
		mqtt.WithLogger(logStub.Logger()),
	)

	b := broker.New(logStub.Logger())
	b.AddListener(l)

	return &b
}

func TestMqtt_RunBroker(t *testing.T) {
	b := newBroker()
	assert.Nil(t, b.Start())
	<-time.After(100 * time.Millisecond)

	stopped := make(chan struct{})
	go func() {
		defer func() { stopped <- struct{}{} }()

		<-time.After(1 * time.Millisecond)
		b.Stop()
	}()

	assert.Nil(t, b.Wait())
	<-stopped
}

func TestMqtt_Connect(t *testing.T) {
	b := newBroker()
	assert.Nil(t, b.Start())
	defer func() { b.Stop() }()
	<-time.After(100 * time.Millisecond)

	opts := paho.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("paho-client")
	c := paho.NewClient(opts)

	token := c.Connect()
	assert.True(t, token.WaitTimeout(100*time.Millisecond))
	assert.Nil(t, token.Error())
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())
}

func TestMqtt_Disconnect(t *testing.T) {
	b := newBroker()
	assert.Nil(t, b.Start())
	defer func() { b.Stop() }()
	<-time.After(100 * time.Millisecond)

	opts := paho.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("paho-client")
	c := paho.NewClient(opts)

	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())
	<-time.After(100 * time.Millisecond)

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
	assert.False(t, c.IsConnected())
}

func TestMqtt_PingReq(t *testing.T) {
	b := newBroker()
	assert.Nil(t, b.Start())
	defer func() { b.Stop() }()
	<-time.After(100 * time.Millisecond)

	opts := paho.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID("paho-client")
	opts.SetKeepAlive(2 * time.Second)
	opts.SetConnectTimeout(1 * time.Second)
	opts.SetPingTimeout(1 * time.Second)
	opts.SetAutoReconnect(false)
	c := paho.NewClient(opts)

	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())

	// Wait enough time to client sends PINGREQ packet
	<-time.After(5 * time.Second)
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())
}
