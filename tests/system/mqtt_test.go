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

package system

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	paho "github.com/eclipse/paho.mqtt.golang"
)

func newMQTTClient(clientID string, clean bool) paho.Client {
	opts := paho.NewClientOptions()
	opts.AddBroker("tcp://localhost:1883")
	opts.SetClientID(clientID)
	opts.SetCleanSession(clean)
	return paho.NewClient(opts)
}

func TestMQTTConnect(t *testing.T) {
	c := newMQTTClient("paho-client", true)
	token := c.Connect()
	assert.True(t, token.WaitTimeout(100*time.Millisecond))
	assert.Nil(t, token.Error())
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTDisconnect(t *testing.T) {
	c := newMQTTClient("paho-client", true)
	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, c.IsConnected())
	require.True(t, c.IsConnectionOpen())
	<-time.After(100 * time.Millisecond)

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
	assert.False(t, c.IsConnected())
}

func TestMQTTPingReq(t *testing.T) {
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
	require.True(t, c.IsConnected())
	require.True(t, c.IsConnectionOpen())

	// Wait enough time to client sends PINGREQ packet
	<-time.After(5 * time.Second)
	assert.True(t, c.IsConnected())
	assert.True(t, c.IsConnectionOpen())

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTSubscribe(t *testing.T) {
	c := newMQTTClient("paho-client", true)
	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, c.IsConnected())
	require.True(t, c.IsConnectionOpen())

	topicFilters := make(map[string]byte)
	topicFilters["temp"] = 0
	topicFilters["sensor/#"] = 1
	topicFilters["data/+/raw"] = 2

	token = c.SubscribeMultiple(topicFilters, nil)
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	subToken := token.(*paho.SubscribeToken)
	assert.Nil(t, subToken.Error())

	results := subToken.Result()
	require.Len(t, results, 3)
	assert.Equal(t, byte(0), results["temp"])
	assert.Equal(t, byte(1), results["sensor/#"])
	assert.Equal(t, byte(2), results["data/+/raw"])

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTUnsubscribe(t *testing.T) {
	c := newMQTTClient("paho-client", true)
	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, c.IsConnected())
	require.True(t, c.IsConnectionOpen())

	topicFilters := make(map[string]byte)
	topicFilters["temp"] = 0
	topicFilters["sensor/#"] = 1
	topicFilters["data/+/raw"] = 2

	token = c.SubscribeMultiple(topicFilters, nil)
	require.True(t, token.WaitTimeout(100*time.Millisecond))
	require.Nil(t, token.Error())

	token = c.Unsubscribe("temp")
	require.True(t, token.WaitTimeout(100*time.Millisecond))
	require.Nil(t, token.Error())

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTPublishQoS0(t *testing.T) {
	c := newMQTTClient("paho-client", true)
	token := c.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, c.IsConnected())
	require.True(t, c.IsConnectionOpen())

	received := make(chan paho.Message)
	token = c.Subscribe("data/#", 0,
		func(c paho.Client, msg paho.Message) {
			received <- msg
		},
	)
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	token = c.Publish("data/1", 0, false, []byte("hello"))
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	msg := <-received
	assert.Equal(t, "data/1", msg.Topic())
	assert.Equal(t, uint8(0), msg.Qos())
	assert.Equal(t, []byte("hello"), msg.Payload())

	c.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTPublishQoS1(t *testing.T) {
	pub := newMQTTClient("paho-pub", true)
	token := pub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, pub.IsConnected())
	require.True(t, pub.IsConnectionOpen())

	sub := newMQTTClient("paho-sub", false)
	token = sub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, sub.IsConnected())
	require.True(t, sub.IsConnectionOpen())

	received := make(chan paho.Message)
	token = sub.Subscribe("data/#", 1,
		func(c paho.Client, msg paho.Message) {
			received <- msg
		},
	)
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	token = pub.Publish("data/1", 1, false, []byte("msg-1"))
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	msg := <-received
	assert.Equal(t, uint8(1), msg.Qos())
	assert.Equal(t, []byte("msg-1"), msg.Payload())
	<-time.After(100 * time.Millisecond)

	sub.Disconnect(1)
	<-time.After(100 * time.Millisecond)

	token = pub.Publish("data/2", 1, false, []byte("msg-2"))
	assert.True(t, token.WaitTimeout(100*time.Millisecond))
	<-time.After(100 * time.Millisecond)

	pub.Disconnect(1)
	<-time.After(100 * time.Millisecond)

	token = sub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	<-time.After(100 * time.Millisecond)

	msg = <-received
	assert.Equal(t, uint8(1), msg.Qos())
	assert.Equal(t, []byte("msg-2"), msg.Payload())
	<-time.After(100 * time.Millisecond)

	sub.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}

func TestMQTTPublishQoS2(t *testing.T) {
	pub := newMQTTClient("paho-pub", true)
	token := pub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, pub.IsConnected())
	require.True(t, pub.IsConnectionOpen())

	sub := newMQTTClient("paho-sub", false)
	token = sub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	require.True(t, sub.IsConnected())
	require.True(t, sub.IsConnectionOpen())

	received := make(chan paho.Message)
	token = sub.Subscribe("data/#", 2,
		func(c paho.Client, msg paho.Message) {
			received <- msg
		},
	)
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	token = pub.Publish("data/1", 2, false, []byte("msg-1"))
	assert.True(t, token.WaitTimeout(100*time.Millisecond))

	msg := <-received
	assert.Equal(t, uint8(2), msg.Qos())
	assert.Equal(t, []byte("msg-1"), msg.Payload())
	<-time.After(100 * time.Millisecond)

	sub.Disconnect(1)
	<-time.After(100 * time.Millisecond)

	token = pub.Publish("data/2", 2, false, []byte("msg-2"))
	assert.True(t, token.WaitTimeout(100*time.Millisecond))
	<-time.After(100 * time.Millisecond)

	pub.Disconnect(1)
	<-time.After(100 * time.Millisecond)

	token = sub.Connect()
	token.WaitTimeout(100 * time.Millisecond)
	<-time.After(100 * time.Millisecond)

	msg = <-received
	assert.Equal(t, uint8(2), msg.Qos())
	assert.Equal(t, []byte("msg-2"), msg.Payload())
	<-time.After(100 * time.Millisecond)

	sub.Disconnect(1)
	<-time.After(100 * time.Millisecond)
}
