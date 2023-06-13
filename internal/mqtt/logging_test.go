// Copyright 2023 The MaxMQ Authors
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
	"bytes"
	"errors"
	"testing"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/stretchr/testify/assert"
)

func TestLoggingOnPacketRead(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	np, err := h.OnPacketRead(c, p)
	assert.Equal(t, p, np)
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "TRACE")
}

func TestLoggingOnPacketSent(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnPacketSent(c, p, nil)
	assert.Contains(t, out.String(), "TRACE")
}

func TestLoggingOnPacketProcessed(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnPacketProcessed(c, p, nil)
	assert.Contains(t, out.String(), "TRACE")
}

func TestLoggingOnPacketProcessedError(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnPacketProcessed(c, p, errors.New("failed"))
	assert.Contains(t, out.String(), "WARN")
}

func TestLoggingOnConnect(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnConnect(c, p)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnSessionEstablished(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}
	c.State.Inflight = mqtt.NewInflights()
	c.State.Subscriptions = mqtt.NewSubscriptions()

	h.OnSessionEstablished(c, p)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnDisconnect(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}

	h.OnDisconnect(c, nil, false)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnSubscribe(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnSubscribe(c, p)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnSubscribed(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}
	p.Filters = append(p.Filters, packets.Subscription{Filter: "test"})

	h.OnSubscribed(c, p, []byte{0})
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnUnsubscribe(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}
	p.Filters = append(p.Filters, packets.Subscription{Filter: "test"})

	h.OnUnsubscribe(c, p)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnUnsubscribed(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}
	p.Filters = append(p.Filters, packets.Subscription{Filter: "test"})

	h.OnUnsubscribed(c, p)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnPublish(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	np, err := h.OnPublish(c, p)
	assert.Equal(t, p, np)
	assert.Nil(t, err)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnPublished(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnPublished(c, p)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnQosPublish(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnQosPublish(c, p, 0, 0)
	assert.Contains(t, out.String(), "TRACE")
}

func TestLoggingOnQosComplete(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnQosComplete(c, p)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnPublishDropped(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnPublishDropped(c, p)
	assert.Contains(t, out.String(), "WARN")
}

func TestLoggingOnRetainMessage(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnRetainMessage(c, p, 0)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnQosDropped(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnQosDropped(c, p)
	assert.Contains(t, out.String(), "DEBUG")
}

func TestLoggingOnWillSent(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}
	p := packets.Packet{}

	h.OnWillSent(c, p)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnClientExpired(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)
	c := &mqtt.Client{}

	h.OnClientExpired(c)
	assert.Contains(t, out.String(), "INFO")
}

func TestLoggingOnRetainedExpired(t *testing.T) {
	out := bytes.NewBufferString("")
	log := logger.New(out, nil, logger.Pretty)

	h := newLoggingHook(log)

	h.OnRetainedExpired("test")
	assert.Contains(t, out.String(), "INFO")
}
