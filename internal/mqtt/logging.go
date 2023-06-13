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
	"strings"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/mochi-co/mqtt/v2"
	"github.com/mochi-co/mqtt/v2/packets"
	"github.com/rs/zerolog"
)

type loggingHook struct {
	log *logger.Logger
	mqtt.HookBase
}

func newLoggingHook(l *logger.Logger) *loggingHook {
	h := &loggingHook{log: l}
	return h
}

// ID returns the ID of the hook.
func (h *loggingHook) ID() string {
	return "logging"
}

// Provides indicates which hook methods this hook provides.
func (h *loggingHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnPacketRead,
		mqtt.OnPacketSent,
		mqtt.OnPacketProcessed,
		mqtt.OnConnect,
		mqtt.OnSessionEstablished,
		mqtt.OnDisconnect,
		mqtt.OnSubscribe,
		mqtt.OnSubscribed,
		mqtt.OnUnsubscribe,
		mqtt.OnUnsubscribed,
		mqtt.OnPublish,
		mqtt.OnPublished,
		mqtt.OnQosPublish,
		mqtt.OnQosComplete,
		mqtt.OnPublishDropped,
		mqtt.OnRetainMessage,
		mqtt.OnQosDropped,
		mqtt.OnWillSent,
		mqtt.OnClientExpired,
		mqtt.OnRetainedExpired,
	}, []byte{b})
}

// OnPacketRead is called when a packet is received.
func (h *loggingHook) OnPacketRead(c *mqtt.Client, p packets.Packet) (packets.Packet, error) {
	c.RLock()
	defer c.RUnlock()

	h.log.Trace().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Str("Listener", c.Net.Listener).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint16("PacketId", p.PacketID).
		Str("PacketType", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])).
		Uint8("Version", p.ProtocolVersion).
		Msg("Packet received")
	return p, nil
}

// OnPacketSent is called immediately after a packet is written to a client.
func (h *loggingHook) OnPacketSent(c *mqtt.Client, p packets.Packet, _ []byte) {
	c.RLock()
	defer c.RUnlock()

	h.log.Trace().
		Str("ClientId", c.ID).
		Str("Listener", c.Net.Listener).
		Str("Remote", c.Net.Remote).
		Uint16("PacketId", p.PacketID).
		Str("PacketType", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])).
		Uint8("Version", p.ProtocolVersion).
		Msg("Packet sent")
}

// OnPacketProcessed is called immediately after a packet from a client is processed.
func (h *loggingHook) OnPacketProcessed(c *mqtt.Client, p packets.Packet, err error) {
	c.RLock()
	defer c.RUnlock()

	level := zerolog.TraceLevel
	if err != nil {
		level = zerolog.WarnLevel
	}

	h.log.WithLevel(level).
		Err(err).
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Str("Listener", c.Net.Listener).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Str("PacketType", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])).
		Uint8("Version", p.ProtocolVersion).
		Msg("Packet processed")
}

// OnConnect is called when a new client connects.
func (h *loggingHook) OnConnect(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Str("ClientId", c.ID).
		Str("Listener", c.Net.Listener).
		Str("Remote", c.Net.Remote).
		Uint8("Version", p.ProtocolVersion).
		Msg("Received CONNECT packet")
}

// OnSessionEstablished is called when a new client establishes a session (after OnConnect).
func (h *loggingHook) OnSessionEstablished(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	will := &struct {
		TopicName         string
		WillDelayInterval uint32
		Qos               byte
		Retain            bool
	}{
		TopicName:         c.Properties.Will.TopicName,
		WillDelayInterval: c.Properties.Will.WillDelayInterval,
		Qos:               c.Properties.Will.Qos,
		Retain:            c.Properties.Will.Retain,
	}
	if c.Properties.Will.Flag == 0 {
		will = nil
	}

	h.log.Info().
		Bool("CleanSession", c.Properties.Clean).
		Str("ClientId", c.ID).
		Int("InflightMessages", c.State.Inflight.Len()).
		Uint16("KeepAlive", c.State.Keepalive).
		Str("Listener", c.Net.Listener).
		Str("Remote", c.Net.Remote).
		Bool("ServerKeepAlive", c.State.ServerKeepalive).
		Int("Subscriptions", c.State.Subscriptions.Len()).
		Uint8("Version", p.ProtocolVersion).
		Any("Will", will).
		Msg("Client connected")
}

// OnDisconnect is called when a client is disconnected for any reason.
func (h *loggingHook) OnDisconnect(c *mqtt.Client, err error, expire bool) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info().
		Err(err).
		Str("ClientId", c.ID).
		Bool("Expire", expire).
		Str("Listener", c.Net.Listener).
		Str("Remote", c.Net.Remote).
		Uint8("Version", c.Properties.ProtocolVersion).
		Msg("Client disconnected")
}

// OnSubscribe is called when a client subscribes to one or more filters.
func (h *loggingHook) OnSubscribe(c *mqtt.Client, p packets.Packet) packets.Packet {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Str("ClientId", c.ID).
		Any("Filters", p.Filters).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Uint8("Version", p.ProtocolVersion).
		Msg("Received SUBSCRIBE packet")
	return p
}

// OnSubscribed is called when a client subscribes to one or more filters.
func (h *loggingHook) OnSubscribed(c *mqtt.Client, p packets.Packet, reasonCodes []byte) {
	c.RLock()
	defer c.RUnlock()

	for i, f := range p.Filters {
		h.log.Info().
			Str("ClientId", c.ID).
			Any("Filter", f).
			Str("Listener", c.Net.Listener).
			Uint16("PacketId", p.PacketID).
			Uint8("ReasonCode", reasonCodes[i]).
			Str("Remote", c.Net.Remote).
			Uint8("Version", p.ProtocolVersion).
			Msg("Client subscribed to topic")
	}
}

// OnUnsubscribe is called when a client unsubscribes from one or more filters.
func (h *loggingHook) OnUnsubscribe(c *mqtt.Client, p packets.Packet) packets.Packet {
	c.RLock()
	defer c.RUnlock()

	filters := make([]string, 0, len(p.Filters))
	for _, f := range p.Filters {
		filters = append(filters, f.Filter)
	}

	h.log.Debug().
		Str("ClientId", c.ID).
		Strs("Filters", filters).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Uint8("Version", p.ProtocolVersion).
		Msg("Received UNSUBSCRIBE packet")
	return p
}

// OnUnsubscribed is called when a client unsubscribes from one or more filters.
func (h *loggingHook) OnUnsubscribed(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	for _, f := range p.Filters {
		h.log.Info().
			Str("ClientId", c.ID).
			Any("Filter", f.Filter).
			Str("Listener", c.Net.Listener).
			Uint16("PacketId", p.PacketID).
			Str("Remote", c.Net.Remote).
			Uint8("Version", p.ProtocolVersion).
			Msg("Client unsubscribed to topic")
	}
}

// OnPublish is called when a client publishes a message.
func (h *loggingHook) OnPublish(c *mqtt.Client, p packets.Packet) (packets.Packet, error) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Received PUBLISH packet")
	return p, nil
}

// OnPublished is called when a client has published a message to subscribers.
func (h *loggingHook) OnPublished(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Client published a packet")
}

// OnQosPublish is called when PUBLISH packet with Qos > 1 is issued to a subscriber.
func (h *loggingHook) OnQosPublish(c *mqtt.Client, p packets.Packet, sent int64, resends int) {
	c.RLock()
	defer c.RUnlock()

	h.log.Trace().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Uint8("ReasonCode", p.ReasonCode).
		Str("Remote", c.Net.Remote).
		Int("Resends", resends).
		Bool("Retain", p.FixedHeader.Retain).
		Int64("Sent", sent).
		Uint8("Version", p.ProtocolVersion).
		Msg("New inflight packet")
}

// OnQosComplete is called when the Qos flow for a message has been completed.
func (h *loggingHook) OnQosComplete(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Publication completed")
}

// OnPublishDropped is called when a message to a client is dropped instead of being delivered.
func (h *loggingHook) OnPublishDropped(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Warn().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Packet (PUBLISH) was dropped")
}

// OnRetainMessage is called then a published message is retained.
func (h *loggingHook) OnRetainMessage(c *mqtt.Client, p packets.Packet, r int64) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Bool("Added", r == 1).
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Retained PUBLISH packet")
}

// OnQosDropped is called the Qos flow for a message expires.
func (h *loggingHook) OnQosDropped(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", p.ProtocolVersion).
		Msg("Packet (PUBLISH) was dropped")
}

// OnWillSent is called when an LWT message has been issued from a disconnecting client.
func (h *loggingHook) OnWillSent(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info().
		Str("ClientId", c.ID).
		Bool("Dup", p.FixedHeader.Dup).
		Uint8("QoS", p.FixedHeader.Qos).
		Str("TopicName", p.TopicName).
		Str("Listener", c.Net.Listener).
		Uint16("PacketId", p.PacketID).
		Str("Remote", c.Net.Remote).
		Bool("Retain", p.FixedHeader.Retain).
		Uint8("Version", c.Properties.ProtocolVersion).
		Msg("Will message sent")
}

// OnClientExpired is called when a client session has expired.
func (h *loggingHook) OnClientExpired(c *mqtt.Client) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info().
		Str("ClientId", c.ID).
		Str("Listener", c.Net.Listener).
		Str("Remote", c.Net.Remote).
		Uint8("Version", c.Properties.ProtocolVersion).
		Msg("Session expired")
}

// OnRetainedExpired is called when a retained message for a topic has expired.
func (h *loggingHook) OnRetainedExpired(topic string) {
	h.log.Info().Str("Topic", topic).Msg("Retained message expired")
}
