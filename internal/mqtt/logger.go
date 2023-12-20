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
	"context"
	"log/slog"
	"strings"

	"github.com/gsalomao/maxmq/internal/logger"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type mochiLoggerWrapper struct {
	log *logger.Logger
	ctx context.Context
}

func (l mochiLoggerWrapper) Enabled(ctx context.Context, _ slog.Level) bool {
	return l.log.Handler().Enabled(ctx, logger.LevelDebug)
}

func (l mochiLoggerWrapper) Handle(_ context.Context, record slog.Record) error {
	if record.Level == slog.LevelInfo {
		record.Level = slog.LevelDebug
	}
	return l.log.Handle(l.ctx, record)
}

func (l mochiLoggerWrapper) WithAttrs(attrs []slog.Attr) slog.Handler {
	return l.log.Handler().WithAttrs(attrs)
}

func (l mochiLoggerWrapper) WithGroup(name string) slog.Handler {
	return l.log.Handler().WithGroup(name)
}

type mochiLoggerHook struct {
	log *logger.Logger
	ctx context.Context
	mqtt.HookBase
}

func (h *mochiLoggerHook) ID() string {
	return "logger"
}

func (h *mochiLoggerHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		mqtt.OnStarted,
		mqtt.OnStopped,
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

func (h *mochiLoggerHook) OnStarted() {
	h.log.Info(h.ctx, "MQTT server started with success")
}

func (h *mochiLoggerHook) OnStopped() {
	h.log.Info(h.ctx, "MQTT server stopped with success")
}

// OnPacketRead is called when a packet is received.
func (h *mochiLoggerHook) OnPacketRead(c *mqtt.Client, p packets.Packet) (packets.Packet, error) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Packet received from client",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Str("listener", c.Net.Listener),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("packet_type", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])),
		logger.Uint8("version", p.ProtocolVersion),
	)
	return p, nil
}

// OnPacketSent is called immediately after a packet is written to a client.
func (h *mochiLoggerHook) OnPacketSent(c *mqtt.Client, p packets.Packet, _ []byte) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Packet sent to client",
		logger.Str("client_id", c.ID),
		logger.Str("listener", c.Net.Listener),
		logger.Str("remote", c.Net.Remote),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("packet_type", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnPacketProcessed is called immediately after a packet from a client is processed.
func (h *mochiLoggerHook) OnPacketProcessed(c *mqtt.Client, p packets.Packet, err error) {
	c.RLock()
	defer c.RUnlock()

	msg := "Packet from client processed with success"
	level := logger.LevelDebug
	attrs := []logger.Attr{
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Str("listener", c.Net.Listener),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("packet_type", strings.ToUpper(packets.PacketNames[p.FixedHeader.Type])),
		logger.Uint8("version", p.ProtocolVersion),
	}

	if err != nil {
		msg = "Packet from client processed with error"
		level = logger.LevelWarn
		attrs = append(attrs, logger.Err(err))
	}

	h.log.Log(h.ctx, level, msg, attrs...)
}

// OnConnect is called when a new client connects.
func (h *mochiLoggerHook) OnConnect(c *mqtt.Client, p packets.Packet) error {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Received CONNECT packet",
		logger.Str("client_id", c.ID),
		logger.Str("listener", c.Net.Listener),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", p.ProtocolVersion),
	)
	return nil
}

// OnSessionEstablished is called when a new client establishes a session (after OnConnect).
func (h *mochiLoggerHook) OnSessionEstablished(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	attrs := []logger.Attr{
		logger.Bool("clean_session", c.Properties.Clean),
		logger.Str("client_id", c.ID),
		logger.Int("inflight_messages", c.State.Inflight.Len()),
		logger.Uint16("keep_alive", c.State.Keepalive),
		logger.Str("listener", c.Net.Listener),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("server_keep_alive", c.State.ServerKeepalive),
		logger.Int("subscriptions", c.State.Subscriptions.Len()),
		logger.Uint8("version", p.ProtocolVersion),
	}

	if c.Properties.Will.Flag != 0 {
		attrs = append(attrs,
			logger.Str("will_topic", c.Properties.Will.TopicName),
			logger.Uint32("will_delay_interval", c.Properties.Will.WillDelayInterval),
			logger.Uint8("will_qos", c.Properties.Will.Qos),
			logger.Bool("will_retain", c.Properties.Will.Retain),
		)
	}

	h.log.Info(h.ctx, "Client connected", attrs...)
}

// OnDisconnect is called when a client is disconnected for any reason.
func (h *mochiLoggerHook) OnDisconnect(c *mqtt.Client, err error, expire bool) {
	c.RLock()
	defer c.RUnlock()

	attrs := []logger.Attr{
		logger.Str("client_id", c.ID),
		logger.Bool("expire", expire),
		logger.Str("listener", c.Net.Listener),
		logger.Str("remote", c.Net.Remote),
		logger.Int("subscriptions", c.State.Subscriptions.Len()),
		logger.Uint8("version", c.Properties.ProtocolVersion),
	}

	if err != nil {
		attrs = append(attrs, logger.Err(err))
	}

	h.log.Info(h.ctx, "Client disconnected", attrs...)
}

// OnSubscribe is called when a client subscribes to one or more filters.
func (h *mochiLoggerHook) OnSubscribe(c *mqtt.Client, p packets.Packet) packets.Packet {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Received SUBSCRIBE packet",
		logger.Str("client_id", c.ID),
		logger.Any("filters", p.Filters),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", p.ProtocolVersion),
	)
	return p
}

// OnSubscribed is called when a client subscribes to one or more filters.
func (h *mochiLoggerHook) OnSubscribed(c *mqtt.Client, p packets.Packet, reasonCodes []byte) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info(h.ctx, "Client subscribed to topics",
		logger.Str("client_id", c.ID),
		logger.Any("filters", p.Filters),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Any("reason_codes", reasonCodes),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnUnsubscribe is called when a client unsubscribes from one or more filters.
func (h *mochiLoggerHook) OnUnsubscribe(c *mqtt.Client, p packets.Packet) packets.Packet {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Received UNSUBSCRIBE packet",
		logger.Str("client_id", c.ID),
		logger.Any("filters", p.Filters),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", p.ProtocolVersion),
	)
	return p
}

// OnUnsubscribed is called when a client unsubscribes from one or more filters.
func (h *mochiLoggerHook) OnUnsubscribed(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info(h.ctx, "Client unsubscribed to topic",
		logger.Str("client_id", c.ID),
		logger.Any("filters", p.Filters),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnPublish is called when a client publishes a message.
func (h *mochiLoggerHook) OnPublish(c *mqtt.Client, p packets.Packet) (packets.Packet, error) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Received PUBLISH packet",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("topic_name", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
	return p, nil
}

// OnPublished is called when a client has published a message to subscribers.
func (h *mochiLoggerHook) OnPublished(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info(h.ctx, "Client published a packet",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("topic_name", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnQosPublish is called when PUBLISH packet with Qos > 1 is issued to a subscriber.
func (h *mochiLoggerHook) OnQosPublish(c *mqtt.Client, p packets.Packet, sent int64, resends int) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "New inflight packet",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Uint8("reason_code", p.ReasonCode),
		logger.Str("remote", c.Net.Remote),
		logger.Int("resends", resends),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Int64("sent", sent),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnQosComplete is called when the Qos flow for a message has been completed.
func (h *mochiLoggerHook) OnQosComplete(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Publication completed",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnPublishDropped is called when a message to a client is dropped instead of being delivered.
func (h *mochiLoggerHook) OnPublishDropped(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Warn(h.ctx, "Packet (PUBLISH) was dropped",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("topic_name", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnRetainMessage is called then a published message is retained.
func (h *mochiLoggerHook) OnRetainMessage(c *mqtt.Client, p packets.Packet, r int64) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Retained PUBLISH packet",
		logger.Bool("Added", r == 1),
		logger.Str("client_id", c.ID),
		logger.Bool("Dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("TopicName", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("Retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnQosDropped is called the Qos flow for a message expires.
func (h *mochiLoggerHook) OnQosDropped(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Debug(h.ctx, "Packet (PUBLISH) was dropped",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("topic_name", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", p.ProtocolVersion),
	)
}

// OnWillSent is called when an LWT message has been issued from a disconnecting client.
func (h *mochiLoggerHook) OnWillSent(c *mqtt.Client, p packets.Packet) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info(h.ctx, "Will message sent",
		logger.Str("client_id", c.ID),
		logger.Bool("dup", p.FixedHeader.Dup),
		logger.Uint8("qos", p.FixedHeader.Qos),
		logger.Str("topic_name", p.TopicName),
		logger.Str("listener", c.Net.Listener),
		logger.Uint16("packet_id", p.PacketID),
		logger.Str("remote", c.Net.Remote),
		logger.Bool("retain", p.FixedHeader.Retain),
		logger.Uint8("version", c.Properties.ProtocolVersion),
	)
}

// OnClientExpired is called when a client session has expired.
func (h *mochiLoggerHook) OnClientExpired(c *mqtt.Client) {
	c.RLock()
	defer c.RUnlock()

	h.log.Info(h.ctx, "Client's session expired",
		logger.Str("client_id", c.ID),
		logger.Str("listener", c.Net.Listener),
		logger.Str("remote", c.Net.Remote),
		logger.Uint8("version", c.Properties.ProtocolVersion),
	)
}

// OnRetainedExpired is called when a retained message for a topic has expired.
func (h *mochiLoggerHook) OnRetainedExpired(topic string) {
	h.log.Info(h.ctx, "Retained message expired", logger.Str("topic", topic))
}
