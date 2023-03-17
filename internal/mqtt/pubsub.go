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
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/handler"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

const (
	pubSubActionStarted pubSubAction = iota
	pubSubActionStop
	pubSubActionStopped
	pubSubActionPublishMessage
)

type pubSubAction byte

type packetDeliverer interface {
	deliverPacket(id packet.ClientID, pkt *packet.Publish) error
}

func newPubSubManager(
	pd packetDeliverer,
	sm handler.SessionStore,
	mt *metrics,
	l *logger.Logger,
) *pubSubManager {
	return &pubSubManager{
		deliverer:  pd,
		sessionMgr: sm,
		metrics:    mt,
		log:        l.WithPrefix("pubsub"),
		tree:       handler.NewSubscriptionTree(),
		action:     make(chan pubSubAction, 1),
	}
}

type pubSubManager struct {
	deliverer  packetDeliverer
	sessionMgr handler.SessionStore
	metrics    *metrics
	log        *logger.Logger
	tree       handler.SubscriptionTree
	queue      handler.MessageQueue
	action     chan pubSubAction
}

// Subscribe adds the given subscription.
func (m *pubSubManager) Subscribe(s *handler.Subscription) error {
	m.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Bool("NoLocal", s.NoLocal).
		Uint8("QoS", byte(s.QoS)).
		Bool("RetainAsPublished", s.RetainAsPublished).
		Uint8("RetainHandling", s.RetainHandling).
		Int("SubscriptionID", s.ID).
		Str("TopicFilter", s.TopicFilter).
		Msg("Subscribing to topic")

	exists, err := m.tree.Insert(*s)
	if err != nil {
		m.log.Error().
			Str("ClientId", string(s.ClientID)).
			Bool("NoLocal", s.NoLocal).
			Uint8("QoS", byte(s.QoS)).
			Bool("RetainAsPublished", s.RetainAsPublished).
			Uint8("RetainHandling", s.RetainHandling).
			Int("SubscriptionId", s.ID).
			Str("TopicFilter", s.TopicFilter).
			Msg("Failed to subscribe to topic")
		return err
	}

	if !exists {
		m.metrics.recordSubscribe()
	}

	m.log.Debug().
		Str("ClientId", string(s.ClientID)).
		Bool("NoLocal", s.NoLocal).
		Uint8("QoS", byte(s.QoS)).
		Bool("RetainAsPublished", s.RetainAsPublished).
		Uint8("RetainHandling", s.RetainHandling).
		Int("SubscriptionId", s.ID).
		Str("TopicFilter", s.TopicFilter).
		Msg("Subscribed to topic")

	return nil
}

// Unsubscribe removes the subscription for the given client identifier and topic.
func (m *pubSubManager) Unsubscribe(id packet.ClientID, topic string) error {
	m.log.Trace().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("Unsubscribing to topic")

	err := m.tree.Remove(id, topic)
	if err != nil {
		m.log.Warn().
			Str("ClientId", string(id)).
			Str("TopicFilter", topic).
			Msg("Failed to remove subscription: " + err.Error())
		return err
	}

	m.metrics.recordUnsubscribe()
	m.log.Debug().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("Unsubscribed to topic")

	return err
}

// Publish publishes the given message to all subscriptions.
func (m *pubSubManager) Publish(msg *handler.Message) error {
	m.log.Trace().
		Uint8("DUP", msg.Packet.Dup).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.PacketID)).
		Int("QueueLen", m.queue.Len()).
		Uint8("QoS", uint8(msg.Packet.QoS)).
		Uint8("Retain", msg.Packet.Retain).
		Str("TopicName", msg.Packet.TopicName).
		Msg("Adding message into the queue")

	m.queue.Enqueue(msg)
	m.action <- pubSubActionPublishMessage

	m.log.Debug().
		Uint8("DUP", msg.Packet.Dup).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.Packet.PacketID)).
		Int("QueueLen", m.queue.Len()).
		Uint8("QoS", uint8(msg.Packet.QoS)).
		Uint8("Retain", msg.Packet.Retain).
		Str("TopicName", msg.Packet.TopicName).
		Msg("Message queued for processing")

	return nil
}

func (m *pubSubManager) start() {
	m.log.Trace().Msg("Starting pubsub")
	go m.run()
	<-m.action
}

func (m *pubSubManager) stop() {
	m.log.Trace().Msg("Stopping pubsub")
	m.action <- pubSubActionStop

	for {
		act := <-m.action
		if act == pubSubActionStopped {
			break
		}
	}

	m.log.Debug().Msg("Pubsub stopped with success")
}

func (m *pubSubManager) run() {
	m.action <- pubSubActionStarted
	m.log.Debug().Msg("Waiting for actions")

	for {
		a := <-m.action
		if a == pubSubActionPublishMessage {
			m.handleQueuedMessages()
		} else {
			break
		}
	}

	m.action <- pubSubActionStopped
}

func (m *pubSubManager) handleQueuedMessages() {
	for m.queue.Len() > 0 {
		msg := m.queue.Dequeue()
		m.log.Trace().
			Uint8("DUP", msg.Packet.Dup).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(msg.Packet.PacketID)).
			Int("QueueLen", m.queue.Len()).
			Uint8("QoS", uint8(msg.Packet.QoS)).
			Uint8("Retain", msg.Packet.Retain).
			Str("TopicName", msg.Packet.TopicName).
			Uint8("Version", uint8(msg.Packet.Version)).
			Msg("Publishing queued message")

		subscriptions := m.tree.FindMatches(msg.Packet.TopicName)
		if len(subscriptions) == 0 {
			m.log.Trace().
				Uint64("MessageId", uint64(msg.ID)).
				Uint16("PacketId", uint16(msg.Packet.PacketID)).
				Int("Subscriptions", len(subscriptions)).
				Str("TopicName", msg.Packet.TopicName).
				Msg("No subscription found")
			continue
		}

		for _, sub := range subscriptions {
			m.publishMsgToClient(sub.ClientID, msg.Clone())
		}

		m.log.Debug().
			Uint8("DUP", msg.Packet.Dup).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(msg.Packet.PacketID)).
			Int("QueueLen", m.queue.Len()).
			Uint8("QoS", uint8(msg.Packet.QoS)).
			Uint8("Retain", msg.Packet.Retain).
			Str("TopicName", msg.Packet.TopicName).
			Msg("Queued message published with success")
	}
}

func (m *pubSubManager) publishMsgToClient(id packet.ClientID, msg *handler.Message) {
	s, err := m.sessionMgr.ReadSession(id)
	if err != nil {
		m.log.Error().
			Str("ClientId", string(id)).
			Uint64("MessageId", uint64(msg.ID)).
			Uint16("PacketId", uint16(msg.PacketID)).
			Uint8("QoS", uint8(msg.Packet.QoS)).
			Uint8("Retain", msg.Packet.Retain).
			Str("TopicName", msg.Packet.TopicName).
			Uint8("Version", uint8(msg.Packet.Version)).
			Msg("Failed to read session (PUBSUB): " + err.Error())
		return
	}

	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	msg.Packet.Version = s.Version
	if msg.Packet.QoS > packet.QoS0 {
		msg.Packet.PacketID = s.NextPacketID()
		msg.PacketID = msg.Packet.PacketID
	}

	m.log.Trace().
		Str("ClientId", string(s.ClientID)).
		Bool("Connected", s.Connected).
		Int("InflightMessages", s.InflightMessages.Len()).
		Uint64("MessageId", uint64(msg.ID)).
		Uint16("PacketId", uint16(msg.PacketID)).
		Uint8("QoS", uint8(msg.Packet.QoS)).
		Uint8("Retain", msg.Packet.Retain).
		Uint64("SessionId", uint64(s.SessionID)).
		Str("TopicName", msg.Packet.TopicName).
		Uint8("Version", uint8(msg.Packet.Version)).
		Msg("Publishing message to client")

	if msg.Packet.QoS > packet.QoS0 {
		if s.Connected {
			msg.Tries = 1
			msg.LastSent = time.Now().UnixMicro()
		}
		s.InflightMessages.PushBack(msg)

		err = m.sessionMgr.SaveSession(s)
		if err != nil {
			m.log.Error().
				Str("ClientId", string(s.ClientID)).
				Bool("Connected", s.Connected).
				Int("InflightMessages", s.InflightMessages.Len()).
				Uint64("MessageId", uint64(msg.ID)).
				Uint16("PacketId", uint16(msg.PacketID)).
				Uint8("QoS", uint8(msg.Packet.QoS)).
				Uint8("Retain", msg.Packet.Retain).
				Uint64("SessionId", uint64(s.SessionID)).
				Str("TopicName", msg.Packet.TopicName).
				Uint8("Version", uint8(msg.Packet.Version)).
				Msg("Failed to save session (PUBSUB): " + err.Error())
			return
		}
	}

	if s.Connected {
		err = m.deliverer.deliverPacket(s.ClientID, msg.Packet)
		if err == nil {
			if msg.Packet.QoS > packet.QoS0 {
				m.log.Debug().
					Str("ClientId", string(s.ClientID)).
					Uint64("MessageId", uint64(msg.ID)).
					Uint16("PacketId", uint16(msg.PacketID)).
					Uint8("QoS", uint8(msg.Packet.QoS)).
					Uint8("Retain", msg.Packet.Retain).
					Uint64("SessionId", uint64(s.SessionID)).
					Str("TopicName", msg.Packet.TopicName).
					Uint8("Version", uint8(msg.Packet.Version)).
					Msg("Message delivered to client")
			} else {
				m.log.Info().
					Str("ClientId", string(s.ClientID)).
					Uint64("MessageId", uint64(msg.ID)).
					Uint16("PacketId", uint16(msg.PacketID)).
					Uint8("QoS", uint8(msg.Packet.QoS)).
					Uint8("Retain", msg.Packet.Retain).
					Uint64("SessionId", uint64(s.SessionID)).
					Str("TopicName", msg.Packet.TopicName).
					Uint8("Version", uint8(msg.Packet.Version)).
					Msg("Message published to client")
			}
		} else {
			m.log.Error().
				Str("ClientId", string(s.ClientID)).
				Uint8("DUP", msg.Packet.Dup).
				Uint64("MessageId", uint64(msg.ID)).
				Uint16("PacketId", uint16(msg.Packet.PacketID)).
				Uint8("QoS", uint8(msg.Packet.QoS)).
				Uint8("Retain", msg.Packet.Retain).
				Str("TopicName", msg.Packet.TopicName).
				Uint8("Version", uint8(msg.Packet.Version)).
				Msg("Failed to deliver message: " + err.Error())
		}
	}
}
