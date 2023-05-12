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
	"context"
	"sync"
	"time"

	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

type packetDeliverer interface {
	deliverPacket(id packet.ClientID, p *packet.Publish) error
}

func newPubSub(pd packetDeliverer, ss *sessionStore, mt *metrics, l *logger.Logger) *pubSub {
	return &pubSub{
		deliverer:  pd,
		sessionMgr: ss,
		metrics:    mt,
		log:        l.WithPrefix("mqtt.pubsub"),
		tree:       newSubscriptionTree(),
	}
}

type pubSub struct {
	deliverer  packetDeliverer
	sessionMgr *sessionStore
	metrics    *metrics
	log        *logger.Logger
	tree       subscriptionTree
	wg         sync.WaitGroup
}

func (ps *pubSub) wait(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ps.wg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}
}

func (ps *pubSub) subscribe(s *subscription) error {
	ps.log.Trace().
		Str("ClientId", string(s.clientID)).
		Bool("NoLocal", s.noLocal).
		Uint8("QoS", byte(s.qos)).
		Bool("RetainAsPublished", s.retainAsPublished).
		Uint8("RetainHandling", s.retainHandling).
		Int("SubscriptionID", int(s.id)).
		Str("TopicFilter", s.topicFilter).
		Msg("Subscribing to topic")

	exists, err := ps.tree.insert(*s)
	if err != nil {
		ps.log.Error().
			Str("ClientId", string(s.clientID)).
			Bool("NoLocal", s.noLocal).
			Uint8("QoS", byte(s.qos)).
			Bool("RetainAsPublished", s.retainAsPublished).
			Uint8("RetainHandling", s.retainHandling).
			Int("SubscriptionId", int(s.id)).
			Str("TopicFilter", s.topicFilter).
			Msg("Failed to subscribe to topic")
		return err
	}

	if !exists {
		ps.metrics.recordSubscribe()
	}

	ps.log.Debug().
		Str("ClientId", string(s.clientID)).
		Bool("NoLocal", s.noLocal).
		Uint8("QoS", byte(s.qos)).
		Bool("RetainAsPublished", s.retainAsPublished).
		Uint8("RetainHandling", s.retainHandling).
		Int("SubscriptionId", int(s.id)).
		Str("TopicFilter", s.topicFilter).
		Msg("Subscribed to topic")

	return nil
}

func (ps *pubSub) unsubscribe(id packet.ClientID, topic string) error {
	ps.log.Trace().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("Unsubscribing to topic")

	err := ps.tree.remove(id, topic)
	if err != nil {
		ps.log.Warn().
			Str("ClientId", string(id)).
			Str("TopicFilter", topic).
			Msg("Failed to remove subscription: " + err.Error())
		return err
	}

	ps.metrics.recordUnsubscribe()
	ps.log.Debug().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("Unsubscribed to topic")

	return err
}

func (ps *pubSub) publish(msg *message) error {
	subscriptions := ps.tree.findMatches(msg.packet.TopicName)
	if len(subscriptions) == 0 {
		ps.log.Trace().
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packet.PacketID)).
			Int("Subscriptions", len(subscriptions)).
			Str("TopicName", msg.packet.TopicName).
			Msg("No subscription found")
		return nil
	}

	ps.log.Trace().
		Uint8("DUP", msg.packet.Dup).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packet.PacketID)).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Int("Subscriptions", len(subscriptions)).
		Str("TopicName", msg.packet.TopicName).
		Uint8("Version", uint8(msg.packet.Version)).
		Msg("Publishing message to subscribers")

	ps.wg.Add(len(subscriptions))
	for _, sub := range subscriptions {
		go func(s subscription) {
			defer ps.wg.Done()
			ps.publishToClient(s.clientID, msg.clone())
		}(sub)
	}

	return nil
}

func (ps *pubSub) publishToClient(id packet.ClientID, msg *message) {
	s, err := ps.sessionMgr.readSession(id)
	if err != nil {
		ps.log.Error().
			Str("ClientId", string(id)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packetID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(msg.packet.Version)).
			Msg("Failed to read session (PUBSUB): " + err.Error())
		return
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	msg.packet.Version = s.version
	if msg.packet.QoS > packet.QoS0 {
		msg.packet.PacketID = s.nextPacketID()
		msg.packetID = msg.packet.PacketID
	}

	ps.log.Trace().
		Str("ClientId", string(s.clientID)).
		Bool("Connected", s.connected).
		Int("InflightMessages", s.inflightMessages.Len()).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packetID)).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Uint64("SessionId", uint64(s.sessionID)).
		Str("TopicName", msg.packet.TopicName).
		Uint8("Version", uint8(msg.packet.Version)).
		Msg("Publishing message to client")

	if msg.packet.QoS > packet.QoS0 {
		ps.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packetID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(msg.packet.Version)).
			Msg("Adding message to inflight messages")

		if s.connected {
			msg.tries = 1
			msg.lastSent = time.Now().UnixMicro()
		}
		s.inflightMessages.PushBack(msg)

		err = ps.sessionMgr.saveSession(s)
		if err != nil {
			ps.log.Error().
				Str("ClientId", string(s.clientID)).
				Bool("Connected", s.connected).
				Int("InflightMessages", s.inflightMessages.Len()).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(msg.packetID)).
				Uint8("QoS", uint8(msg.packet.QoS)).
				Uint8("Retain", msg.packet.Retain).
				Uint64("SessionId", uint64(s.sessionID)).
				Str("TopicName", msg.packet.TopicName).
				Uint8("Version", uint8(msg.packet.Version)).
				Msg("Failed to save session (PUBSUB): " + err.Error())
			return
		}
	}

	if s.connected {
		ps.log.Trace().
			Str("ClientId", string(s.clientID)).
			Bool("Connected", s.connected).
			Int("InflightMessages", s.inflightMessages.Len()).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packetID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(msg.packet.Version)).
			Msg("Delivering message to client")

		err = ps.deliverer.deliverPacket(s.clientID, msg.packet)
		if err == nil {
			if msg.packet.QoS > packet.QoS0 {
				ps.log.Debug().
					Str("ClientId", string(s.clientID)).
					Uint64("MessageId", uint64(msg.id)).
					Uint16("PacketId", uint16(msg.packetID)).
					Uint8("QoS", uint8(msg.packet.QoS)).
					Uint8("Retain", msg.packet.Retain).
					Uint64("SessionId", uint64(s.sessionID)).
					Str("TopicName", msg.packet.TopicName).
					Uint8("Version", uint8(msg.packet.Version)).
					Msg("Message delivered to client")
			} else {
				ps.log.Info().
					Str("ClientId", string(s.clientID)).
					Uint64("MessageId", uint64(msg.id)).
					Uint16("PacketId", uint16(msg.packetID)).
					Uint8("QoS", uint8(msg.packet.QoS)).
					Uint8("Retain", msg.packet.Retain).
					Uint64("SessionId", uint64(s.sessionID)).
					Str("TopicName", msg.packet.TopicName).
					Uint8("Version", uint8(msg.packet.Version)).
					Msg("Message published to client")
			}
		} else {
			ps.log.Error().
				Str("ClientId", string(s.clientID)).
				Uint8("DUP", msg.packet.Dup).
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(msg.packet.PacketID)).
				Uint8("QoS", uint8(msg.packet.QoS)).
				Uint8("Retain", msg.packet.Retain).
				Str("TopicName", msg.packet.TopicName).
				Uint8("Version", uint8(msg.packet.Version)).
				Msg("Failed to deliver message: " + err.Error())
		}
	} else {
		ps.log.Debug().
			Str("ClientId", string(s.clientID)).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packetID)).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Uint64("SessionId", uint64(s.sessionID)).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(msg.packet.Version)).
			Msg("Client not connected")
	}
}
