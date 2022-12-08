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

package mqtt

import (
	"github.com/gsalomao/maxmq/internal/logger"
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
)

const (
	pubSubActionStarted pubSubAction = iota
	pubSubActionStop
	pubSubActionStopped
	pubSubActionPublishMessage
)

type pubSubAction byte

type messagePublisher interface {
	publishMessage(id ClientID, msg *message) error
}

func newPubSub(pub messagePublisher, idGen IDGenerator, metrics *metrics,
	log *logger.Logger) pubSub {
	return pubSub{
		publisher: pub,
		metrics:   metrics,
		log:       log,
		tree:      newSubscriptionTree(),
		idGen:     idGen,
		action:    make(chan pubSubAction, 1),
	}
}

type pubSub struct {
	publisher messagePublisher
	idGen     IDGenerator
	metrics   *metrics
	log       *logger.Logger
	tree      subscriptionTree
	queue     messageQueue
	action    chan pubSubAction
}

func (p *pubSub) start() {
	p.log.Trace().Msg("MQTT Starting PubSub")
	go p.run()
	<-p.action
}

func (p *pubSub) stop() {
	p.log.Trace().Msg("MQTT Stopping PubSub")
	p.action <- pubSubActionStop

	for {
		act := <-p.action
		if act == pubSubActionStopped {
			break
		}
	}

	p.log.Debug().Msg("MQTT PubSub stopped with success")
}

func (p *pubSub) run() {
	p.action <- pubSubActionStarted
	p.log.Debug().Msg("MQTT PubSub waiting for actions")

	for {
		a := <-p.action
		if a == pubSubActionPublishMessage {
			p.publishQueuedMessages()
		} else {
			break
		}
	}

	p.action <- pubSubActionStopped
}

func (p *pubSub) subscribe(session *Session, topic packet.Topic,
	subscriptionID int) (Subscription, error) {

	p.log.Trace().
		Str("ClientId", string(session.ClientID)).
		Bool("NoLocal", topic.NoLocal).
		Uint8("QoS", byte(topic.QoS)).
		Bool("RetainAsPublished", topic.RetainAsPublished).
		Uint8("RetainHandling", topic.RetainHandling).
		Int("SubscriptionID", subscriptionID).
		Str("TopicFilter", topic.Name).
		Msg("MQTT Subscribing to topic")

	sub := Subscription{
		ID:                subscriptionID,
		ClientID:          session.ClientID,
		TopicFilter:       topic.Name,
		QoS:               topic.QoS,
		RetainHandling:    topic.RetainHandling,
		RetainAsPublished: topic.RetainAsPublished,
		NoLocal:           topic.NoLocal,
	}

	exists, err := p.tree.insert(sub)
	if err != nil {
		p.log.Error().
			Str("ClientId", string(session.ClientID)).
			Bool("NoLocal", topic.NoLocal).
			Uint8("QoS", byte(topic.QoS)).
			Bool("RetainAsPublished", topic.RetainAsPublished).
			Uint8("RetainHandling", topic.RetainHandling).
			Int("SubscriptionID", subscriptionID).
			Str("TopicFilter", topic.Name).
			Msg("MQTT Failed to subscribe to topic")
		return Subscription{}, err
	}

	if !exists {
		p.metrics.recordSubscribe()
	}

	p.log.Debug().
		Str("ClientId", string(session.ClientID)).
		Bool("NoLocal", topic.NoLocal).
		Uint8("QoS", byte(topic.QoS)).
		Bool("RetainAsPublished", topic.RetainAsPublished).
		Uint8("RetainHandling", topic.RetainHandling).
		Int("SubscriptionID", subscriptionID).
		Str("TopicFilter", topic.Name).
		Msg("MQTT Subscribed to topic")

	return sub, nil
}

func (p *pubSub) unsubscribe(id ClientID, topic string) error {
	p.log.Trace().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("MQTT Unsubscribing to topic")

	err := p.tree.remove(id, topic)
	if err != nil {
		p.log.Warn().
			Str("ClientId", string(id)).
			Str("TopicFilter", topic).
			Msg("MQTT Failed to remove subscription: " + err.Error())
		return err
	}

	p.metrics.recordUnsubscribe()
	p.log.Debug().
		Str("ClientId", string(id)).
		Str("TopicFilter", topic).
		Msg("MQTT Unsubscribed to topic")

	return err
}

func (p *pubSub) publish(pkt *packet.Publish) *message {
	id := p.idGen.NextID()
	msg := &message{id: messageID(id), packet: pkt}
	p.log.Trace().
		Uint8("DUP", msg.packet.Dup).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packet.PacketID)).
		Int("QueueLen", p.queue.len()).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Str("TopicName", msg.packet.TopicName).
		Msg("MQTT Adding message into the queue")

	p.queue.enqueue(msg)
	p.action <- pubSubActionPublishMessage

	p.log.Debug().
		Uint8("DUP", msg.packet.Dup).
		Uint64("MessageId", uint64(msg.id)).
		Uint16("PacketId", uint16(msg.packet.PacketID)).
		Int("QueueLen", p.queue.len()).
		Uint8("QoS", uint8(msg.packet.QoS)).
		Uint8("Retain", msg.packet.Retain).
		Str("TopicName", msg.packet.TopicName).
		Msg("MQTT Message queued for processing")

	return msg
}

func (p *pubSub) publishQueuedMessages() {
	for p.queue.len() > 0 {
		msg := p.queue.dequeue()
		p.log.Trace().
			Uint8("DUP", msg.packet.Dup).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packet.PacketID)).
			Int("QueueLen", p.queue.len()).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Str("TopicName", msg.packet.TopicName).
			Uint8("Version", uint8(msg.packet.Version)).
			Msg("MQTT Publishing queued message")

		subscriptions := p.tree.findMatches(msg.packet.TopicName)
		if len(subscriptions) > 0 {
			p.log.Trace().
				Uint64("MessageId", uint64(msg.id)).
				Uint16("PacketId", uint16(msg.packet.PacketID)).
				Int("Subscriptions", len(subscriptions)).
				Str("TopicName", msg.packet.TopicName).
				Msg("MQTT Found subscriptions")
		}

		for _, sub := range subscriptions {
			m := &message{id: msg.id, packet: msg.packet.Clone()}

			err := p.publisher.publishMessage(sub.ClientID, m)
			if err != nil {
				p.log.Error().
					Str("ClientId", string(sub.ClientID)).
					Uint8("DUP", m.packet.Dup).
					Uint64("MessageId", uint64(m.id)).
					Uint16("PacketId", uint16(m.packet.PacketID)).
					Uint8("QoS", uint8(m.packet.QoS)).
					Uint8("Retain", m.packet.Retain).
					Str("TopicName", m.packet.TopicName).
					Uint8("Version", uint8(m.packet.Version)).
					Msg("MQTT Failed to publish message to session: " +
						err.Error())
			}
		}

		p.log.Debug().
			Uint8("DUP", msg.packet.Dup).
			Uint64("MessageId", uint64(msg.id)).
			Uint16("PacketId", uint16(msg.packet.PacketID)).
			Int("QueueLen", p.queue.len()).
			Uint8("QoS", uint8(msg.packet.QoS)).
			Uint8("Retain", msg.packet.Retain).
			Str("TopicName", msg.packet.TopicName).
			Msg("MQTT Queued message published with success")
	}
}