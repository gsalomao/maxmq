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
	"github.com/gsalomao/maxmq/logger"
	"github.com/gsalomao/maxmq/mqtt/packet"
)

type pubSub struct {
	metrics *metrics
	log     *logger.Logger
	tree    subscriptionTree
}

func newPubSub(metrics *metrics, log *logger.Logger) pubSub {
	return pubSub{metrics: metrics, log: log, tree: newSubscriptionTree()}
}

func (p *pubSub) subscribe(session *Session, topic packet.Topic,
	subscriptionID uint32) (Subscription, error) {

	p.log.Trace().
		Bytes("ClientID", session.ClientID).
		Bool("NoLocal", topic.NoLocal).
		Uint8("QoS", byte(topic.QoS)).
		Bool("RetainAsPublished", topic.RetainAsPublished).
		Uint8("RetainHandling", topic.RetainHandling).
		Uint32("SubscriptionID", subscriptionID).
		Bytes("TopicFilter", topic.Name).
		Msg("MQTT Subscribing to topic")

	sub := Subscription{
		ID:                subscriptionID,
		Session:           session,
		TopicFilter:       string(topic.Name),
		QoS:               topic.QoS,
		RetainHandling:    topic.RetainHandling,
		RetainAsPublished: topic.RetainAsPublished,
		NoLocal:           topic.NoLocal,
	}

	err := p.tree.insert(sub)
	if err != nil {
		p.log.Error().
			Bytes("ClientID", session.ClientID).
			Bool("NoLocal", topic.NoLocal).
			Uint8("QoS", byte(topic.QoS)).
			Bool("RetainAsPublished", topic.RetainAsPublished).
			Uint8("RetainHandling", topic.RetainHandling).
			Uint32("SubscriptionID", subscriptionID).
			Bytes("TopicFilter", topic.Name).
			Msg("MQTT Failed to subscribe to topic")
		return Subscription{}, err
	}

	p.log.Debug().
		Bytes("ClientID", session.ClientID).
		Bool("NoLocal", topic.NoLocal).
		Uint8("QoS", byte(topic.QoS)).
		Bool("RetainAsPublished", topic.RetainAsPublished).
		Uint8("RetainHandling", topic.RetainHandling).
		Uint32("SubscriptionID", subscriptionID).
		Bytes("TopicFilter", topic.Name).
		Msg("MQTT Subscribed to topic")

	return sub, nil
}

func (p *pubSub) unsubscribe(id ClientID, topic string) error {
	p.log.Trace().
		Bytes("ClientID", id).
		Str("TopicFilter", topic).
		Msg("MQTT Unsubscribing to topic")

	err := p.tree.remove(id, topic)
	if err != nil {
		p.log.Warn().
			Bytes("ClientID", id).
			Str("TopicFilter", topic).
			Msg("MQTT Failed to remove subscription: " + err.Error())
		return err
	}

	p.log.Debug().
		Bytes("ClientID", id).
		Str("TopicFilter", topic).
		Msg("MQTT Unsubscribed to topic")

	return err
}
