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
	"math"
	"time"

	"github.com/gsalomao/maxmq/internal/mqtt/packet"
	"github.com/rs/xid"
)

func bufferSizeOrDefault(value int) int {
	if value < 1 || value > 65535 {
		return 1024
	}

	return value
}

func connectTimeoutOrDefault(value int) int {
	if value < 1 {
		return 5
	}

	return value
}

func defaultVersionOrDefault(value int) int {
	if value < int(packet.MQTT31) || value > int(packet.MQTT50) {
		return int(packet.MQTT311)
	}

	return value
}

func maxPacketSizeOrDefault(value int) int {
	if value < 20 || value > 268435456 {
		return 268435456
	}

	return value
}

func maximumQosOrDefault(value int) int {
	if value < 0 || value > 2 {
		return 2
	}

	return value
}
func maxTopicAliasOrDefault(value int) int {
	if value < 0 || value > 65535 {
		return 0
	}

	return value
}

func maxInflightMsgOrDefault(value int) int {
	if value < 0 || value > 65535 {
		return 0
	}

	return value
}

func maxInflightRetriesOrDefault(value int) int {
	if value < 0 || value > 65535 {
		return 0
	}

	return value
}

func maxClientIDLenOrDefault(value int) int {
	if value < 23 || value > 65535 {
		return 23
	}

	return value
}

func sessionKeepAlive(conf *Configuration, keepAlive int) int {
	if conf.MaxKeepAlive > 0 &&
		(keepAlive == 0 || keepAlive > conf.MaxKeepAlive) {
		return conf.MaxKeepAlive
	}
	return keepAlive
}

func createClientID(prefix []byte) ClientID {
	guid := xid.New()
	prefixLen := len(prefix)
	guidEncodedLen := 20
	id := make([]byte, prefixLen+guidEncodedLen)

	if prefixLen > 0 {
		_ = copy(id, prefix)
	}

	_ = guid.Encode(id[prefixLen:])
	return ClientID(id)
}

func addAssignedClientID(p *packet.ConnAck, v packet.MQTTVersion, id ClientID,
	created bool) {

	if v == packet.MQTT50 && created {
		props := newPropertyIfInvalid(p.Properties)
		props.AssignedClientID = []byte(id)
		p.Properties = props
	}
}

func newConnAck(
	version packet.MQTTVersion,
	code packet.ReasonCode,
	keepAlive int,
	sessionPresent bool,
	conf *Configuration,
	props *packet.Properties,
	userProps []packet.UserProperty,
) packet.ConnAck {
	if version == packet.MQTT50 && code == packet.ReasonCodeV5Success {
		var expInterval *uint32

		if props != nil {
			expInterval = props.SessionExpiryInterval
			props.Reset()
		}

		props = newPropertyIfInvalid(props)
		props = addServerKeepAliveToProperties(props, keepAlive, conf)
		props = addSessionExpiryIntervalToProperties(props, expInterval, conf)
		props = addMaxPacketSizeToProperties(props, conf)
		props = addReceiveMaximumToProperties(props, conf)
		props = addMaximumQoSToProperties(props, conf)
		props = addTopicAliasMaxToProperties(props, conf)
		props = addRetainAvailableToProperties(props, conf)
		props = addWildcardSubscriptionAvailableToProperties(props, conf)
		props = addSubscriptionIDAvailableToProperties(props, conf)
		props = addSharedSubscriptionAvailableToProperties(props, conf)
		props.UserProperties = userProps
	}

	return packet.NewConnAck(version, code, sessionPresent, props)
}

func newPropertyIfInvalid(p *packet.Properties) *packet.Properties {
	if p != nil {
		return p
	}

	return &packet.Properties{}
}

func addServerKeepAliveToProperties(
	p *packet.Properties,
	keepAlive int,
	conf *Configuration,
) *packet.Properties {
	ka := sessionKeepAlive(conf, keepAlive)
	if ka != keepAlive {
		p = newPropertyIfInvalid(p)

		p.ServerKeepAlive = new(uint16)
		*p.ServerKeepAlive = uint16(ka)
	}

	return p
}

func addSessionExpiryIntervalToProperties(
	p *packet.Properties,
	expInt *uint32,
	conf *Configuration,
) *packet.Properties {
	if expInt == nil {
		return p
	}

	maxExpInt := conf.MaxSessionExpiryInterval

	if maxExpInt > 0 && expInt != nil && *expInt > maxExpInt {
		p = newPropertyIfInvalid(p)

		p.SessionExpiryInterval = new(uint32)
		*p.SessionExpiryInterval = conf.MaxSessionExpiryInterval
	}

	return p
}

func addReceiveMaximumToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if conf.MaxInflightMessages > 0 && conf.MaxInflightMessages < 65535 {
		p = newPropertyIfInvalid(p)

		p.ReceiveMaximum = new(uint16)
		*p.ReceiveMaximum = uint16(conf.MaxInflightMessages)
	}

	return p
}

func addMaxPacketSizeToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if conf.MaxPacketSize > 0 && conf.MaxPacketSize < 268435456 {
		p = newPropertyIfInvalid(p)

		p.MaximumPacketSize = new(uint32)
		*p.MaximumPacketSize = uint32(conf.MaxPacketSize)
	}

	return p
}

func addMaximumQoSToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if conf.MaximumQoS < 2 {
		p = newPropertyIfInvalid(p)

		p.MaximumQoS = new(byte)
		*p.MaximumQoS = byte(conf.MaximumQoS)
	}

	return p
}

func addTopicAliasMaxToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if conf.MaxTopicAlias > 0 {
		p = newPropertyIfInvalid(p)

		p.TopicAliasMaximum = new(uint16)
		*p.TopicAliasMaximum = uint16(conf.MaxTopicAlias)
	}

	return p
}

func addRetainAvailableToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if !conf.RetainAvailable {
		p = newPropertyIfInvalid(p)
		p.RetainAvailable = new(byte)
		*p.RetainAvailable = 0
	}

	return p
}

func addWildcardSubscriptionAvailableToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if !conf.WildcardSubscriptionAvailable {
		p = newPropertyIfInvalid(p)
		p.WildcardSubscriptionAvailable = new(byte)
		*p.WildcardSubscriptionAvailable = 0
	}

	return p
}

func addSubscriptionIDAvailableToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if !conf.SubscriptionIDAvailable {
		p = newPropertyIfInvalid(p)
		p.SubscriptionIDAvailable = new(byte)
		*p.SubscriptionIDAvailable = 0
	}

	return p
}

func addSharedSubscriptionAvailableToProperties(
	p *packet.Properties,
	conf *Configuration,
) *packet.Properties {
	if !conf.SharedSubscriptionAvailable {
		p = newPropertyIfInvalid(p)
		p.SharedSubscriptionAvailable = new(byte)
		*p.SharedSubscriptionAvailable = 0
	}

	return p
}

func sessionExpiryIntervalOnConnect(p *packet.Connect, maxExp uint32) uint32 {
	sessionExp := uint32(math.MaxUint32)

	if p.Version == packet.MQTT50 {
		if p.Properties == nil || p.Properties.SessionExpiryInterval == nil {
			return 0
		}

		sessionExp = *p.Properties.SessionExpiryInterval
		if maxExp > 0 && sessionExp > maxExp {
			sessionExp = maxExp
		}
	}

	return sessionExp
}

func appendPendingInflightMessages(replies *[]packet.Packet, session *Session) {

	inflightMsg := session.inflightMessages.Front()
	for inflightMsg != nil {
		msg := inflightMsg.Value.(*message)
		if msg.packet != nil {
			msg.tries++
			msg.lastSent = time.Now().UnixMicro()
			*replies = append(*replies, msg.packet)
		}
		inflightMsg = inflightMsg.Next()
	}
}
