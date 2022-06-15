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

	"github.com/gsalomao/maxmq/mqtt/packet"
	"github.com/rs/xid"
)

func bufferSizeOrDefault(bs int) int {
	if bs < 1 || bs > 65535 {
		return 1024
	}

	return bs
}

func maxPacketSizeOrDefault(mps int) int {
	if mps < 20 || mps > 268435456 {
		return 268435456
	}

	return mps
}

func connectTimeoutOrDefault(ct int) int {
	if ct < 1 {
		return 5
	}

	return ct
}

func maximumQosOrDefault(mq int) int {
	if mq < 0 || mq > 2 {
		return 2
	}

	return mq
}
func maxTopicAliasOrDefault(mta int) int {
	if mta < 0 || mta > 65535 {
		return 0
	}

	return mta
}

func maxInflightMsgOrDefault(im int) int {
	if im < 0 || im > 65535 {
		return 0
	}

	return im
}

func maxClientIDLenOrDefault(idLen int) int {
	if idLen < 23 || idLen > 65535 {
		return 23
	}

	return idLen
}

func getClientID(p *packet.Connect, prefix []byte) (id ClientID, created bool) {
	if len(p.ClientID) > 0 {
		return p.ClientID, false
	}

	prefixLen := len(prefix)
	id = make([]byte, prefixLen+20)

	if prefixLen > 0 {
		_ = copy(id, prefix)
	}

	guid := xid.New()
	_ = guid.Encode(id[prefixLen:])
	return id, true
}

func addAssignedClientID(p *packet.ConnAck, s *Session, created bool) {
	if s.Version == packet.MQTT50 && created {
		props := getPropertiesOrCreate(p.Properties)
		props.AssignedClientID = s.ClientID
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

		props = getPropertiesOrCreate(props)
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

func getPropertiesOrCreate(p *packet.Properties) *packet.Properties {
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
	if conf.MaxKeepAlive > 0 &&
		(keepAlive == 0 || keepAlive > conf.MaxKeepAlive) {

		p = getPropertiesOrCreate(p)

		p.ServerKeepAlive = new(uint16)
		*p.ServerKeepAlive = uint16(conf.MaxKeepAlive)
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
		p = getPropertiesOrCreate(p)

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
		p = getPropertiesOrCreate(p)

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
		p = getPropertiesOrCreate(p)

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
		p = getPropertiesOrCreate(p)

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
		p = getPropertiesOrCreate(p)

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
		p = getPropertiesOrCreate(p)
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
		p = getPropertiesOrCreate(p)
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
		p = getPropertiesOrCreate(p)
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
		p = getPropertiesOrCreate(p)
		p.SharedSubscriptionAvailable = new(byte)
		*p.SharedSubscriptionAvailable = 0
	}

	return p
}

func getSessionExpiryInterval(p *packet.Connect, maxExp uint32) uint32 {
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
