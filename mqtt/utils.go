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

func maxClientIDLenOrDefault(cIDLen int) int {
	if cIDLen < 23 || cIDLen > 65535 {
		return 23
	}

	return cIDLen
}

func generateClientID(prefix []byte) []byte {
	pLen := len(prefix)
	cID := make([]byte, pLen+20)

	if pLen > 0 {
		_ = copy(cID, prefix)
	}

	guid := xid.New()
	_ = guid.Encode(cID[pLen:])
	return cID
}

func nextConnectionDeadline(conn Connection) time.Time {
	if conn.timeout > 0 {
		timeout := math.Ceil(float64(conn.timeout) * 1.5)
		return time.Now().Add(time.Duration(timeout) * time.Second)
	}

	// Zero value of time to disable the timeout
	return time.Time{}
}

func newConnAck(
	conn *Connection,
	rc packet.ReasonCode,
	sessionPresent bool,
	conf Configuration,
	props *packet.Properties,
) packet.ConnAck {
	ver := packet.MQTT311
	if conn != nil {
		ver = conn.version
	}

	if ver == packet.MQTT50 && rc == packet.ReasonCodeV5Success {
		var expInterval *uint32

		if props != nil {
			expInterval = props.SessionExpiryInterval
			props.Reset()
		}

		props = addServerKeepAliveToProperties(props, conn.timeout, conf)
		props = addSessionExpiryIntervalToProperties(props, expInterval, conf)
		props = addMaxPacketSizeToProperties(props, conf)
		props = addReceiveMaximumToProperties(props, conf)
		props = addMaximumQoSToProperties(props, conf)
		props = addTopicAliasMaxToProperties(props, conf)
		props = addRetainAvailableToProperties(props, conf)
		props = addWildcardSubscriptionAvailableToProperties(props, conf)
		props = addSubscriptionIDAvailableToProperties(props, conf)
		props = addSharedSubscriptionAvailableToProperties(props, conf)
	}

	return packet.NewConnAck(ver, rc, sessionPresent, props)
}

func getPropertiesOrCreate(p *packet.Properties) *packet.Properties {
	if p != nil {
		return p
	}

	return &packet.Properties{}
}

func addServerKeepAliveToProperties(
	p *packet.Properties,
	keepAlive uint16,
	conf Configuration,
) *packet.Properties {
	if conf.MaxKeepAlive > 0 && keepAlive > uint16(conf.MaxKeepAlive) {
		p = getPropertiesOrCreate(p)

		p.ServerKeepAlive = new(uint16)
		*p.ServerKeepAlive = uint16(conf.MaxKeepAlive)
	}

	return p
}

func addSessionExpiryIntervalToProperties(
	p *packet.Properties,
	expInt *uint32,
	conf Configuration,
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
	conf Configuration,
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
	conf Configuration,
) *packet.Properties {
	if conf.MaxPacketSize != 268435456 {
		p = getPropertiesOrCreate(p)

		p.MaximumPacketSize = new(uint32)
		*p.MaximumPacketSize = uint32(conf.MaxPacketSize)
	}

	return p
}

func addMaximumQoSToProperties(
	p *packet.Properties,
	conf Configuration,
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
	conf Configuration,
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
	conf Configuration,
) *packet.Properties {
	if !conf.RetainAvailable {
		var available byte
		if conf.RetainAvailable {
			available = 1
		}

		p = getPropertiesOrCreate(p)
		p.RetainAvailable = new(byte)
		*p.RetainAvailable = available
	}

	return p
}

func addWildcardSubscriptionAvailableToProperties(
	p *packet.Properties,
	conf Configuration,
) *packet.Properties {
	if !conf.WildcardSubscriptionAvailable {
		var available byte
		if conf.WildcardSubscriptionAvailable {
			available = 1
		}

		p = getPropertiesOrCreate(p)
		p.WildcardSubscriptionAvailable = new(byte)
		*p.WildcardSubscriptionAvailable = available
	}

	return p
}

func addSubscriptionIDAvailableToProperties(
	p *packet.Properties,
	conf Configuration,
) *packet.Properties {
	if !conf.SubscriptionIDAvailable {
		var available byte
		if conf.SubscriptionIDAvailable {
			available = 1
		}

		p = getPropertiesOrCreate(p)
		p.SubscriptionIDAvailable = new(byte)
		*p.SubscriptionIDAvailable = available
	}

	return p
}

func addSharedSubscriptionAvailableToProperties(
	p *packet.Properties,
	conf Configuration,
) *packet.Properties {
	if !conf.SharedSubscriptionAvailable {
		var available byte
		if conf.SharedSubscriptionAvailable {
			available = 1
		}

		p = getPropertiesOrCreate(p)
		p.SharedSubscriptionAvailable = new(byte)
		*p.SharedSubscriptionAvailable = available
	}

	return p
}
