/*
 * Copyright 2022 The MaxMQ Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	connProps *packet.Properties,
) packet.ConnAck {
	var props *packet.Properties

	ver := packet.MQTT311
	if conn != nil {
		ver = conn.version
	}

	if ver == packet.MQTT50 && rc == packet.ReasonCodeV5Success {
		props = &packet.Properties{}

		addServerKeepAliveToProperties(props, conn.timeout, conf)
		addSessionExpiryIntervalToProperties(props, connProps, conf)
		addMaxPacketSizeToProperties(props, conf)
		addReceiveMaximumToProperties(props, conf)
		addMaximumQoSToProperties(props, conf)
		addTopicAliasMaxToProperties(props, conf)
		addRetainAvailableToProperties(props, conf)
		addWildcardSubscriptionAvailableToProperties(props, conf)
		addSubscriptionIDAvailableToProperties(props, conf)
		addSharedSubscriptionAvailableToProperties(props, conf)
	}

	return packet.NewConnAck(ver, rc, sessionPresent, props)
}

func addServerKeepAliveToProperties(
	pr *packet.Properties,
	keepAlive uint16,
	conf Configuration,
) {
	if conf.MaxKeepAlive > 0 && keepAlive > uint16(conf.MaxKeepAlive) {
		pr.ServerKeepAlive = new(uint16)
		*pr.ServerKeepAlive = uint16(conf.MaxKeepAlive)
	}
}

func addSessionExpiryIntervalToProperties(
	pr *packet.Properties,
	props *packet.Properties,
	conf Configuration,
) {
	if props == nil || props.SessionExpiryInterval == nil {
		return
	}

	expInt := *props.SessionExpiryInterval
	maxExpInt := conf.MaxSessionExpiryInterval

	if maxExpInt > 0 && expInt > maxExpInt {
		pr.SessionExpiryInterval = new(uint32)
		*pr.SessionExpiryInterval = conf.MaxSessionExpiryInterval
	}
}

func addReceiveMaximumToProperties(pr *packet.Properties, conf Configuration) {
	if conf.MaxInflightMessages > 0 && conf.MaxInflightMessages < 65535 {
		pr.ReceiveMaximum = new(uint16)
		*pr.ReceiveMaximum = uint16(conf.MaxInflightMessages)
	}
}

func addMaxPacketSizeToProperties(pr *packet.Properties, conf Configuration) {
	if conf.MaxPacketSize != 268435456 {
		pr.MaximumPacketSize = new(uint32)
		*pr.MaximumPacketSize = uint32(conf.MaxPacketSize)
	}
}

func addMaximumQoSToProperties(pr *packet.Properties, conf Configuration) {
	if conf.MaximumQoS < 2 {
		pr.MaximumQoS = new(byte)
		*pr.MaximumQoS = byte(conf.MaximumQoS)
	}
}

func addTopicAliasMaxToProperties(pr *packet.Properties, conf Configuration) {
	if conf.MaxTopicAlias > 0 {
		pr.TopicAliasMaximum = new(uint16)
		*pr.TopicAliasMaximum = uint16(conf.MaxTopicAlias)
	}
}

func addRetainAvailableToProperties(pr *packet.Properties, conf Configuration) {
	if !conf.RetainAvailable {
		available := byte(1)
		if !conf.RetainAvailable {
			available = 0
		}

		pr.RetainAvailable = new(byte)
		*pr.RetainAvailable = available
	}
}

func addWildcardSubscriptionAvailableToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if !conf.WildcardSubscriptionAvailable {
		available := byte(1)
		if !conf.WildcardSubscriptionAvailable {
			available = 0
		}

		pr.WildcardSubscriptionAvailable = new(byte)
		*pr.WildcardSubscriptionAvailable = available
	}
}

func addSubscriptionIDAvailableToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if !conf.SubscriptionIDAvailable {
		available := byte(1)
		if !conf.SubscriptionIDAvailable {
			available = 0
		}

		pr.SubscriptionIDAvailable = new(byte)
		*pr.SubscriptionIDAvailable = available
	}
}

func addSharedSubscriptionAvailableToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if !conf.SharedSubscriptionAvailable {
		available := byte(1)
		if !conf.SharedSubscriptionAvailable {
			available = 0
		}

		pr.SharedSubscriptionAvailable = new(byte)
		*pr.SharedSubscriptionAvailable = available
	}
}
