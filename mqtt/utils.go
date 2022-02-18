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

import "github.com/gsalomao/maxmq/mqtt/packet"

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

func maxInflightMsgOrDefault(mim int) int {
	if mim < 0 || mim > 65535 {
		return 0
	}

	return mim
}

func newConnAck(
	connPkt *packet.Connect,
	code packet.ReturnCode,
	sessionPresent bool,
	conf Configuration,
) packet.ConnAck {
	var props *packet.Properties
	version := packet.MQTT311

	if connPkt != nil && connPkt.Version == packet.MQTT50 {
		version = packet.MQTT50
		props = &packet.Properties{}

		addServerKeepAliveToProperties(props, int(connPkt.KeepAlive), conf)
		addMaxPacketSizeToProperties(props, conf)
		addMaximumQoSToProperties(props, conf)
		addTopicAliasMaxToProperties(props, conf)
		addRetainAvailableToProperties(props, conf)
		addReceiveMaximumToProperties(props, conf)

		connProps := connPkt.Properties
		if connProps != nil && connProps.SessionExpiryInterval != nil {
			sesExpInt := *connPkt.Properties.SessionExpiryInterval
			addSessionExpiryIntervalToProperties(props, sesExpInt, conf)
		}
	}

	return packet.NewConnAck(version, code, sessionPresent, props)
}

func addServerKeepAliveToProperties(
	pr *packet.Properties,
	keepAlive int,
	conf Configuration,
) {
	if conf.MaxKeepAlive > 0 && keepAlive > conf.MaxKeepAlive {
		pr.ServerKeepAlive = new(uint16)
		*pr.ServerKeepAlive = uint16(conf.MaxKeepAlive)
	}
}

func addSessionExpiryIntervalToProperties(
	pr *packet.Properties,
	sesExpInt uint32,
	conf Configuration,
) {
	maxSesExpInt := conf.MaxSessionExpiryInterval

	if maxSesExpInt > 0 && sesExpInt > maxSesExpInt {
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
		ra := byte(1)
		if !conf.RetainAvailable {
			ra = 0
		}

		pr.RetainAvailable = new(byte)
		*pr.RetainAvailable = ra
	}
}
