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

func newConnAck(
	connPkt *packet.Connect,
	code packet.ReturnCode,
	sessionPresent bool,
	conf Configuration,
	userProperties []packet.UserProperty,
) packet.ConnAck {
	var props *packet.Properties
	version := packet.MQTT311

	if connPkt != nil && connPkt.Version == packet.MQTT50 {
		version = packet.MQTT50
		props = &packet.Properties{}

		addServerKeepAliveToProperties(props, connPkt.KeepAlive, conf)
		addMaxPacketSizeToProperties(props, conf)
		addMaximumQoSToProperties(props, conf)
		addRetainAvailableToProperties(props, conf)

		connProps := connPkt.Properties
		if connProps != nil && connProps.SessionExpiryInterval != nil {
			sesExpInt := *connPkt.Properties.SessionExpiryInterval
			addSessionExpiryIntervalToProperties(props, sesExpInt, conf)
		}

		props.UserProperties = userProperties
	}

	return packet.NewConnAck(version, code, sessionPresent, props)
}

func addServerKeepAliveToProperties(
	pr *packet.Properties,
	keepAlive uint16,
	conf Configuration,
) {
	if conf.MaxKeepAlive > 0 && keepAlive > conf.MaxKeepAlive {
		pr.ServerKeepAlive = new(uint16)
		*pr.ServerKeepAlive = conf.MaxKeepAlive
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

func addMaxPacketSizeToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if conf.MaxPacketSize != maxPacketSize {
		pr.MaximumPacketSize = new(uint32)
		*pr.MaximumPacketSize = uint32(conf.MaxPacketSize)
	}
}

func addMaximumQoSToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if conf.MaximumQoS < maxQoS {
		pr.MaximumQoS = new(byte)
		*pr.MaximumQoS = byte(conf.MaximumQoS)
	}
}

func addRetainAvailableToProperties(
	pr *packet.Properties,
	conf Configuration,
) {
	if !conf.RetainAvailable {
		ra := byte(1)
		if !conf.RetainAvailable {
			ra = 0
		}

		pr.RetainAvailable = new(byte)
		*pr.RetainAvailable = ra
	}
}
