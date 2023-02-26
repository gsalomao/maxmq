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
	"github.com/gsalomao/maxmq/internal/mqtt/packet"
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
