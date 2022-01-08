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

package broker

import (
	"github.com/gsalomao/maxmq/pkg/config"
	"github.com/gsalomao/maxmq/pkg/logger"
)

// Broker represents the message broker.
type Broker struct {
	conf config.Config
	log  *logger.Logger
}

// New creates a new broker.
func New(c config.Config, l *logger.Logger) (Broker, error) {
	return Broker{
		conf: c,
		log:  l,
	}, nil
}

// Start starts the broker.
func (b Broker) Start() error {
	b.log.Info().Msg("Starting MaxMQ broker")
	return nil
}
