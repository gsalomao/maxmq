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
	"errors"
	"sync"

	"github.com/gsalomao/maxmq/logger"
)

// Broker represents the message broker.
type Broker struct {
	log     *logger.Logger
	wg      sync.WaitGroup
	runners []Runner
	err     error
}

// New creates a new broker.
func New(log *logger.Logger) Broker {
	return Broker{
		log: log,
	}
}

// AddRunner adds a runner to the broker.
func (b *Broker) AddRunner(r Runner) {
	b.runners = append(b.runners, r)
}

// Start starts the broker running all runners.
func (b *Broker) Start() error {
	b.log.Info().Msg("Starting broker")

	if len(b.runners) == 0 {
		return errors.New("no available runner")
	}

	for _, r := range b.runners {
		b.wg.Add(1)
		go func(r Runner) {
			defer b.wg.Done()

			err := r.Run()
			if err != nil {
				b.err = err
			}
		}(r)
	}

	b.log.Info().Msg("Broker started with success")
	return nil
}

// Stop stops the broker stopping all runners.
func (b *Broker) Stop() {
	b.log.Info().Msg("Stopping broker")
	b.wg.Add(1)

	for _, r := range b.runners {
		r.Stop()
	}

	b.log.Info().Msg("Broker stopped with success")
	b.wg.Done()
}

// Wait blocks while the broker is running.
func (b *Broker) Wait() error {
	b.wg.Wait()
	return b.err
}
