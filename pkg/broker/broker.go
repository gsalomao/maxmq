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
	"context"
	"errors"
	"sync"

	"github.com/gsalomao/maxmq/pkg/logger"
)

// Listener is a network interface for stream-oriented protocols.
type Listener interface {
	// Run executes the listener.
	// It blocks until the listener stops.
	Run() error

	// Stop stops the listener unblocking the call of the Run().
	Stop()
}

// Broker represents the message broker.
type Broker struct {
	ctx       context.Context
	log       *logger.Logger
	wg        sync.WaitGroup
	listeners []Listener
	err       error
}

// New creates a new broker.
func New(ctx context.Context, log *logger.Logger) (Broker, error) {
	return Broker{
		ctx: ctx,
		log: log,
	}, nil
}

// AddListener adds a listener to the broker.
func (b *Broker) AddListener(l Listener) {
	b.listeners = append(b.listeners, l)
}

// Start starts the broker running all listeners.
func (b *Broker) Start() error {
	b.log.Info().Msg("Starting broker")

	if len(b.listeners) == 0 {
		return errors.New("no available listener")
	}

	for _, l := range b.listeners {
		b.wg.Add(1)
		go func(l Listener) {
			defer b.wg.Done()

			err := l.Run()
			if err != nil {
				b.err = err
				return
			}
		}(l)
	}

	b.log.Info().Msg("Broker started with success")
	return nil
}

// Stop stops the broker stopping all listeners.
func (b *Broker) Stop() {
	b.log.Info().Msg("Stoping broker")
	b.wg.Add(1)

	for _, l := range b.listeners {
		l.Stop()
	}

	b.log.Info().Msg("Broker stopped with success")
	b.wg.Done()
}

// Wait blocks while the broker is running.
func (b *Broker) Wait() error {
	b.wg.Wait()
	return b.err
}
