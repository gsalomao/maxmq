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

package mocks

import "github.com/stretchr/testify/mock"

// RunnerMock is responsible to mock the broker.Runner.
type RunnerMock struct {
	mock.Mock
	RunningCh chan bool
	StopCh    chan bool
	Err       error
}

// NewRunnerMock creates a RunnerMock.
func NewRunnerMock() *RunnerMock {
	return &RunnerMock{
		RunningCh: make(chan bool),
		StopCh:    make(chan bool),
	}
}

// Run runs the runner.
func (r *RunnerMock) Run() error {
	r.Called()

	r.RunningCh <- true
	<-r.StopCh

	return r.Err
}

// Stop stops the runner unblocking the Run function.
func (r *RunnerMock) Stop() {
	r.Called()
	r.StopCh <- true
}
