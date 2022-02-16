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

package cli

import (
	"testing"

	"github.com/gsalomao/maxmq/mocks"
	"github.com/stretchr/testify/assert"
)

func TestCLI_StartBroker(t *testing.T) {
	logStub := mocks.NewLoggerStub()

	mockRunner := mocks.NewRunnerMock()
	mockRunner.On("Run")

	done := make(chan bool)
	go func() {
		defer func() {
			done <- true
		}()

		startBroker(mockRunner, logStub.Logger())
		assert.Contains(t, logStub.String(), "Starting broker")
	}()

	<-mockRunner.RunningCh
	mockRunner.StopCh <- true
	<-done
}
