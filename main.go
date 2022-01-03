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

package main

import (
	"github.com/gsalomao/maxmq/logger"
)

func main() {
	logger := logger.New()

	logger.Trace().Msg("This is a trace")
	logger.Debug().Msg("This is a debug")
	logger.Info().Msg("This is an info")
	logger.Warn().Msg("This is a warning")
	logger.Error().Msg("This is an error")

	logger.Fatal().Str("str", "val").Bool("bool", true).Int("int", 1).Msg("Bye")
}
