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

package api

// Configuration holds the HTTP server configuration.
type Configuration struct {
	// TCP address (<IP>:<port>) that the HTTP server will bind to.
	Address string

	// The amount of time, in seconds, the HTTP server waits for reading the
	// entire request, including the body.
	ReadTimeout int

	// The amount of time, in seconds, the HTTP server waits before timing out
	// writes of the response.
	WriteTimeout int

	// The amount of time, in seconds, the broker waits for graceful shutdown of
	// the HTTP server.
	ShutdownTimeout int
}
