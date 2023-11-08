// Copyright 2023 The MaxMQ Authors
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

package metric

// Option is function to set an optional parameter into the server.
type Option func(s *Server)

// WithAddress returns an Option which sets the network address the server must listen to.
func WithAddress(addr string) Option {
	return func(s *Server) { s.address = addr }
}

// WithPath returns an Option which sets the path the server must use to return the metrics.
func WithPath(path string) Option {
	return func(s *Server) { s.path = path }
}

// WithProfile returns an Option which sets the server to serve profile data.
func WithProfile(profile bool) Option {
	return func(s *Server) { s.profile = profile }
}
