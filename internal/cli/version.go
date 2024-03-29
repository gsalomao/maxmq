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

package cli

import (
	"github.com/gsalomao/maxmq/internal/build"
	"github.com/spf13/cobra"
)

func newCommandVersion() *cobra.Command {
	return &cobra.Command{
		Use:                   "version",
		Short:                 "Show version and build summary",
		Long:                  "Show version and build summary.",
		DisableFlagsInUseLine: true,
		Run: func(c *cobra.Command, _ []string) {
			info := build.GetInfo()
			_, _ = c.OutOrStdout().Write([]byte(info.LongVersion()))
		},
	}
}
