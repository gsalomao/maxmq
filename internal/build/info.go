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

package build

import (
	"bytes"
	"fmt"
	"runtime"
	"text/tabwriter"
)

// Variables updated at build time.
var (
	version      = "0.0.0"               // The application version
	revision     = "0"                   // The commit ID of the build
	buildTime    = "2020-01-01 00:00:00" // The build time in UTC (year-month-day hour:min:sec)
	distribution = "OSS"                 // The application distribution
)

// Info contains build information
type Info struct {
	// The application version.
	Version string

	// The commit ID of the build.
	Revision string

	// The build time in UTC (year-month-day hour:min:sec).
	BuildTime string

	// The runtime platform (architecture and operating system).
	Platform string

	// The application distribution.
	Distribution string

	// The runtime Go version.
	GoVersion string
}

// GetInfo returns the build Info.
func GetInfo() Info {
	return Info{
		Version:      version,
		Revision:     revision,
		BuildTime:    buildTime,
		Distribution: distribution,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
		GoVersion:    runtime.Version(),
	}
}

// ShortVersion return a pretty printed version summary.
func (i Info) ShortVersion() string {
	return fmt.Sprintf("MaxMQ %s %s\n", i.Distribution, i.Version)
}

// LongVersion returns a pretty printed build summary.
func (i Info) LongVersion() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	_, _ = fmt.Fprintf(tw, "Version:         %s\n", i.Version)
	_, _ = fmt.Fprintf(tw, "Revision:        %s\n", i.Revision)
	_, _ = fmt.Fprintf(tw, "Built:           %s\n", i.BuildTime)
	_, _ = fmt.Fprintf(tw, "Distribution:    %s\n", i.Distribution)
	_, _ = fmt.Fprintf(tw, "Platform:        %s\n", i.Platform)
	_, _ = fmt.Fprintf(tw, "Go version:      %s\n", i.GoVersion)

	_ = tw.Flush()
	return buf.String()
}
