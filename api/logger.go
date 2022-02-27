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

import (
	"strconv"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/labstack/echo/v4"
)

const (
	logID           = "RequestID"
	logRemoteIP     = "RemoteIP"
	logURI          = "URI"
	logHost         = "Host"
	logMethod       = "Method"
	logPath         = "Path"
	logProtocol     = "Protocol"
	logUserAgent    = "UserAgent"
	logStatus       = "Status"
	logError        = "Error"
	logLatency      = "Latency"
	logLatencyHuman = "LatencyHuman"
	logBytesIn      = "BytesIn"
	logBytesOut     = "BytesOut"
)

func fromLogger(log *logger.Logger) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			start := time.Now()

			err = next(c)
			if err != nil {
				c.Error(err)
			}

			elapsed := time.Since(start)
			f := fields(c, elapsed, err)

			log.Trace().Fields(f).Msg("HTTP Request received")
			return nil
		}
	}
}

func fields(c echo.Context, d time.Duration, err error) map[string]interface{} {
	logFields := map[string]interface{}{}
	req := c.Request()
	res := c.Response()

	id := req.Header.Get(echo.HeaderXRequestID)
	if id == "" {
		id = res.Header().Get(echo.HeaderXRequestID)
	}

	logFields[logID] = id
	logFields[logRemoteIP] = c.RealIP()
	logFields[logURI] = req.RequestURI
	logFields[logHost] = req.Host
	logFields[logMethod] = req.Method

	if err != nil {
		logFields[logError] = err
	}

	path := req.URL.Path
	if path == "" {
		path = "/"
	}

	logFields[logPath] = path
	logFields[logProtocol] = req.Proto
	logFields[logUserAgent] = req.UserAgent()
	logFields[logStatus] = res.Status
	logFields[logLatency] = strconv.FormatInt(int64(d), 10)
	logFields[logLatencyHuman] = d.String()

	cl := req.Header.Get(echo.HeaderContentLength)
	if cl == "" {
		cl = "0"
	}

	logFields[logBytesIn] = cl
	logFields[logBytesOut] = strconv.FormatInt(res.Size, 10)

	return logFields
}
