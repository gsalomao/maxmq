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
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gsalomao/maxmq/logger"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// HTTPServer represents the HTTP server.
type HTTPServer struct {
	// Instance of the Echo framework.
	Echo *echo.Echo

	// Base of API routes.
	RouteBase *echo.Group

	// API routes v1.
	RouteV1 *echo.Group

	conf Configuration
	log  *logger.Logger
}

// NewHTTPServer creates a HTTPServer.
func NewHTTPServer(c Configuration, log *logger.Logger) (*HTTPServer, error) {
	if log == nil {
		return nil, errors.New("HTTP missing logger")
	}
	if c.Address == "" {
		return nil, errors.New("HTTP missing address")
	}

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Server.ReadTimeout = time.Duration(c.ReadTimeout) * time.Second
	e.Server.WriteTimeout = time.Duration(c.WriteTimeout) * time.Second
	e.Use(middleware.RequestID())
	e.Use(fromLogger(log))

	api := e.Group("/api")
	v1 := api.Group("/v1")

	httpSrv := &HTTPServer{
		RouteBase: api,
		RouteV1:   v1,
		Echo:      e,
		conf:      c,
		log:       log,
	}

	e.HTTPErrorHandler = httpSrv.handleError

	return httpSrv, nil
}

// Run starts the execution of the HTTPServer.
// Once called, it blocks waiting for connections until it's stopped by the
// Stop function.
func (s *HTTPServer) Run() error {
	lsn, err := net.Listen("tcp", s.conf.Address)
	if err != nil {
		return err
	}

	s.log.Info().Msg("HTTP Listening on " + lsn.Addr().String())
	s.Echo.Listener = lsn

	if err := s.Echo.Start(s.conf.Address); err != http.ErrServerClosed {
		return err
	}

	s.log.Debug().Msg("HTTP Server stopped with success")
	return nil
}

// Stop stops the HTTPServer.
// Once called, it unblocks the Run function.
func (s *HTTPServer) Stop() {
	s.log.Debug().Msg("HTTP Stopping server")

	t := time.Duration(s.conf.ShutdownTimeout) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()

	err := s.Echo.Shutdown(ctx)
	if err != nil {
		_ = s.Echo.Close()
	}
}

func (s HTTPServer) handleError(err error, c echo.Context) {
	httpErr, ok := err.(*echo.HTTPError)
	if ok {
		s.log.Debug().
			Str("Path", c.Path()).
			Int("Status", httpErr.Code).
			Msg(fmt.Sprintf("HTTP Request error: %s", httpErr.Message))
	} else {
		httpErr = echo.ErrInternalServerError
		s.log.Warn().Msg("HTTP Request error: " + err.Error())

	}

	if c.Request().Method == http.MethodHead {
		err = c.NoContent(httpErr.Code)
	} else {
		err = c.JSON(httpErr.Code, httpErr)
	}
	if err != nil {
		s.log.Error().
			Str("Path", c.Path()).
			Int("Status", httpErr.Code).
			Msg("HTTP Failed to send error response: " + err.Error())
	}
}
