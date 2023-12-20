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

package listener

import (
	"crypto/tls"
	"net"
)

type TCP struct {
	name     string
	address  string
	conf     *tls.Config
	listener net.Listener
}

func NewTCP(name, address string, conf *tls.Config) *TCP {
	return &TCP{
		name:    name,
		address: address,
		conf:    conf,
	}
}

func (t *TCP) Name() string {
	return t.name
}

func (t *TCP) Address() string {
	return t.address
}

func (t *TCP) Listen() error {
	var err error

	if t.conf != nil {
		t.listener, err = tls.Listen("tcp", t.address, t.conf)
	} else {
		t.listener, err = net.Listen("tcp", t.address)
	}

	return err
}

func (t *TCP) Accept() (net.Conn, error) {
	return t.listener.Accept()
}

func (t *TCP) Close() error {
	return t.listener.Close()
}
