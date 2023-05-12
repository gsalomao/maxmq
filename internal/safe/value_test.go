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

package safe

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValueLoad(t *testing.T) {
	v := NewValue(5)
	assert.Equal(t, 5, v.Load())
}

func TestValueStore(t *testing.T) {
	v := NewValue(5)
	v.Store(10)
	assert.Equal(t, 10, v.Load())
}
