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

import "sync"

// Value represent a value which can be used concurrently.
type Value[T any] struct {
	// Value is the underlying value.
	Value T
	sync.RWMutex
}

// NewValue creates a new instance of the Value.
func NewValue[T any](val T) Value[T] {
	return Value[T]{Value: val}
}

// Load loads the value. This function is thread-safe.
func (v *Value[T]) Load() T {
	v.RLock()
	defer v.RUnlock()
	return v.Value
}

// Store stores a new value. This function is thread-safe.
func (v *Value[T]) Store(val T) {
	v.Lock()
	defer v.Unlock()
	v.Value = val
}
