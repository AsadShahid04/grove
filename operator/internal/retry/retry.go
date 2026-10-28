// /*
// Copyright 2025 The Grove Authors.
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
// */

package retry

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

// IsRetryableFn is a predicate that determines whether an error should be retried.
type IsRetryableFn func(error) bool

// OnRetryFn is an optional callback invoked before each retry attempt.
// It receives the attempt number (1-indexed, so the first retry is attempt 2)
// and the error from the previous attempt.
type OnRetryFn func(attempt int, err error)

// Retrier provides a generic, context-aware retry mechanism with configurable
// backoff, retryable-error classification, and optional retry callbacks.
type Retrier struct {
	// Backoff defines the retry timing: duration, factor, jitter, and maximum steps.
	Backoff wait.Backoff
	// IsRetryable determines whether a given error should be retried.
	IsRetryable IsRetryableFn
	// OnRetry is an optional callback invoked before each retry (not before the first attempt).
	OnRetry OnRetryFn
}

// Do executes fn with retries according to the Retrier's configuration.
// It returns nil on success, or the last error if all retries are exhausted or
// ctx is canceled. The backoff respects ctx cancellation: if ctx is canceled
// during a backoff sleep, Do returns ctx.Err() immediately.
func (r *Retrier) Do(ctx context.Context, fn func() error) error {
	var lastErr error
	attempt := 0
	backoff := r.Backoff

	for {
		if ctx.Err() != nil {
			if lastErr != nil {
				return lastErr
			}
			return ctx.Err()
		}

		if attempt > 0 && r.OnRetry != nil {
			r.OnRetry(attempt+1, lastErr)
		}
		attempt++

		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if !r.IsRetryable(lastErr) {
			return lastErr
		}

		if backoff.Steps == 0 {
			return lastErr
		}

		delay := backoff.Step()
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
