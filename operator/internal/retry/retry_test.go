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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	errTransient = fmt.Errorf("transient error")
	errPermanent = fmt.Errorf("permanent error")
)

func isTransient(err error) bool {
	return err == errTransient
}

func TestRetrier_SucceedsImmediately(t *testing.T) {
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(context.Background(), func() error {
		callCount++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, callCount, "should succeed on first attempt")
}

func TestRetrier_SucceedsAfterRetries(t *testing.T) {
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 5},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(context.Background(), func() error {
		callCount++
		if callCount <= 2 {
			return errTransient
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, callCount, "should retry twice then succeed")
}

func TestRetrier_PermanentErrorNotRetried(t *testing.T) {
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 5},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(context.Background(), func() error {
		callCount++
		return errPermanent
	})
	require.Error(t, err)
	assert.Equal(t, errPermanent, err)
	assert.Equal(t, 1, callCount, "permanent error should not be retried")
}

func TestRetrier_ExhaustsRetries(t *testing.T) {
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(context.Background(), func() error {
		callCount++
		return errTransient
	})
	require.Error(t, err)
	assert.Equal(t, errTransient, err)
	assert.Equal(t, 4, callCount, "should attempt once + 3 retry steps")
}

func TestRetrier_OnRetryCallback(t *testing.T) {
	var callbacks []int
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
		OnRetry: func(attempt int, err error) {
			callbacks = append(callbacks, attempt)
			assert.Equal(t, errTransient, err)
		},
	}
	callCount := 0
	_ = r.Do(context.Background(), func() error {
		callCount++
		return errTransient
	})
	assert.Equal(t, []int{2, 3, 4}, callbacks, "OnRetry should be called before attempts 2, 3, 4")
}

func TestRetrier_ContextCanceledDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := Retrier{
		Backoff:     wait.Backoff{Duration: 10 * time.Second, Factor: 1.0, Steps: 5},
		IsRetryable: isTransient,
	}
	callCount := 0
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.Do(ctx, func() error {
			callCount++
			return errTransient
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.Equal(t, context.Canceled, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for Do to return after context cancellation")
	}
	assert.Equal(t, 1, callCount, "should stop retrying after ctx cancel")
}

func TestRetrier_ContextCanceledBeforeFirstAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(ctx, func() error {
		callCount++
		return errTransient
	})
	assert.Equal(t, context.Canceled, err)
	assert.Equal(t, 0, callCount, "should not call fn when ctx is already canceled")
}

func TestRetrier_ContextCanceledAfterPermanentError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
	}
	callCount := 0
	err := r.Do(ctx, func() error {
		callCount++
		return errPermanent
	})
	assert.Equal(t, errPermanent, err, "should return permanent error, not ctx.Err()")
	assert.Equal(t, 1, callCount)
}

func TestRetrier_OnRetryNotCalledOnFirstAttempt(t *testing.T) {
	callbackCount := 0
	r := Retrier{
		Backoff:     wait.Backoff{Duration: time.Millisecond, Factor: 1.0, Steps: 3},
		IsRetryable: isTransient,
		OnRetry: func(_ int, _ error) {
			callbackCount++
		},
	}
	_ = r.Do(context.Background(), func() error {
		return nil
	})
	assert.Equal(t, 0, callbackCount, "OnRetry should not be called when fn succeeds immediately")
}
