// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestCacheNotify_IndexRollback(t *testing.T) {
	for _, duringCallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("duringCallback=%t", duringCallback), func(t *testing.T) {
			requests := make(chan FetchOptions, 1)
			responses := make(chan FetchResult, 1)
			stopped := make(chan struct{})
			typ := &MockType{}
			typ.On("RegisterOptions").Return(RegisterOptions{
				SupportsBlocking: true,
				Refresh:          true,
				LastGetTTL:       time.Hour,
			})
			typ.On("Fetch", mock.Anything, mock.Anything).Return(func(opts FetchOptions, _ Request) FetchResult {
				select {
				case requests <- opts:
				case <-stopped:
					return FetchResult{}
				}
				select {
				case result := <-responses:
					return result
				case <-stopped:
					return FetchResult{}
				}
			}, nil)
			c := New(Options{})
			c.RegisterType("t", typ)
			t.Cleanup(func() {
				c.Close()
				close(stopped)
			})
			parentCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &cacheQueryContext{Context: parentCtx, waiting: make(chan struct{}, 1)}
			callbackEntered := make(chan struct{})
			releaseCallback := make(chan struct{})
			updates := make(chan UpdateEvent, 4)
			first := true
			require.NoError(t, c.NotifyCallback(ctx, "t", TestRequest(t, RequestInfo{Key: "hello"}), "test", func(_ context.Context, event UpdateEvent) {
				if first {
					first = false
					close(callbackEntered)
					if duringCallback {
						select {
						case <-releaseCallback:
						case <-parentCtx.Done():
							return
						}
					}
				}
				select {
				case updates <- event:
				case <-parentCtx.Done():
				}
			}))
			expectCacheFetchIndex(t, requests, 0)
			waitForCacheQuery(t, ctx)
			responses <- FetchResult{Value: "original", Index: 100}
			select {
			case <-callbackEntered:
			case <-time.After(time.Second):
				t.Fatal("initial result did not enter callback")
			}
			if !duringCallback {
				expectCacheUpdate(t, updates, 100, "original")
				waitForCacheQuery(t, ctx)
			}
			expectCacheFetchIndex(t, requests, 100)

			getCtx, cancelGet := context.WithCancel(context.Background())
			defer cancelGet()
			futureCtx := &cacheQueryContext{Context: getCtx, waiting: make(chan struct{}, 1)}
			futureResult := make(chan error, 1)
			go func() {
				_, _, err := c.Get(futureCtx, "t", TestRequest(t, RequestInfo{Key: "hello", MinIndex: 101}))
				futureResult <- err
			}()
			waitForCacheQuery(t, futureCtx)
			responses <- FetchResult{Value: "restored", Index: 50}
			expectCacheFetchIndex(t, requests, 50)
			waitForCacheQuery(t, futureCtx)
			select {
			case err := <-futureResult:
				t.Fatalf("ordinary Get returned below its requested future index: %v", err)
			default:
			}
			if duringCallback {
				close(releaseCallback)
				expectCacheUpdate(t, updates, 100, "original")
			}
			expectCacheUpdate(t, updates, 50, "restored")
			waitForCacheQuery(t, ctx)

			responses <- FetchResult{Value: "restored", Index: 50}
			expectCacheFetchIndex(t, requests, 50)
			waitForCacheQuery(t, ctx)
			select {
			case event := <-updates:
				t.Fatalf("duplicate index woke the notification: %+v", event)
			default:
			}
			responses <- FetchResult{Value: "new update", Index: 51}
			expectCacheFetchIndex(t, requests, 51)
			expectCacheUpdate(t, updates, 51, "new update")
			cancelGet()
			select {
			case err := <-futureResult:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("ordinary Get did not stop on cancellation")
			}
		})
	}
}

func TestCacheNotify_RollbackBeforeFetch(t *testing.T) {
	c := New(Options{})
	defer c.Close()
	c.RegisterType("t", TestType(t))
	req := TestRequest(t, RequestInfo{Key: "hello", MinIndex: 100})
	opts := newGetOptions(c.types["t"], req)
	generation := uint64(1)
	opts.observedGeneration = &generation
	key := makeEntryKey("t", "", "", "", "hello")
	c.entriesLock.Lock()
	c.entries[key] = cacheEntry{
		Valid:      true,
		Value:      "restored",
		Index:      50,
		generation: 2,
		Fetching:   true,
		Waiter:     make(chan struct{}),
	}
	c.entriesLock.Unlock()

	waiter := c.fetch(key, opts, true, 0, false)
	select {
	case <-waiter:
	default:
		t.Fatal("replacement published before fetch must trigger an immediate retry")
	}
}

type cacheQueryContext struct {
	context.Context
	waiting chan struct{}
}

func (c *cacheQueryContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func waitForCacheQuery(t *testing.T, ctx *cacheQueryContext) {
	t.Helper()
	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("cache query did not begin waiting")
	}
}

func expectCacheFetchIndex(t *testing.T, requests <-chan FetchOptions, index uint64) {
	t.Helper()
	select {
	case opts := <-requests:
		require.Equal(t, index, opts.MinIndex)
	case <-time.After(time.Second):
		t.Fatal("backend fetch did not begin")
	}
}

func expectCacheUpdate(t *testing.T, updates <-chan UpdateEvent, index uint64, value string) {
	t.Helper()
	select {
	case event := <-updates:
		require.NoError(t, event.Err)
		require.Equal(t, index, event.Meta.Index)
		require.Equal(t, value, event.Result)
	case <-time.After(time.Second):
		t.Fatalf("cache notification at index %d did not arrive", index)
	}
}
