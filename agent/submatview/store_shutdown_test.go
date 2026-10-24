// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/cache"
)

func TestStoreShutdownStopsIdleAndActiveMaterializers(t *testing.T) {
	store, stop, done := shutdownTestStore(t)
	idle := newShutdownMaterializer(t)
	active := newShutdownMaterializer(t)
	idleKey, _, err := store.readEntry(&shutdownRequest{key: "idle", mat: idle})
	require.NoError(t, err)
	activeKey, _, err := store.readEntry(&shutdownRequest{key: "active", mat: active})
	require.NoError(t, err)
	<-idle.started
	<-active.started
	store.releaseEntry(idleKey)
	stop()
	shutdownReceive(t, done)
	shutdownReceive(t, idle.stopped)
	shutdownReceive(t, active.stopped)
	store.releaseEntry(activeKey)
	store.evictNow(idleKey)
	store.lock.RLock()
	defer store.lock.RUnlock()
	require.Empty(t, store.byKey, "late cleanup must not recreate stopped entries")
}

func TestStoreShutdownCancelsGetAndRejectsRequests(t *testing.T) {
	store, stop, done := shutdownTestStore(t)
	mat := newShutdownMaterializer(t)
	req := &shutdownRequest{key: "blocked", mat: mat}
	type callerKey struct{}
	caller, cancel := context.WithTimeout(context.WithValue(context.Background(), callerKey{}, "preserved"), time.Minute)
	defer cancel()
	queryCtx := make(chan context.Context, 1)
	mat.query = func(ctx context.Context, _ uint64) (Result, error) {
		queryCtx <- ctx
		<-ctx.Done()
		return Result{Index: 5}, ctx.Err()
	}
	result := make(chan error, 1)
	go func() { _, err := store.Get(caller, req); result <- err }()
	observed := shutdownReceive(t, queryCtx)
	require.Equal(t, "preserved", observed.Value(callerKey{}))
	deadline, _ := caller.Deadline()
	observedDeadline, ok := observed.Deadline()
	require.True(t, ok)
	require.Equal(t, deadline, observedDeadline)
	stop()
	shutdownReceive(t, done)
	require.ErrorIs(t, shutdownReceive(t, result), context.Canceled)
	require.NoError(t, caller.Err(), "store shutdown must not cancel the caller's context")
	_, err := store.Get(caller, req)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, store.Notify(caller, req, "stopped", make(chan cache.UpdateEvent)), context.Canceled)
	require.Equal(t, int32(1), req.created.Load(), "shutdown requests must not start another materializer")
	store.lock.RLock()
	defer store.lock.RUnlock()
	require.Empty(t, store.byKey)
}

func TestStoreShutdownCancelsNotificationCallback(t *testing.T) {
	store, stop, done := shutdownTestStore(t)
	mat := newShutdownMaterializer(t)
	mat.query = func(ctx context.Context, index uint64) (Result, error) {
		if index == 0 {
			return Result{Index: 1}, nil
		}
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	entered := make(chan context.Context, 1)
	returned := make(chan struct{})
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, store.NotifyCallback(caller, &shutdownRequest{key: "notify", mat: mat}, "test", func(ctx context.Context, _ cache.UpdateEvent) {
		entered <- ctx
		<-ctx.Done()
		close(returned)
	}))
	callbackCtx := shutdownReceive(t, entered)
	stop()
	shutdownReceive(t, done)
	shutdownReceive(t, returned)
	require.ErrorIs(t, callbackCtx.Err(), context.Canceled)
	shutdownReceive(t, mat.stopped)
}

type shutdownRequest struct {
	key     string
	mat     *shutdownMaterializer
	created atomic.Int32
}

func (r *shutdownRequest) CacheInfo() cache.RequestInfo { return cache.RequestInfo{Key: r.key} }
func (r *shutdownRequest) Type() string                 { return "shutdown" }
func (r *shutdownRequest) NewMaterializer() (Materializer, error) {
	r.created.Add(1)
	return r.mat, nil
}

type shutdownMaterializer struct {
	started, stopped, cleanup chan struct{}
	query                     func(context.Context, uint64) (Result, error)
}

func newShutdownMaterializer(t *testing.T) *shutdownMaterializer {
	m := &shutdownMaterializer{started: make(chan struct{}), stopped: make(chan struct{}), cleanup: make(chan struct{})}
	t.Cleanup(func() { close(m.cleanup) })
	return m
}
func (m *shutdownMaterializer) Run(ctx context.Context) {
	close(m.started)
	defer close(m.stopped)
	select {
	case <-ctx.Done():
	case <-m.cleanup:
	}
}
func (m *shutdownMaterializer) Query(ctx context.Context, index uint64) (Result, error) {
	return m.query(ctx, index)
}
func shutdownTestStore(t *testing.T) (*Store, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	store := NewStore(hclog.NewNullLogger())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() { defer close(done); store.Run(ctx) }()
	return store, cancel, done
}
func shutdownReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(time.Second):
		t.Fatal("store shutdown did not release owned work")
		var zero T
		return zero
	}
}
