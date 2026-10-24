// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import "context"

// Observe the context consumed by Query, after Store adds cancellation wiring.
func observeStoreQueries(store *Store, req Request, ctx *controlledDeadlineContext) context.Context {
	store.lock.Lock()
	defer store.lock.Unlock()
	key := makeEntryKey(req.Type(), req.CacheInfo())
	e := store.byKey[key]
	e.materializer = &observedQueryMaterializer{Materializer: e.materializer, waiting: ctx.waiting}
	store.byKey[key] = e
	return unobservedDeadlineContext{ctx}
}

type unobservedDeadlineContext struct{ *controlledDeadlineContext }

func (c unobservedDeadlineContext) Done() <-chan struct{} { return c.expired }

type observedQueryMaterializer struct {
	Materializer
	waiting chan struct{}
}

func (m *observedQueryMaterializer) Query(ctx context.Context, index uint64) (Result, error) {
	return m.Materializer.Query(querySignalContext{Context: ctx, waiting: m.waiting}, index)
}
func (m *observedQueryMaterializer) queryUpdates(ctx context.Context, index uint64, previous *Result) (Result, error) {
	if querier, ok := m.Materializer.(updateQuerier); ok {
		return querier.queryUpdates(querySignalContext{Context: ctx, waiting: m.waiting}, index, previous)
	}
	return m.Query(ctx, index)
}

type querySignalContext struct {
	context.Context
	waiting chan struct{}
}

func (c querySignalContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}
