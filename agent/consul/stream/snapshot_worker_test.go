// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func snapshotTestSubscribe(t *testing.T, publisher *EventPublisher, key string) *Subscription {
	t.Helper()
	sub, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject(key)})
	require.NoError(t, err)
	t.Cleanup(sub.Unsubscribe)
	return sub
}

func snapshotTestEnd(t *testing.T, sub *Subscription) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		event, err := sub.Next(ctx)
		require.NoError(t, err)
		if event.IsEndOfSnapshot() {
			return event.Index
		}
	}
}

func snapshotTestWorkerIdle(t *testing.T, publisher *EventPublisher) {
	t.Helper()
	require.Eventually(t, func() bool {
		publisher.lock.RLock()
		defer publisher.lock.RUnlock()
		return !publisher.snapshotWorkerRunning
	}, 5*time.Second, time.Millisecond)
}

func releaseSnapshotTestBarrier(release chan struct{}) {
	select {
	case <-release:
	default:
		close(release)
	}
}

func TestEventPublisher_SnapshotSingleflightCancellation(t *testing.T) {
	publisher := NewEventPublisher(0)
	entered, release := make(chan struct{}), make(chan struct{})
	defer releaseSnapshotTestBarrier(release)
	called := make(chan string, 256)
	require.NoError(t, publisher.RegisterHandler(testTopic, func(req SubscribeRequest, _ SnapshotAppender) (uint64, error) {
		called <- req.Subject.String()
		if req.Subject == StringSubject("running") {
			close(entered)
			<-release
		}
		return 1, nil
	}, false))
	running := snapshotTestSubscribe(t, publisher, "running")
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := running.Next(ctx)
	require.ErrorIs(t, err, context.Canceled)
	first := snapshotTestSubscribe(t, publisher, "shared")
	second := snapshotTestSubscribe(t, publisher, "shared")
	first.Unsubscribe()
	for n := 0; n < 128; n++ {
		sub := snapshotTestSubscribe(t, publisher, fmt.Sprint(n))
		sub.Unsubscribe()
	}
	publisher.lock.Lock()
	remaining := len(publisher.snapshotJobs)
	buffers := len(publisher.topicBuffers)
	job := publisher.snapshotJobs[second.req.topicSubject()]
	waiters := 0
	if job != nil {
		waiters = job.waiters
	}
	queueOnlyShared := publisher.snapshotHead == job && publisher.snapshotTail == job
	publisher.lock.Unlock()
	require.NotNil(t, job)
	require.Equal(t, 2, remaining, "canceled unique keys must not accumulate behind a blocked callback")
	require.Equal(t, 2, buffers)
	require.Equal(t, 1, waiters)
	require.True(t, queueOnlyShared)
	running.Unsubscribe()
	close(release)
	require.Equal(t, uint64(1), snapshotTestEnd(t, second))
	snapshotTestWorkerIdle(t, publisher)
	require.Equal(t, "running", <-called)
	require.Equal(t, "shared", <-called)
	require.Empty(t, called, "singleflight must apply even when completed snapshot caching is disabled")
	publisher.lock.Lock()
	pendingJobs, cachedSnapshots := len(publisher.snapshotJobs), len(publisher.snapCache)
	snapshotReleased, anchorReleased, handlerReleased := job.snapshot == nil, job.anchor == nil, job.handler == nil
	publisher.lock.Unlock()
	require.Zero(t, pendingJobs)
	require.Zero(t, cachedSnapshots)
	require.True(t, snapshotReleased, "completed cleanup closures must release the original snapshot root")
	require.True(t, anchorReleased)
	require.True(t, handlerReleased)
}

func TestEventPublisher_SnapshotRetiredGeneration(t *testing.T) {
	for _, refreshAll := range []bool{false, true} {
		name := "RefreshTopic"
		if refreshAll {
			name = "RefreshAllTopics"
		}
		t.Run(name, func(t *testing.T) {
			publisher := NewEventPublisher(time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			defer releaseSnapshotTestBarrier(release)
			var calls atomic.Int32
			require.NoError(t, publisher.RegisterHandler(testTopic, func(req SubscribeRequest, app SnapshotAppender) (uint64, error) {
				n := calls.Add(1)
				index := uint64(20)
				if n == 1 {
					close(entered)
					<-release
					index = 41
				}
				app.Append([]Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: req.Subject.String(), value: fmt.Sprint(n)}}})
				return index, nil
			}, false))
			old := snapshotTestSubscribe(t, publisher, "key")
			<-entered
			pending := snapshotTestSubscribe(t, publisher, "pending")
			if refreshAll {
				publisher.RefreshAllTopics()
			} else {
				require.NoError(t, publisher.RefreshTopic(testTopic))
			}
			replacement := snapshotTestSubscribe(t, publisher, "key")
			close(release)
			require.Equal(t, uint64(20), snapshotTestEnd(t, replacement))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := old.Next(ctx)
			require.ErrorIs(t, err, ErrSubForceClosed)
			_, err = pending.Next(ctx)
			require.ErrorIs(t, err, ErrSubForceClosed)
			old.Unsubscribe()
			cached := snapshotTestSubscribe(t, publisher, "key")
			event, err := cached.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "2", event.Payload.(simplePayload).value)
			require.Equal(t, int32(2), calls.Load(), "retired pending callbacks must not execute or replace the new cache")
			snapshotTestWorkerIdle(t, publisher)
		})
	}
}

func TestEventPublisher_SnapshotFailureDoesNotStrandWorker(t *testing.T) {
	publisher := NewEventPublisher(time.Hour)
	callbackError := errors.New("snapshot callback failed")
	require.NoError(t, publisher.RegisterHandler(testTopic, func(req SubscribeRequest, _ SnapshotAppender) (uint64, error) {
		switch req.Subject {
		case StringSubject("panic"):
			panic("private snapshot panic")
		case StringSubject("error"):
			return 0, callbackError
		}
		return 12, nil
	}, false))
	panicked := snapshotTestSubscribe(t, publisher, "panic")
	failed := snapshotTestSubscribe(t, publisher, "error")
	healthy := snapshotTestSubscribe(t, publisher, "healthy")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := panicked.Next(ctx)
	require.ErrorIs(t, err, ErrSnapshotFailed)
	require.NotContains(t, err.Error(), "private snapshot panic")
	_, err = failed.Next(ctx)
	require.ErrorIs(t, err, callbackError)
	require.Equal(t, uint64(12), snapshotTestEnd(t, healthy))
	snapshotTestWorkerIdle(t, publisher)
	publisher.lock.Lock()
	panicCached := publisher.snapCache[panicked.req.topicSubject()] != nil
	errorCached := publisher.snapCache[failed.req.topicSubject()] != nil
	healthyCached := publisher.snapCache[healthy.req.topicSubject()] != nil
	publisher.lock.Unlock()
	require.False(t, panicCached)
	require.False(t, errorCached)
	require.True(t, healthyCached)
}

func TestEventPublisher_SnapshotExpiredAnchor(t *testing.T) {
	for _, floor := range []uint64{10, 100} {
		t.Run(fmt.Sprint(floor), func(t *testing.T) {
			publisher := NewEventPublisher(time.Hour)
			entered, release := make(chan struct{}), make(chan struct{})
			defer releaseSnapshotTestBarrier(release)
			var calls atomic.Int32
			require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
				index := uint64(100)
				if calls.Add(1) == 1 {
					index = floor
					close(entered)
					<-release
				}
				app.Append([]Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "snapshot"}}})
				return index, nil
			}, false))
			sub := snapshotTestSubscribe(t, publisher, "key")
			<-entered
			publisher.lock.Lock()
			publisher.topicBuffers[sub.req.topicSubject()].buf.retention = 4
			publisher.lock.Unlock()
			publish := func(index uint64) {
				publisher.publishEvent(eventBatch{events: []Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "live"}}}})
			}
			for index := uint64(11); index <= 16; index++ {
				publish(index)
			}
			close(release)
			require.Equal(t, floor, snapshotTestEnd(t, sub))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if floor == 10 {
				_, err := sub.Next(ctx)
				require.ErrorIs(t, err, ErrSubForceClosed, "missing post-snapshot transactions must trigger reset")
				publisher.lock.Lock()
				cached := publisher.snapCache[sub.req.topicSubject()] != nil
				publisher.lock.Unlock()
				require.False(t, cached)
				replacement, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject("key"), Index: floor})
				require.NoError(t, err)
				defer replacement.Unsubscribe()
				require.Equal(t, uint64(100), snapshotTestEnd(t, replacement))
			} else {
				publish(100)
				publish(101)
				event, err := sub.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, uint64(101), event.Index, "snapshot-covered history can safely skip an expired original anchor")
				cached := snapshotTestSubscribe(t, publisher, "key")
				require.Equal(t, uint64(100), snapshotTestEnd(t, cached))
				require.Equal(t, int32(1), calls.Load())
			}
			snapshotTestWorkerIdle(t, publisher)
		})
	}
}

func TestEventPublisher_SnapshotExpiredFirstLiveLink(t *testing.T) {
	publisher := NewEventPublisher(time.Hour)
	entered, release := make(chan struct{}), make(chan struct{})
	defer releaseSnapshotTestBarrier(release)
	var calls atomic.Int32
	require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
		call := calls.Add(1)
		index := uint64(100)
		if call == 2 {
			close(entered)
			<-release
		}
		if call > 2 {
			index = 107
		}
		app.Append([]Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "snapshot"}}})
		return index, nil
	}, false))
	req := &SubscribeRequest{Topic: testTopic, Subject: StringSubject("key")}
	simulateExistingSubscriber(t, publisher, req)
	publisher.lock.Lock()
	publisher.topicBuffers[req.topicSubject()].buf.retention = 4
	publisher.lock.Unlock()
	publish := func(index uint64) {
		publisher.publishEvent(eventBatch{events: []Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "live"}}}})
	}
	publish(101)
	slow := snapshotTestSubscribe(t, publisher, "key")
	<-entered
	for index := uint64(102); index <= 107; index++ {
		publish(index)
	}
	close(release)
	require.Equal(t, uint64(100), snapshotTestEnd(t, slow))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := slow.Next(ctx)
	if !errors.Is(err, ErrSubForceClosed) {
		t.Errorf("an expired first live link must reset instead of exposing an incomplete suffix: %v", err)
	}
	retry := snapshotTestSubscribe(t, publisher, "key")
	require.Equal(t, uint64(107), snapshotTestEnd(t, retry), "a snapshot with an already-expired live link must never poison the cache")
	require.Equal(t, int32(3), calls.Load())
}

func TestEventPublisher_SnapshotDoesNotBlockPublishing(t *testing.T) {
	publisher := NewEventPublisher(0)
	entered, release := make(chan struct{}), make(chan struct{})
	require.NoError(t, publisher.RegisterHandler(testTopic, func(req SubscribeRequest, _ SnapshotAppender) (uint64, error) {
		if req.Subject == StringSubject("blocked") {
			close(entered)
			<-release
		}
		return 1, nil
	}, false))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	healthy, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject("healthy")})
	require.NoError(t, err)
	initial, err := healthy.Next(ctx)
	require.NoError(t, err)
	require.True(t, initial.IsEndOfSnapshot())
	runDone := make(chan struct{})
	go func() { defer close(runDone); publisher.Run(ctx) }()
	slowResult := make(chan *Subscription, 1)
	go func() {
		sub, _ := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject("blocked")})
		slowResult <- sub
	}()
	writesDone := make(chan struct{})
	writing := false
	var slow *Subscription
	defer func() {
		close(release)
		if writing {
			<-writesDone
		}
		if slow == nil {
			slow = <-slowResult
		}
		if slow != nil {
			slow.Unsubscribe()
		}
		healthy.Unsubscribe()
		cancel()
		<-runDone
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("callback did not enter")
	}
	select {
	case slow = <-slowResult:
	case <-time.After(200 * time.Millisecond):
		t.Error("Subscribe waited for the snapshot callback")
	}
	writing = true
	count := cap(publisher.publishCh) + 2
	go func() {
		defer close(writesDone)
		for n := 0; n < count; n++ {
			publisher.Publish([]Event{{Topic: testTopic, Index: uint64(n + 2), Payload: simplePayload{key: "healthy", value: fmt.Sprint(n)}}})
		}
	}()
	select {
	case <-writesDone:
	case <-time.After(200 * time.Millisecond):
		t.Error("snapshot callback backpressured Publish beyond the asynchronous queue")
		return
	}
	for n := 0; n < count; n++ {
		event, err := healthy.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(n+2), event.Index)
	}
}

func TestEventPublisher_SnapshotShutdown(t *testing.T) {
	publisher := NewEventPublisher(0)
	entered, release := make(chan struct{}), make(chan struct{})
	defer releaseSnapshotTestBarrier(release)
	var calls atomic.Int32
	require.NoError(t, publisher.RegisterHandler(testTopic, func(SubscribeRequest, SnapshotAppender) (uint64, error) {
		calls.Add(1)
		close(entered)
		<-release
		return 1, nil
	}, false))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); publisher.Run(ctx) }()
	running := snapshotTestSubscribe(t, publisher, "running")
	<-entered
	pending := snapshotTestSubscribe(t, publisher, "pending")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("publisher shutdown waited for a snapshot callback")
	}
	_, err := running.Next(context.Background())
	require.ErrorIs(t, err, ErrShuttingDown)
	_, err = pending.Next(context.Background())
	require.ErrorIs(t, err, ErrShuttingDown)
	_, err = publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject("new")})
	require.ErrorIs(t, err, ErrShuttingDown)
	publisher.lock.Lock()
	pendingJobs := len(publisher.snapshotJobs)
	queueEmpty := publisher.snapshotHead == nil && publisher.snapshotTail == nil
	publisher.lock.Unlock()
	require.Zero(t, pendingJobs)
	require.True(t, queueEmpty)
	close(release)
	snapshotTestWorkerIdle(t, publisher)
	require.Equal(t, int32(1), calls.Load(), "shutdown must retire queued callbacks without executing them")
}

func TestEventPublisher_SnapshotTTLStartsAtCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := NewEventPublisher(10 * time.Second)
		entered, release := make(chan struct{}), make(chan struct{})
		defer releaseSnapshotTestBarrier(release)
		calls := 0
		require.NoError(t, publisher.RegisterHandler(testTopic, func(SubscribeRequest, SnapshotAppender) (uint64, error) {
			calls++
			if calls == 1 {
				close(entered)
				<-release
			}
			return uint64(calls), nil
		}, false))
		first := snapshotTestSubscribe(t, publisher, "key")
		<-entered
		time.Sleep(20 * time.Second)
		publisher.lock.Lock()
		cachedWhileBuilding := len(publisher.snapCache) > 0
		publisher.lock.Unlock()
		require.False(t, cachedWhileBuilding, "in-flight work must remain outside the TTL cache")
		close(release)
		require.Equal(t, uint64(1), snapshotTestEnd(t, first))
		time.Sleep(9 * time.Second)
		cached := snapshotTestSubscribe(t, publisher, "key")
		require.Equal(t, uint64(1), snapshotTestEnd(t, cached))
		time.Sleep(time.Second)
		synctest.Wait()
		expired := snapshotTestSubscribe(t, publisher, "key")
		require.Equal(t, uint64(2), snapshotTestEnd(t, expired), "a slow build must retain its complete TTL after completion")
	})
}
