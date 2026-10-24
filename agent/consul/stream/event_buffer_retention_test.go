// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventPublisher_SlowSubscriberRetention(t *testing.T) {
	for _, readSnapshot := range []bool{false, true} {
		name := "blocked in snapshot"
		if readSnapshot {
			name = "blocked after snapshot"
		}
		t.Run(name, func(t *testing.T) {
			publisher := NewEventPublisher(time.Hour)
			latest := uint64(1)
			snapshots := 0
			require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
				snapshots++
				app.Append([]Event{{Topic: testTopic, Index: latest, Payload: simplePayload{key: "key", value: "snapshot"}}})
				return latest, nil
			}, false))
			request := func() *SubscribeRequest {
				return &SubscribeRequest{Topic: testTopic, Subject: StringSubject("key"), Token: "token"}
			}
			slow, err := publisher.Subscribe(request())
			require.NoError(t, err)
			defer slow.Unsubscribe()
			healthy, err := publisher.Subscribe(request())
			require.NoError(t, err)
			defer healthy.Unsubscribe()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			consumeSnapshot := func(sub *Subscription) {
				event, err := sub.Next(ctx)
				require.NoError(t, err)
				require.Equal(t, "snapshot", event.Payload.(simplePayload).value)
				event, err = sub.Next(ctx)
				require.NoError(t, err)
				require.True(t, event.IsEndOfSnapshot())
			}
			retention := publisher.topicBuffers[slow.req.topicSubject()].buf.retention
			consumeSnapshot(healthy)
			if readSnapshot {
				consumeSnapshot(slow)
			}

			for n := 0; n < retention*4; n++ {
				latest++
				events := []Event{
					{Topic: testTopic, Index: latest, Payload: simplePayload{key: "key", value: "first"}},
					{Topic: testTopic, Index: latest, Payload: simplePayload{key: "key", value: "second"}},
				}
				publisher.publishEvent(eventBatch{events: events})
				got, err := healthy.Next(ctx)
				require.NoError(t, err, "a slow subscriber must not interrupt healthy readers")
				require.Equal(t, latest, got.Index)
				require.Equal(t, events, got.Payload.(*PayloadEvents).Items)
			}

			retained := 0
			for item := slow.currentItem; item != nil && item.Err == nil; {
				retained += len(item.Events)
				if item.link == nil {
					break
				}
				next := item.link.next.Load()
				if next == nil {
					break
				}
				item = next.(*bufferItem)
			}
			require.LessOrEqual(t, retained, retention*2+2, "an idle subscriber must not retain the entire publication history")
			if !readSnapshot {
				consumeSnapshot(slow)
			}
			_, err = slow.Next(ctx)
			require.ErrorIs(t, err, ErrSubForceClosed)

			fresh, err := publisher.Subscribe(request())
			require.NoError(t, err)
			defer fresh.Unsubscribe()
			got, err := fresh.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, latest, got.Index, "an evicted live suffix must invalidate the old cached snapshot")
			require.Equal(t, 2, snapshots)
			got, err = fresh.Next(ctx)
			require.NoError(t, err)
			require.True(t, got.IsEndOfSnapshot())
			require.Equal(t, latest, got.Index)

			publisher.publishEvent(eventBatch{events: []Event{{Payload: closeSubscriptionPayload{tokensSecretIDs: []string{"token"}}}}})
			_, err = slow.Next(ctx)
			require.ErrorIs(t, err, ErrACLChanged, "ACL invalidation takes priority over a retention reset")
		})
	}
}

func TestEventPublisher_RetentionPreservesSnapshotAndResume(t *testing.T) {
	publisher := NewEventPublisher(time.Hour)
	snapshots := 0
	require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
		snapshots++
		for n := 0; n < 16; n++ {
			app.Append([]Event{{Topic: testTopic, Index: 10, Payload: simplePayload{key: "key", value: "snapshot"}}})
		}
		return 10, nil
	}, false))
	req := &SubscribeRequest{Topic: testTopic, Subject: StringSubject("key")}
	sub, err := publisher.Subscribe(req)
	require.NoError(t, err)
	defer sub.Unsubscribe()
	publisher.topicBuffers[req.topicSubject()].buf.retention = 4
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	publish := func(index uint64) {
		publisher.publishEvent(eventBatch{events: []Event{
			{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "first"}},
			{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "second"}},
		}})
	}
	for index := uint64(11); index <= 14; index++ {
		publish(index)
	}
	for n := 0; n < 16; n++ {
		event, err := sub.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, "snapshot", event.Payload.(simplePayload).value)
	}
	event, err := sub.Next(ctx)
	require.NoError(t, err)
	require.True(t, event.IsEndOfSnapshot())
	for index := uint64(11); index <= 14; index++ {
		event, err := sub.Next(ctx)
		require.NoError(t, err, "a snapshot larger than the live window must still deliver completely")
		require.Equal(t, index, event.Index)
		require.Len(t, event.Payload.(*PayloadEvents).Items, 2)
	}
	publish(15)
	event, err = sub.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(15), event.Index)

	resumed, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: req.Subject, Index: 15})
	require.NoError(t, err)
	defer resumed.Unsubscribe()
	publish(16)
	event, err = resumed.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(16), event.Index)
	require.Len(t, event.Payload.(*PayloadEvents).Items, 2)
	require.Equal(t, 1, snapshots, "a current view can resume after an older snapshot anchor expires")
}

func TestEventPublisher_RetentionPreservesSnapshotFloor(t *testing.T) {
	publisher := NewEventPublisher(time.Hour)
	require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
		app.Append([]Event{{Topic: testTopic, Index: 100, Payload: simplePayload{key: "key", value: "snapshot"}}})
		return 100, nil
	}, false))
	req := &SubscribeRequest{Topic: testTopic, Subject: StringSubject("key")}
	sub, err := publisher.Subscribe(req)
	require.NoError(t, err)
	defer sub.Unsubscribe()
	publisher.topicBuffers[req.topicSubject()].buf.retention = 4
	publish := func(index uint64) {
		publisher.publishEvent(eventBatch{events: []Event{{Topic: testTopic, Index: index, Payload: simplePayload{key: "key", value: "queued"}}}})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = sub.Next(ctx)
	require.NoError(t, err)
	event, err := sub.Next(ctx)
	require.NoError(t, err)
	require.True(t, event.IsEndOfSnapshot())
	for index := uint64(95); index < 100; index++ {
		publish(index)
	}
	_, err = sub.Next(ctx)
	require.ErrorIs(t, err, ErrSubForceClosed)

	replacement, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: req.Subject, Index: 100})
	require.NoError(t, err)
	defer replacement.Unsubscribe()
	for n := 0; n < 3; n++ {
		event, err = replacement.Next(ctx)
		require.NoError(t, err)
		if n == 0 {
			require.True(t, event.IsNewSnapshotToFollow())
		}
		if n == 2 {
			require.True(t, event.IsEndOfSnapshot())
		}
	}
	publish(100)
	publish(101)
	event, err = replacement.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(101), event.Index, "a replacement must not replay queued events covered by its snapshot")
}

func TestEventBuffer_RetentionConcurrentReaders(t *testing.T) {
	buffer := newEventBuffer()
	buffer.retention = 8
	head := buffer.Head()
	const batches = 512
	const readers = 32
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, readers)
	for n := 0; n < readers; n++ {
		go func() {
			current := head
			last := uint64(0)
			for last < batches {
				next, err := current.Next(ctx, nil)
				if errors.Is(err, ErrSubForceClosed) {
					current = buffer.Head()
					last = current.Events[0].Index
					continue
				}
				if err != nil {
					results <- err
					return
				}
				if len(next.Events) != 2 || next.Events[0].Index != last+1 || next.Events[1].Index != last+1 {
					results <- fmt.Errorf("incomplete or skipped transaction after index %d: %#v", last, next.Events)
					return
				}
				current = next
				last++
			}
			results <- nil
		}()
	}
	for n := uint64(1); n <= batches; n++ {
		buffer.Append([]Event{{Index: n}, {Index: n}})
		runtime.Gosched()
	}
	for n := 0; n < readers; n++ {
		require.NoError(t, <-results)
	}
}
