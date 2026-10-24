// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package fsm

import (
	"bytes"
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/sdk/testutil"
)

func TestFSM_RestoreConcurrentSubscription(t *testing.T) {
	publisher := stream.NewEventPublisher(time.Hour)

	fsm := newStreamingRestoreFSM(t, publisher)
	oldState := fsm.State()
	require.NoError(t, oldState.SystemMetadataSet(10, &structs.SystemMetadataEntry{Key: "generation", Value: "old"}))

	replacement := newStreamingRestoreFSM(t, nil)
	require.NoError(t, replacement.State().SystemMetadataSet(20, &structs.SystemMetadataEntry{Key: "generation", Value: "new"}))
	snapshot, err := replacement.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	sink := &MockSink{Buffer: new(bytes.Buffer)}
	require.NoError(t, snapshot.Persist(sink))

	topic := stream.StringSubject("restore-lock-order")
	snapshotStarted := make(chan struct{})
	continueSnapshot := make(chan struct{})
	var calls atomic.Int32
	require.NoError(t, publisher.RegisterHandler(topic, func(_ stream.SubscribeRequest, _ stream.SnapshotAppender) (uint64, error) {
		if calls.Add(1) == 2 {
			close(snapshotStarted)
			<-continueSnapshot
		}
		snapshot := fsm.State().Snapshot()
		defer snapshot.Close()
		return snapshot.LastIndex(), nil
	}, false))

	request := &stream.SubscribeRequest{Topic: topic, Subject: stream.StringSubject("cached")}
	cached, err := publisher.Subscribe(request)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	event, err := cached.Next(ctx)
	require.NoError(t, err)
	require.True(t, event.IsEndOfSnapshot())
	require.Equal(t, uint64(10), event.Index)

	type subscriptionResult struct {
		sub *stream.Subscription
		err error
	}
	subscribed := make(chan subscriptionResult, 1)
	go func() {
		sub, err := publisher.Subscribe(&stream.SubscribeRequest{Topic: topic, Subject: stream.StringSubject("uncached")})
		subscribed <- subscriptionResult{sub, err}
	}()
	<-snapshotStarted

	restored := make(chan error, 1)
	go func() { restored <- fsm.Restore(sink) }()
	// Pause snapshot construction until restore reaches the publisher lock.
	require.Eventually(t, func() bool {
		stacks := make([]byte, 1<<20)
		n := runtime.Stack(stacks, true)
		return strings.Contains(string(stacks[:n]), "(*EventPublisher).RefreshAllTopics")
	}, 5*time.Second, time.Millisecond)
	close(continueSnapshot)

	select {
	case err := <-restored:
		require.NoError(t, err)
	case <-ctx.Done():
		stacks := make([]byte, 1<<20)
		n := runtime.Stack(stacks, true)
		t.Fatalf("restore and subscription deadlocked:\n%s", stacks[:n])
	}
	result := <-subscribed
	require.NoError(t, result.err)
	defer result.sub.Unsubscribe()
	defer cached.Unsubscribe()
	_, err = result.sub.Next(ctx)
	require.ErrorIs(t, err, stream.ErrSubForceClosed)
	_, err = cached.Next(ctx)
	require.ErrorIs(t, err, stream.ErrSubForceClosed)
	require.NotSame(t, oldState, fsm.State())
	select {
	case <-oldState.AbandonCh():
	default:
		t.Fatal("restored state did not abandon its predecessor")
	}

	fresh, err := publisher.Subscribe(request)
	require.NoError(t, err)
	defer fresh.Unsubscribe()
	event, err = fresh.Next(ctx)
	require.NoError(t, err)
	require.True(t, event.IsEndOfSnapshot())
	require.Equal(t, uint64(20), event.Index, "restore must evict snapshots from the old state generation")
}

func newStreamingRestoreFSM(t *testing.T, publisher *stream.EventPublisher) *FSM {
	t.Helper()
	return NewFromDeps(Deps{
		Logger: testutil.Logger(t),
		NewStateStore: func() *state.Store {
			if publisher != nil {
				return state.NewStateStoreWithEventPublisher(nil, publisher)
			}
			return state.NewStateStore(nil)
		},
		Publisher:      publisher,
		StorageBackend: newStorageBackend(t, nil),
	})
}
