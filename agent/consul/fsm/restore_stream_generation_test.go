// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package fsm

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
)

func TestFSM_RestoreServiceHealthGeneration(t *testing.T) {
	publisher := stream.NewEventPublisher(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go publisher.Run(ctx)
	fsm := newStreamingRestoreFSM(t, publisher)

	register := func(fsm *FSM, index uint64, node, address string) {
		t.Helper()
		require.NoError(t, fsm.State().EnsureNode(index-1, &structs.Node{Node: node, Address: "127.0.0.1"}))
		require.NoError(t, fsm.State().EnsureService(index, node, &structs.NodeService{
			ID: "web", Service: "web", Address: address, Port: 8080,
		}))
	}
	assertService := func(sub *stream.Subscription, index uint64, node, address string) {
		t.Helper()
		event, err := sub.Next(ctx)
		require.NoError(t, err)
		require.Equal(t, index, event.Index)
		payload, ok := event.Payload.(state.EventPayloadCheckServiceNode)
		require.True(t, ok, "expected service health event, got %T", event.Payload)
		require.Equal(t, node, payload.Value.Node.Node)
		require.Equal(t, address, payload.Value.Service.Address)
	}
	assertSnapshotEnd := func(sub *stream.Subscription, index uint64) {
		t.Helper()
		event, err := sub.Next(ctx)
		require.NoError(t, err)
		require.True(t, event.IsEndOfSnapshot())
		require.Equal(t, index, event.Index)
	}
	request := func() *stream.SubscribeRequest {
		return &stream.SubscribeRequest{Topic: state.EventTopicServiceHealth, Subject: state.EventSubjectService{Key: "web"}}
	}

	register(fsm, 40, "old", "192.0.2.1")
	old, err := publisher.Subscribe(request())
	require.NoError(t, err)
	defer old.Unsubscribe()
	assertService(old, 40, "old", "192.0.2.1")
	assertSnapshotEnd(old, 40)
	require.NoError(t, fsm.State().EnsureService(41, "old", &structs.NodeService{
		ID: "web", Service: "web", Address: "192.0.2.2", Port: 8080,
	}))
	assertService(old, 41, "old", "192.0.2.2")

	replacement := newStreamingRestoreFSM(t, nil)
	register(replacement, 20, "new", "192.0.2.3")
	snapshot, err := replacement.Snapshot()
	require.NoError(t, err)
	defer snapshot.Release()
	sink := &MockSink{Buffer: new(bytes.Buffer)}
	require.NoError(t, snapshot.Persist(sink))
	require.NoError(t, fsm.Restore(sink))
	_, err = old.Next(ctx)
	require.ErrorIs(t, err, stream.ErrSubForceClosed)

	fresh, err := publisher.Subscribe(request())
	require.NoError(t, err)
	defer fresh.Unsubscribe()
	assertService(fresh, 20, "new", "192.0.2.3")
	assertSnapshotEnd(fresh, 20)
	require.NoError(t, fsm.State().EnsureService(21, "new", &structs.NodeService{
		ID: "web", Service: "web", Address: "192.0.2.4", Port: 8080,
	}))
	assertService(fresh, 21, "new", "192.0.2.4")
}
