// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/lib/retry"
	"github.com/hashicorp/consul/proto/private/pbservice"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestRPCMaterializer_RetryReportedFailureWithSnapshot(t *testing.T) {
	client := newRetrySnapshotClient()
	mat := NewRPCMaterializer(client, Deps{
		View:   &fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)},
		Logger: hclog.NewNullLogger(),
		Waiter: &retry.Waiter{MinFailures: 1},
		Request: func(index uint64) *pbsubscribe.SubscribeRequest {
			return &pbsubscribe.SubscribeRequest{Index: index}
		},
	})
	require.NoError(t, mat.updateView([]*pbsubscribe.Event{newEventServiceHealthRegister(100, 9, "old")}, 100))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); mat.Run(ctx) }()
	defer func() { cancel(); <-done }()
	require.Equal(t, uint64(100), <-client.requests)
	<-client.stream.receiving
	req := &fakeRPCRequest{index: 100}
	store := NewStore(hclog.NewNullLogger())
	store.byKey[makeEntryKey(req.Type(), req.CacheInfo())] = entry{materializer: mat}
	updates := make(chan cache.UpdateEvent, 4)
	require.NoError(t, store.Notify(ctx, req, "recovery", updates))
	defer func() {
		cancel()
		select {
		case <-store.expiryHeap.NotifyCh:
		case <-time.After(time.Second):
			t.Error("cancelled recovery watcher did not release its entry")
		}
	}()
	nextUpdate := func() cache.UpdateEvent {
		t.Helper()
		select {
		case update := <-updates:
			return update
		case <-time.After(time.Second):
			t.Fatal("recovery transition did not reach the watcher")
			return cache.UpdateEvent{}
		}
	}
	for _, tc := range []struct {
		snapshotIndex uint64
		code          codes.Code
	}{
		{50, codes.PermissionDenied},
		{50, codes.Unknown},
		{100, codes.Unknown},
	} {
		snapshotIndex := tc.snapshotIndex
		failure := status.Error(tc.code, "ACL not found or authorization changed")
		client.stream.events <- eventOrErr{Err: failure}
		index := <-client.requests
		<-client.stream.receiving
		require.Zero(t, index, "reported failures must recover from a complete snapshot even on a quiet topic")
		require.ErrorIs(t, nextUpdate().Err, failure)
		result, err := mat.Query(ctx, 0)
		require.ErrorIs(t, err, failure, "opening a stream does not establish successful authorization")
		require.Zero(t, result.Index)
		client.send(newEventServiceHealthRegister(snapshotIndex, 1, "replacement"))
		result, err = mat.Query(ctx, 0)
		require.ErrorIs(t, err, failure, "partial snapshot data must not clear the failure")
		require.Zero(t, result.Index)
		require.Empty(t, result.Value.(fakeResult).srvs)
		client.send(newEndOfSnapshotEvent(snapshotIndex))
		update := nextUpdate()
		require.NoError(t, update.Err)
		require.Equal(t, snapshotIndex, update.Meta.Index)
		result, err = mat.Query(ctx, 0)
		require.NoError(t, err)
		require.Equal(t, snapshotIndex, result.Index)
		require.Len(t, result.Value.(fakeResult).srvs, 1)
		require.Equal(t, "replacement", result.Value.(fakeResult).srvs[0].Service.Service)
	}
}

func TestRPCMaterializer_TemporaryFailuresResumeUntilReported(t *testing.T) {
	client := newRetrySnapshotClient()
	mat := NewRPCMaterializer(client, Deps{
		View:    &fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)},
		Logger:  hclog.NewNullLogger(),
		Waiter:  &retry.Waiter{MinFailures: 2},
		Request: func(index uint64) *pbsubscribe.SubscribeRequest { return &pbsubscribe.SubscribeRequest{Index: index} },
	})
	require.NoError(t, mat.updateView([]*pbsubscribe.Event{newEventServiceHealthRegister(100, 1, "retained")}, 100))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); mat.Run(ctx) }()
	defer func() { cancel(); <-done }()
	<-client.requests
	<-client.stream.receiving
	client.stream.events <- eventOrErr{Err: resetErr("temporary disconnection")}
	require.Equal(t, uint64(100), <-client.requests)
	<-client.stream.receiving
	result, err := mat.Query(ctx, 0)
	require.NoError(t, err)
	require.Len(t, result.Value.(fakeResult).srvs, 1)
	repeatedFailure := resetErr("repeated temporary disconnection")
	client.stream.events <- eventOrErr{Err: repeatedFailure}
	require.Zero(t, <-client.requests, "a second temporary failure is reported and needs snapshot recovery")
	<-client.stream.receiving
	result, err = mat.Query(ctx, 0)
	require.ErrorIs(t, err, repeatedFailure)
	require.Zero(t, result.Index)
	client.send(newEventServiceHealthRegister(100, 1, "retained"))
	client.send(newEndOfSnapshotEvent(100))
	client.send(newEventServiceHealthRegister(101, 2, "next"))
	result, err = mat.Query(ctx, 100)
	require.NoError(t, err)
	require.Len(t, result.Value.(fakeResult).srvs, 2)
}

type retrySnapshotClient struct {
	stream   *cancellationStreamClient
	requests chan uint64
}

func newRetrySnapshotClient() *retrySnapshotClient {
	return &retrySnapshotClient{
		stream:   &cancellationStreamClient{events: make(chan eventOrErr), receiving: make(chan struct{})},
		requests: make(chan uint64, 1),
	}
}

func (c *retrySnapshotClient) Subscribe(ctx context.Context, req *pbsubscribe.SubscribeRequest, _ ...grpc.CallOption) (pbsubscribe.StateChangeSubscription_SubscribeClient, error) {
	select {
	case c.requests <- req.Index:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &cancellationSubscribeClient{ctx: ctx, client: c.stream}, nil
}

func (c *retrySnapshotClient) send(event *pbsubscribe.Event) {
	c.stream.events <- eventOrErr{Event: event}
	<-c.stream.receiving
}
