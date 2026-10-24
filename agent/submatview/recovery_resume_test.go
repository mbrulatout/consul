// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/acl/resolver"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/lib/retry"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestLocalMaterializer_RetryReportedFailureWithCachedSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publisher := stream.NewEventPublisher(time.Minute)
	const topic = pbsubscribe.Topic_ServiceResolver
	snapshotEvent := stream.Event{Topic: topic, Index: 100, Payload: state.EventPayloadConfigEntry{
		Value: &structs.ServiceResolverConfigEntry{Name: "web", Meta: map[string]string{"source": "cached-snapshot"}},
	}}
	require.NoError(t, publisher.RegisterHandler(topic, func(_ stream.SubscribeRequest, app stream.SnapshotAppender) (uint64, error) {
		app.Append([]stream.Event{snapshotEvent})
		return 100, nil
	}, false))
	publisherDone := make(chan struct{})
	go func() {
		defer close(publisherDone)
		publisher.Run(ctx)
	}()
	defer func() { cancel(); <-publisherDone }()
	anchor, err := publisher.Subscribe(&stream.SubscribeRequest{Topic: topic, Subject: state.EventSubjectConfigEntry{Name: "web"}})
	require.NoError(t, err)
	defer anchor.Unsubscribe()
	_, err = anchor.Next(ctx)
	require.NoError(t, err)
	_, err = anchor.Next(ctx)
	require.NoError(t, err)
	event := stream.Event{Topic: topic, Index: 101, Payload: state.EventPayloadConfigEntry{
		Value: &structs.ServiceResolverConfigEntry{Name: "web", Meta: map[string]string{"source": "live-replay"}},
	}}
	publisher.Publish([]stream.Event{event})
	_, err = anchor.Next(ctx)
	require.NoError(t, err)

	backend := &recoveryResumeBackend{
		publisher: publisher,
		failure:   errors.New("backend temporarily unavailable"),
		resumed:   make(chan uint64, 1),
	}
	aclResolver := NewMockACLResolver(t)
	aclResolver.On("ResolveTokenAndDefaultMeta", "token", mock.Anything, mock.Anything).
		Return(resolver.Result{Authorizer: acl.AllowAll()}, nil)
	view := &recoveryReplayView{updates: make(chan struct{}, 3)}
	mat := NewLocalMaterializer(LocalMaterializerDeps{
		Backend:     backend,
		ACLResolver: aclResolver,
		Deps: Deps{
			Logger: hclog.NewNullLogger(),
			View:   view,
			Waiter: &retry.Waiter{MinFailures: 1},
			Request: func(index uint64) *pbsubscribe.SubscribeRequest {
				return &pbsubscribe.SubscribeRequest{
					Topic: topic, Token: "token", Index: index,
					Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}},
				}
			},
		},
	})
	require.NoError(t, mat.updateView([]*pbsubscribe.Event{newEventServiceHealthRegister(101, 9, "abandoned")}, 101))
	<-view.updates
	done := make(chan struct{})
	go func() { defer close(done); mat.Run(ctx) }()
	defer func() { cancel(); <-done }()
	select {
	case index := <-backend.resumed:
		require.Zero(t, index, "a reported error must force a replacement snapshot")
	case <-time.After(time.Second):
		t.Fatal("materializer did not resume the retained publisher stream")
	}
	for range 2 {
		select {
		case <-view.updates:
		case <-time.After(time.Second):
			t.Fatal("cached snapshot and its live suffix were not applied after reconnect")
		}
	}
	result, err := mat.Query(ctx, 0)
	require.NoError(t, err, "a recovered subscription must not retain its previous failure indefinitely")
	require.Equal(t, uint64(101), result.Index)
	events := result.Value.([]*pbsubscribe.Event)
	require.Len(t, events, 2, "the abandoned view must be replaced before applying the cached snapshot")
	require.Equal(t, []*pbsubscribe.Event{
		snapshotEvent.Payload.ToSubscriptionEvent(100),
		event.Payload.ToSubscriptionEvent(101),
	}, events)
}

type recoveryResumeBackend struct {
	publisher *stream.EventPublisher
	failure   error
	resumed   chan uint64
	failed    bool
}

func (b *recoveryResumeBackend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	if !b.failed {
		b.failed = true
		return nil, b.failure
	}
	sub, err := b.publisher.Subscribe(req)
	if err == nil {
		b.resumed <- req.Index
	}
	return sub, err
}

type recoveryReplayView struct {
	events  []*pbsubscribe.Event
	updates chan struct{}
}

func (v *recoveryReplayView) Update(events []*pbsubscribe.Event) error {
	v.events = append(v.events, events...)
	v.updates <- struct{}{}
	return nil
}

func (v *recoveryReplayView) Result(uint64) any {
	return append([]*pbsubscribe.Event(nil), v.events...)
}

func (v *recoveryReplayView) Reset() { v.events = nil }
