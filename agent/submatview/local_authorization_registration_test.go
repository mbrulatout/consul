// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/acl/resolver"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestLocalMaterializer_ACLChangeBeforeRegistration(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "revoke read permission"
		if deleted {
			name = "delete token"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			publisher := stream.NewEventPublisher(time.Minute)
			const topic = pbsubscribe.Topic_ServiceResolver
			require.NoError(t, publisher.RegisterHandler(topic, func(_ stream.SubscribeRequest, app stream.SnapshotAppender) (uint64, error) {
				app.Append([]stream.Event{{Topic: topic, Index: 100, Payload: state.EventPayloadConfigEntry{Value: &structs.ServiceResolverConfigEntry{Name: "web"}}}})
				return 100, nil
			}, false))
			publisherDone := make(chan struct{})
			go func() { defer close(publisherDone); publisher.Run(ctx) }()
			defer func() { cancel(); <-publisherDone }()
			guard, err := publisher.Subscribe(&stream.SubscribeRequest{Topic: topic, Subject: state.EventSubjectConfigEntry{Name: "guard"}, Token: "reader"})
			require.NoError(t, err)
			defer guard.Unsubscribe()
			for {
				event, err := guard.Next(ctx)
				require.NoError(t, err)
				if event.IsEndOfSnapshot() {
					break
				}
			}
			auth := &registrationACLResolver{deleted: deleted}
			backend := registrationACLBackend{publisher: publisher, before: func() error {
				auth.revoked.Store(true)
				publisher.Publish([]stream.Event{stream.NewCloseSubscriptionEvent([]string{"reader"})})
				_, err := guard.Next(ctx)
				if err != stream.ErrACLChanged {
					return fmt.Errorf("invalidation barrier: %w", err)
				}
				return nil
			}}
			view := newTestView()
			mat := NewLocalMaterializer(LocalMaterializerDeps{Backend: backend, ACLResolver: auth, Deps: Deps{
				Logger: hclog.NewNullLogger(), View: view, Request: func(index uint64) *pbsubscribe.SubscribeRequest {
					return &pbsubscribe.SubscribeRequest{Topic: topic, Token: "reader", Index: index, Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}}}
				},
			}})
			done := make(chan struct{})
			go func() { defer close(done); mat.Run(ctx) }()
			defer func() { cancel(); <-done }()
			select {
			case events := <-view.eventsCh:
				require.False(t, deleted, "a deleted token must terminate instead of publishing its snapshot")
				require.Empty(t, events, "an invalidation drained before registration must not leak the now-denied snapshot")
			case <-done:
				require.True(t, deleted, "denied reads must keep a filtered subscription")
				_, err := mat.Query(ctx, 0)
				require.ErrorIs(t, err, acl.ErrNotFound, "post-registration token deletion must reach readers before terminal exit")
			case <-ctx.Done():
				t.Fatal("materializer neither delivered its filtered snapshot nor rejected the deleted token")
			}
		})
	}
}

type registrationACLResolver struct {
	revoked atomic.Bool
	deleted bool
}

func (r *registrationACLResolver) ResolveTokenAndDefaultMeta(string, *acl.EnterpriseMeta, *acl.AuthorizerContext) (resolver.Result, error) {
	if r.revoked.Load() {
		if r.deleted {
			return resolver.Result{}, acl.ErrNotFound
		}
		return resolver.Result{Authorizer: acl.DenyAll()}, nil
	}
	return resolver.Result{Authorizer: acl.AllowAll()}, nil
}

type registrationACLBackend struct {
	publisher *stream.EventPublisher
	before    func() error
}

func (b registrationACLBackend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	if err := b.before(); err != nil {
		return nil, err
	}
	return b.publisher.Subscribe(req)
}
