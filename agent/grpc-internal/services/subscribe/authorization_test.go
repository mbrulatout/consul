// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_ACLChangeBeforeRegistration(t *testing.T) {
	for _, deleteToken := range []bool{false, true} {
		name := "revoke read permission"
		if deleteToken {
			name = "delete token"
		}
		t.Run(name, func(t *testing.T) {
			backend := newTestBackend(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			require.NoError(t, backend.store.EnsureRegistration(10, &structs.RegisterRequest{
				Node: "node1", Address: "127.0.0.1",
				Service: &structs.NodeService{ID: "web", Service: "web", Port: 8080},
			}))
			guard, err := backend.publisher.Subscribe(&stream.SubscribeRequest{
				Topic: state.EventTopicServiceHealth, Subject: state.EventSubjectService{Key: "sync"}, Token: "reader",
			})
			require.NoError(t, err)
			defer guard.Unsubscribe()
			event, err := guard.Next(ctx)
			require.NoError(t, err)
			require.True(t, event.IsEndOfSnapshot())

			var revoked atomic.Bool
			backend.resolveTokenAndDefaultMeta = func(string, *acl.EnterpriseMeta, *acl.AuthorizerContext) (acl.Authorizer, error) {
				if !revoked.Load() {
					return acl.AllowAll(), nil
				}
				if deleteToken {
					return nil, status.Error(codes.Unauthenticated, "token revoked")
				}
				return acl.DenyAll(), nil
			}
			wrapped := &beforeSubscribeBackend{Backend: backend, before: func() {
				revoked.Store(true)
				backend.publisher.Publish([]stream.Event{stream.NewCloseSubscriptionEvent([]string{"reader"})})
				_, err := guard.Next(ctx)
				require.ErrorIs(t, err, stream.ErrACLChanged)
			}}
			addr := runTestServer(t, NewServer(wrapped, hclog.NewNullLogger()))
			conn, err := gogrpc.DialContext(ctx, addr.String(), gogrpc.WithInsecure()) //nolint:staticcheck
			require.NoError(t, err)
			defer conn.Close()
			client := pbsubscribe.NewStateChangeSubscriptionClient(conn)
			handle, err := client.Subscribe(ctx, &pbsubscribe.SubscribeRequest{
				Topic: pbsubscribe.Topic_ServiceHealth, Token: "reader",
				Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}},
			})
			require.NoError(t, err)
			received, err := handle.Recv()
			if deleteToken {
				require.Equal(t, codes.Unauthenticated, status.Code(err), "a token invalidated before registration must be rejected")
			} else {
				require.NoError(t, err)
				require.True(t, received.GetEndOfSnapshot(), "a missed invalidation must not leak the now-denied service: %v", received)
			}
		})
	}
}

type beforeSubscribeBackend struct {
	Backend
	before func()
}

func (b *beforeSubscribeBackend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	b.before()
	return b.Backend.Subscribe(req)
}
