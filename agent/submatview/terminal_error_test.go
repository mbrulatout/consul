// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/acl/resolver"
	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/proto/private/pbservice"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestLocalMaterializer_QueryTerminalAuthorizationError(t *testing.T) {
	for _, snapshotIndex := range []uint64{0, 100} {
		for _, waiting := range []bool{false, true} {
			t.Run(fmt.Sprintf("snapshot=%d/waiting=%t", snapshotIndex, waiting), func(t *testing.T) {
				aclResolver := NewMockACLResolver(t)
				aclResolver.On("ResolveTokenAndDefaultMeta", "deleted-token", mock.Anything, mock.Anything).
					Return(resolver.Result{}, acl.ErrNotFound).Once()
				mat := NewLocalMaterializer(LocalMaterializerDeps{
					ACLResolver: aclResolver,
					Deps: Deps{
						Logger: hclog.NewNullLogger(),
						View:   &fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)},
						Request: func(index uint64) *pbsubscribe.SubscribeRequest {
							return &pbsubscribe.SubscribeRequest{Token: "deleted-token", Index: index}
						},
					},
				})
				if snapshotIndex > 0 {
					require.NoError(t, mat.updateView([]*pbsubscribe.Event{
						newEventServiceHealthRegister(snapshotIndex, 1, "web"),
					}, snapshotIndex))
				}
				ctx := &controlledDeadlineContext{
					Context: context.Background(),
					waiting: make(chan struct{}, 1),
					expired: make(chan struct{}),
				}
				results := make(chan resultOrError, 1)
				query := func() {
					result, err := mat.Query(ctx, snapshotIndex)
					results <- resultOrError{Result: result, Err: err}
				}
				if waiting {
					go query()
					waitForMaterializerQuery(t, ctx)
				}
				mat.Run(context.Background())
				if !waiting {
					go query()
				}
				close(ctx.expired)
				got := <-results
				require.ErrorIs(t, got.Err, acl.ErrNotFound,
					"terminal ACL failures must wake readers instead of disappearing when Run exits")
				require.Equal(t, snapshotIndex, got.Result.Index)
			})
		}
	}
}

func TestLocalMaterializer_NotifyTerminalAuthorizationError(t *testing.T) {
	for _, snapshotIndex := range []uint64{0, 100} {
		t.Run(fmt.Sprintf("snapshot=%d", snapshotIndex), func(t *testing.T) {
			store, _, req := newAuthorizationTestStore(t, true, snapshotIndex, snapshotIndex)
			aclResolver := NewMockACLResolver(t)
			aclResolver.On("ResolveTokenAndDefaultMeta", "deleted-token", mock.Anything, mock.Anything).
				Return(resolver.Result{}, acl.ErrNotFound).Once()
			key := makeEntryKey(req.Type(), req.CacheInfo())
			e := store.byKey[key]
			e.requests = 0
			store.byKey[key] = e
			mat := e.materializer.(*LocalMaterializer)
			mat.deps.ACLResolver = aclResolver
			mat.deps.Request = func(index uint64) *pbsubscribe.SubscribeRequest {
				return &pbsubscribe.SubscribeRequest{Token: "deleted-token", Index: index}
			}
			ctx := &controlledDeadlineContext{
				Context: context.Background(),
				waiting: make(chan struct{}, 1),
				expired: make(chan struct{}),
			}
			updates := make(chan cache.UpdateEvent, 2)
			require.NoError(t, store.Notify(observeStoreQueries(store, req, ctx), req, "web", updates))
			defer func() {
				close(ctx.expired)
				select {
				case <-store.expiryHeap.NotifyCh:
				case <-time.After(time.Second):
					t.Error("cancelled watcher did not release its store entry")
				}
			}()
			waitForMaterializerQuery(t, ctx)
			mat.Run(context.Background())
			select {
			case update := <-updates:
				require.ErrorIs(t, update.Err, acl.ErrNotFound)
			case <-time.After(time.Second):
				t.Fatal("terminal ACL failure did not reach the active watcher")
			}
			waitForMaterializerQuery(t, ctx)
			select {
			case update := <-updates:
				t.Fatalf("terminal error repeated without another failure: %+v", update)
			default:
			}
		})
	}
}
