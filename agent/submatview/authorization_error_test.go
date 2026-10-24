// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/proto/private/pbservice"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

var authorizationQueryCases = []struct{ snapshotIndex, minIndex uint64 }{
	{100, 100},
	{100, 101},
	{0, 0},
}

func TestStore_Get_RecordedAuthorizationError(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated} {
			for _, tc := range authorizationQueryCases {
				name := fmt.Sprintf("local=%t/%s/snapshot=%d/min=%d", local, code, tc.snapshotIndex, tc.minIndex)
				t.Run(name, func(t *testing.T) {
					store, core, req := newAuthorizationTestStore(t, local, tc.snapshotIndex, tc.minIndex)
					authErr := status.Error(code, "authorization revoked")
					core.handleError(&pbsubscribe.SubscribeRequest{}, authErr)
					ctx := &controlledDeadlineContext{
						Context: context.Background(),
						waiting: make(chan struct{}, 1),
						expired: make(chan struct{}),
					}
					resultCh := make(chan resultOrError, 1)
					go func() {
						result, err := store.Get(observeStoreQueries(store, req, ctx), req)
						resultCh <- resultOrError{Result: result, Err: err}
					}()
					var got resultOrError
					select {
					case got = <-resultCh:
					case <-ctx.waiting:
						t.Error("Get waited despite a recorded authorization error")
						close(ctx.expired)
						got = <-resultCh
					case <-time.After(time.Second):
						t.Fatal("Get did not return or enter its deadline wait")
					}
					require.ErrorIs(t, got.Err, authErr)
					require.Equal(t, tc.snapshotIndex, got.Result.Index)
				})
			}
		}
	}
}

func TestStore_Notify_RecordedAuthorizationError(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated} {
			for _, tc := range authorizationQueryCases {
				name := fmt.Sprintf("local=%t/%s/snapshot=%d/min=%d", local, code, tc.snapshotIndex, tc.minIndex)
				t.Run(name, func(t *testing.T) {
					store, core, req := newAuthorizationTestStore(t, local, tc.snapshotIndex, tc.minIndex)
					authErr := status.Error(code, "authorization revoked")
					core.handleError(&pbsubscribe.SubscribeRequest{}, authErr)
					ctx := &controlledDeadlineContext{
						Context: context.Background(),
						waiting: make(chan struct{}, 1),
						expired: make(chan struct{}),
					}
					defer close(ctx.expired)
					callbackEntered := make(chan struct{})
					releaseCallback := make(chan struct{})
					updates := make(chan cache.UpdateEvent, 3)
					first := true
					require.NoError(t, store.NotifyCallback(observeStoreQueries(store, req, ctx), req, "web", func(_ context.Context, update cache.UpdateEvent) {
						if first {
							first = false
							close(callbackEntered)
							select {
							case <-releaseCallback:
							case <-ctx.expired:
								return
							}
						}
						select {
						case updates <- update:
						case <-ctx.expired:
						}
					}))
					select {
					case <-callbackEntered:
					case <-ctx.waiting:
						t.Fatal("Notify waited instead of delivering the recorded authorization error")
					case <-time.After(time.Second):
						t.Fatal("recorded authorization error did not reach the callback")
					}
					nextErr := status.Error(code, "authorization still revoked")
					core.handleError(&pbsubscribe.SubscribeRequest{}, nextErr)
					close(releaseCallback)
					for _, want := range []error{authErr, nextErr} {
						select {
						case update := <-updates:
							require.ErrorIs(t, update.Err, want)
						case <-time.After(time.Second):
							t.Fatal("authorization change during callback was lost")
						}
					}
					waitForMaterializerQuery(t, ctx)
					select {
					case update := <-updates:
						t.Fatalf("recorded error was delivered again without a new notification: %+v", update)
					default:
					}
					nextIndex := tc.snapshotIndex + 1
					require.NoError(t, core.updateView([]*pbsubscribe.Event{
						newEventServiceHealthRegister(nextIndex, 2, "web"),
					}, nextIndex))
					select {
					case update := <-updates:
						require.NoError(t, update.Err)
						require.Equal(t, nextIndex, update.Meta.Index)
					case <-time.After(time.Second):
						t.Fatal("Notify did not recover after a successful update")
					}
				})
			}
		}
	}
}

func newAuthorizationTestStore(t *testing.T, local bool, snapshotIndex, minIndex uint64) (*Store, *materializer, *fakeRPCRequest) {
	t.Helper()
	core := newMaterializer(hclog.NewNullLogger(),
		&fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)}, nil)
	if snapshotIndex > 0 {
		require.NoError(t, core.updateView([]*pbsubscribe.Event{
			newEventServiceHealthRegister(snapshotIndex, 1, "web"),
		}, snapshotIndex))
	}
	var mat Materializer = &RPCMaterializer{mat: core}
	if local {
		mat = &LocalMaterializer{mat: core}
	}
	req := &fakeRPCRequest{index: minIndex}
	store := NewStore(hclog.NewNullLogger())
	store.byKey[makeEntryKey(req.Type(), req.CacheInfo())] = entry{materializer: mat, requests: 1}
	return store, core, req
}
