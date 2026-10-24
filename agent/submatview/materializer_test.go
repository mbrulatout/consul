// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
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

func TestStore_Get_DeadlineDuringSnapshotReset(t *testing.T) {
	for _, startBeforeReset := range []bool{false, true} {
		name := "query starts after reset"
		if startBeforeReset {
			name = "query waits across reset"
		}
		t.Run(name, func(t *testing.T) {
			req := &fakeRPCRequest{index: 100}
			mat := &RPCMaterializer{mat: newMaterializer(hclog.NewNullLogger(),
				&fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)}, nil)}
			require.NoError(t, mat.mat.updateView([]*pbsubscribe.Event{
				newEventServiceHealthRegister(100, 1, "web"),
			}, 100))

			store := NewStore(hclog.NewNullLogger())
			key := makeEntryKey(req.Type(), req.CacheInfo())
			store.byKey[key] = entry{materializer: mat, requests: 1}
			ctx := &controlledDeadlineContext{
				Context: context.Background(),
				waiting: make(chan struct{}, 1),
				expired: make(chan struct{}),
			}
			resultCh := make(chan resultOrError, 1)
			if !startBeforeReset {
				mat.mat.reset()
			}
			go func() {
				result, err := store.Get(ctx, req)
				resultCh <- resultOrError{Result: result, Err: err}
			}()
			<-ctx.waiting
			if startBeforeReset {
				mat.mat.reset()
			}
			close(ctx.expired)

			result := <-resultCh
			require.ErrorIs(t, result.Err, context.DeadlineExceeded,
				"an incomplete snapshot must not become a successful empty result")
			require.Zero(t, result.Result.Index)
			require.Zero(t, result.Result.Value.(fakeResult).index)
		})
	}
}

func TestStore_CachedAuthorizationError(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.Unauthenticated} {
		t.Run(code.String(), func(t *testing.T) {
			req := &fakeRPCRequest{}
			mat := &RPCMaterializer{mat: newMaterializer(hclog.NewNullLogger(),
				&fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)}, nil)}
			require.NoError(t, mat.mat.updateView([]*pbsubscribe.Event{
				newEventServiceHealthRegister(100, 1, "web"),
			}, 100))
			authErr := status.Error(code, "authorization revoked")
			mat.mat.handleError(&pbsubscribe.SubscribeRequest{}, authErr)

			store := NewStore(hclog.NewNullLogger())
			key := makeEntryKey(req.Type(), req.CacheInfo())
			store.byKey[key] = entry{materializer: mat, requests: 1}
			result, err := store.Get(context.Background(), req)
			require.ErrorIs(t, err, authErr,
				"cached reads must not hide a recorded authorization failure")
			require.Equal(t, uint64(100), result.Index)

			ctx := &controlledDeadlineContext{
				Context: context.Background(),
				waiting: make(chan struct{}, 1),
				expired: make(chan struct{}),
			}
			defer close(ctx.expired)
			updates := make(chan cache.UpdateEvent, 3)
			require.NoError(t, store.NotifyCallback(ctx, req, "web", func(_ context.Context, update cache.UpdateEvent) {
				select {
				case updates <- update:
				case <-ctx.expired:
				}
			}))
			select {
			case update := <-updates:
				require.ErrorIs(t, update.Err, authErr)
			case <-time.After(time.Second):
				t.Fatal("cached authorization error was not delivered")
			}

			select {
			case <-ctx.waiting:
			case <-time.After(time.Second):
				t.Fatal("notification did not wait after reporting the error")
			}
			select {
			case update := <-updates:
				t.Fatalf("authorization error was emitted again without an update: %+v", update)
			default:
			}

			require.NoError(t, mat.mat.updateView([]*pbsubscribe.Event{
				newEventServiceHealthRegister(101, 2, "web"),
			}, 101))
			select {
			case update := <-updates:
				require.NoError(t, update.Err)
				require.Equal(t, uint64(101), update.Meta.Index)
				require.Len(t, update.Result.(fakeResult).srvs, 2)
			case <-time.After(time.Second):
				t.Fatal("notification did not recover after an authorized update")
			}
			result, err = store.Get(context.Background(), req)
			require.NoError(t, err)
			require.Equal(t, uint64(101), result.Index)
		})
	}
}

type controlledDeadlineContext struct {
	context.Context
	waiting chan struct{}
	expired chan struct{}
}

func (c *controlledDeadlineContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.expired
}

func (c *controlledDeadlineContext) Err() error {
	select {
	case <-c.expired:
		return context.DeadlineExceeded
	default:
		return nil
	}
}
