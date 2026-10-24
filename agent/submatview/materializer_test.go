// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

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
