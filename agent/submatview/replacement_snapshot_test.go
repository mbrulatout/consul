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

func TestStore_Notify_ReplacementSnapshot(t *testing.T) {
	for _, resetPath := range []string{"snapshot marker", "first aborted stream"} {
		for _, index := range []uint64{100, 50} {
			for _, startBeforeReset := range []bool{true, false} {
				name := fmt.Sprintf("%s/index=%d/startBeforeReset=%t", resetPath, index, startBeforeReset)
				t.Run(name, func(t *testing.T) {
					req := &fakeRPCRequest{index: 100}
					mat := &RPCMaterializer{mat: newMaterializer(hclog.NewNullLogger(),
						&fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)}, nil)}
					require.NoError(t, mat.mat.updateView([]*pbsubscribe.Event{
						newEventServiceHealthRegister(100, 1, "web"),
					}, 100))
					store := NewStore(hclog.NewNullLogger())
					store.byKey[makeEntryKey(req.Type(), req.CacheInfo())] = entry{materializer: mat, requests: 1}
					ctx := &controlledDeadlineContext{
						Context: context.Background(),
						waiting: make(chan struct{}, 1),
						expired: make(chan struct{}),
					}
					defer close(ctx.expired)
					updates := make(chan cache.UpdateEvent, 3)
					startWatch := func() {
						require.NoError(t, store.NotifyCallback(ctx, req, "web", func(_ context.Context, update cache.UpdateEvent) {
							select {
							case updates <- update:
							case <-ctx.expired:
							}
						}))
						waitForMaterializerQuery(t, ctx)
					}
					if startBeforeReset {
						startWatch()
					}

					handler := initialHandler(100)
					switch resetPath {
					case "snapshot marker":
						var err error
						handler, err = handler(mat, newNewSnapshotToFollowEvent())
						require.NoError(t, err)
					case "first aborted stream":
						client := NewTestStreamingClient("")
						client.QueueErr(status.Error(codes.Aborted, "reset stream"))
						mat.client = client
						subscribeReq := &pbsubscribe.SubscribeRequest{Index: 100}
						err := mat.subscribeOnce(context.Background(), subscribeReq)
						require.Error(t, err)
						mat.mat.handleError(subscribeReq, err)
						handler = initialHandler(mat.mat.currentIndex())
					}
					if !startBeforeReset {
						startWatch()
					}

					var err error
					handler, err = handler(mat, newEventServiceHealthRegister(index, 2, "web"))
					require.NoError(t, err)
					select {
					case update := <-updates:
						t.Fatalf("incomplete replacement snapshot was published: %+v", update)
					default:
					}
					handler, err = handler(mat, newEndOfSnapshotEvent(index))
					require.NoError(t, err)
					select {
					case update := <-updates:
						require.NoError(t, update.Err)
						require.Equal(t, index, update.Meta.Index)
						srvs := update.Result.(fakeResult).srvs
						require.Len(t, srvs, 1)
						require.Equal(t, "node2", srvs[0].Node.Node)
					case <-time.After(time.Second):
						t.Fatal("completed replacement snapshot did not wake the notification")
					}
					waitForMaterializerQuery(t, ctx)

					handler, err = handler(mat, newEventServiceHealthRegister(index, 3, "web"))
					require.NoError(t, err)
					waitForMaterializerQuery(t, ctx)
					select {
					case update := <-updates:
						t.Fatalf("ordinary same-index event woke the notification: %+v", update)
					default:
					}
					_, err = handler(mat, newEventServiceHealthRegister(index+1, 4, "web"))
					require.NoError(t, err)
					select {
					case update := <-updates:
						require.NoError(t, update.Err)
						require.Equal(t, index+1, update.Meta.Index)
						require.Len(t, update.Result.(fakeResult).srvs, 3)
					case <-time.After(time.Second):
						t.Fatal("notification did not continue after the replacement snapshot")
					}
				})
			}
		}
	}
}

func waitForMaterializerQuery(t *testing.T, ctx *controlledDeadlineContext) {
	t.Helper()
	select {
	case <-ctx.waiting:
	case <-time.After(time.Second):
		t.Fatal("notification did not enter a blocking query")
	}
}
