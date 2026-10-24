// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package submatview

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/hashicorp/consul/lib/retry"
	"github.com/hashicorp/consul/proto/private/pbservice"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestRPCMaterializer_CancelIncompleteSnapshot(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		name := "initial"
		if replacement {
			name = "replacement"
		}
		t.Run(name, func(t *testing.T) {
			for range 50 {
				client := &cancellationStreamClient{
					events:    make(chan eventOrErr),
					receiving: make(chan struct{}),
				}
				mat := NewRPCMaterializer(client, Deps{
					View:   &fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)},
					Logger: hclog.NewNullLogger(),
					Request: func(index uint64) *pbsubscribe.SubscribeRequest {
						return &pbsubscribe.SubscribeRequest{Index: index}
					},
				})
				if replacement {
					require.NoError(t, mat.updateView(nil, 100))
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); mat.Run(ctx) }()
				<-client.receiving
				send := func(event *pbsubscribe.Event) {
					client.events <- eventOrErr{Event: event}
					<-client.receiving
				}
				if replacement {
					send(newNewSnapshotToFollowEvent())
				}
				send(newEventServiceHealthRegister(50, 1, "web"))
				cancel()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("cancellation did not stop snapshot receive")
				}
				result, err := mat.Query(ctx, 0)
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, result.Index)
				require.Empty(t, result.Value.(fakeResult).srvs,
					"cancelling an incomplete snapshot must not publish its partial data")
			}
		})
	}
}

func TestRPCMaterializer_CancelRetryBackoff(t *testing.T) {
	for range 50 {
		client := &cancellationStreamClient{
			events:    make(chan eventOrErr),
			receiving: make(chan struct{}),
		}
		backingOff := make(chan struct{})
		mat := NewRPCMaterializer(client, Deps{
			View:   &fakeView{srvs: make(map[string]*pbservice.CheckServiceNode)},
			Logger: hclog.NewNullLogger(),
			Request: func(index uint64) *pbsubscribe.SubscribeRequest {
				return &pbsubscribe.SubscribeRequest{Index: index}
			},
			Waiter: &retry.Waiter{
				Factor: time.Hour,
				Jitter: func(delay time.Duration) time.Duration {
					close(backingOff)
					return delay
				},
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); mat.Run(ctx) }()
		<-client.receiving
		client.events <- eventOrErr{Err: io.EOF}
		<-backingOff
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("cancellation did not interrupt retry backoff")
		}
	}
}

type cancellationStreamClient struct {
	events    chan eventOrErr
	receiving chan struct{}
}

func (c *cancellationStreamClient) Subscribe(ctx context.Context, _ *pbsubscribe.SubscribeRequest, _ ...grpc.CallOption) (pbsubscribe.StateChangeSubscription_SubscribeClient, error) {
	return &cancellationSubscribeClient{ctx: ctx, client: c}, nil
}

type cancellationSubscribeClient struct {
	grpc.ClientStream
	ctx    context.Context
	client *cancellationStreamClient
}

func (c *cancellationSubscribeClient) Recv() (*pbsubscribe.Event, error) {
	select {
	case c.client.receiving <- struct{}{}:
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
	select {
	case event := <-c.client.events:
		return event.Event, event.Err
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	}
}
