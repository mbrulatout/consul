// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package peerstream

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/stream"
)

func TestSubscriptionManager_StopsRootWatchWhenPublisherShutsDown(t *testing.T) {
	publisher := stream.NewEventPublisher(0)
	publisherCtx, stopPublisher := context.WithCancel(context.Background())
	stopPublisher()
	publisher.Run(publisherCtx)

	releaseRetry := make(chan struct{})
	backend := &shutdownRetrySubscriber{
		publisher: publisher, retried: make(chan struct{}), release: releaseRetry,
	}
	ready := make(chan struct{})
	close(ready)
	manager := &subscriptionManager{
		logger: hclog.NewNullLogger(), backend: backend, trustBundlesSubReady: ready,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		manager.notifyRootCAUpdatesForPartition(ctx, nil, "")
	}()
	defer func() {
		cancel()
		close(releaseRetry)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("root watch did not stop after cleanup")
		}
	}()

	select {
	case <-done:
		require.Equal(t, int32(1), backend.calls.Load(), "publisher shutdown must be terminal")
	case <-backend.retried:
		t.Error("root watch retried a publisher that cannot restart")
	case <-time.After(time.Second):
		t.Error("root watch did not stop when its publisher shut down")
	}
}

type shutdownRetrySubscriber struct {
	publisher *stream.EventPublisher
	calls     atomic.Int32
	retried   chan struct{}
	release   <-chan struct{}
}

func (b *shutdownRetrySubscriber) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	if b.calls.Add(1) == 2 {
		close(b.retried)
		<-b.release
	}
	sub, err := b.publisher.Subscribe(req)
	if err != nil {
		return nil, fmt.Errorf("subscription backend: %w", err)
	}
	return sub, nil
}
