// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package stream

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEventPublisher_SnapshotTTLDoesNotEvictReplacement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		publisher := NewEventPublisher(10 * time.Second)
		snapshots := 0
		require.NoError(t, publisher.RegisterHandler(testTopic, func(_ SubscribeRequest, app SnapshotAppender) (uint64, error) {
			snapshots++
			app.Append([]Event{{Topic: testTopic, Index: uint64(snapshots), Payload: simplePayload{key: "key", value: fmt.Sprint(snapshots)}}})
			return uint64(snapshots), nil
		}, false))
		subscribe := func() *Subscription {
			sub, err := publisher.Subscribe(&SubscribeRequest{Topic: testTopic, Subject: StringSubject("key")})
			require.NoError(t, err)
			return sub
		}
		original := subscribe()
		defer original.Unsubscribe()
		time.Sleep(5 * time.Second)
		require.NoError(t, publisher.RefreshTopic(testTopic))
		replacement := subscribe()
		defer replacement.Unsubscribe()
		got, err := replacement.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "2", got.Payload.(simplePayload).value)

		time.Sleep(5 * time.Second)
		synctest.Wait()
		cached := subscribe()
		defer cached.Unsubscribe()
		got, err = cached.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "2", got.Payload.(simplePayload).value, "the old deadline must not evict a replacement snapshot")
		require.Equal(t, 2, snapshots)

		time.Sleep(5 * time.Second)
		synctest.Wait()
		expired := subscribe()
		defer expired.Unsubscribe()
		got, err = expired.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, "3", got.Payload.(simplePayload).value, "the replacement must still expire at its own deadline")
		require.Equal(t, 3, snapshots)
	})
}
