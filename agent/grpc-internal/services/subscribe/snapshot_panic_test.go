// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_SnapshotPanicIsRecovered(t *testing.T) {
	if os.Getenv("CONSUL_SNAPSHOT_PANIC_CHILD") != "1" {
		// A panic escaping the snapshot worker would terminate the test process.
		cmd := exec.Command(os.Args[0], "-test.run=^TestServer_Subscribe_SnapshotPanicIsRecovered$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), "CONSUL_SNAPSHOT_PANIC_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
	var logs bytes.Buffer
	previousLogger := hclog.SetDefault(hclog.New(&hclog.LoggerOptions{Output: &logs}))
	defer hclog.SetDefault(previousLogger)

	const panicValue = "private snapshot panic payload"
	publisher := stream.NewEventPublisher(0)
	require.NoError(t, publisher.RegisterHandler(state.EventTopicServiceHealth, func(req stream.SubscribeRequest, _ stream.SnapshotAppender) (uint64, error) {
		if req.Subject.String() == "panic" {
			panic(panicValue)
		}
		return 42, nil
	}, false))
	backend := &testBackend{
		publisher:  publisher,
		authorizer: func(string, *acl.EnterpriseMeta) acl.Authorizer { return acl.AllowAll() },
	}
	addr := runTestServer(t, NewServer(backend, hclog.NewNullLogger()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	client := pbsubscribe.NewStateChangeSubscriptionClient(conn)
	subscribe := func(key string) pbsubscribe.StateChangeSubscription_SubscribeClient {
		t.Helper()
		handle, err := client.Subscribe(ctx, &pbsubscribe.SubscribeRequest{
			Topic:   pbsubscribe.Topic_ServiceHealth,
			Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: key}},
		})
		require.NoError(t, err)
		return handle
	}
	_, err = subscribe("panic").Recv()
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), panicValue)
	require.Contains(t, logs.String(), panicValue)
	require.Contains(t, logs.String(), "snapshot_panic_test.go")

	event, err := subscribe("healthy").Recv()
	require.NoError(t, err)
	require.True(t, event.GetEndOfSnapshot())
	require.Equal(t, uint64(42), event.Index)
}
