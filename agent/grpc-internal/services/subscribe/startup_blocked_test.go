// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_LeaderContactEndsDuringSnapshot(t *testing.T) {
	for _, cleanupBlocked := range []bool{false, true} {
		name := "initial snapshot"
		if cleanupBlocked {
			name = "unrelated snapshot blocks unsubscribe"
		}
		t.Run(name, func(t *testing.T) { testLeaderContactEndsDuringSnapshot(t, cleanupBlocked) })
	}
}

func testLeaderContactEndsDuringSnapshot(t *testing.T, cleanupBlocked bool) {
	t.Helper()
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	publisher := stream.NewEventPublisher(0)
	require.NoError(t, publisher.RegisterHandler(state.EventTopicServiceHealth, func(req stream.SubscribeRequest, _ stream.SnapshotAppender) (uint64, error) {
		if req.Subject.String() == "blocked" {
			close(entered)
			<-release
		}
		return 42, nil
	}, false))
	raft := &fakeRaft{}
	raft.contact.Store(time.Now().UnixNano())
	backend := &leaderContactBackend{
		testBackend: &testBackend{
			publisher:  publisher,
			authorizer: func(string, *acl.EnterpriseMeta) acl.Authorizer { return acl.AllowAll() },
		},
		watcher: NewLeaderContactWatcher(raft.isLeader, raft.lastContact),
	}
	returned := make(chan error, 1)
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		returned <- err
		return err
	}))
	defer server.Stop()
	pbsubscribe.RegisterStateChangeSubscriptionServer(server, NewServer(backend, hclog.NewNullLogger()))
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///snapshot-startup",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
	)
	require.NoError(t, err)
	defer conn.Close()
	key := "blocked"
	if cleanupBlocked {
		key = "fast"
	}
	handle, err := pbsubscribe.NewStateChangeSubscriptionClient(conn).Subscribe(ctx, &pbsubscribe.SubscribeRequest{
		Topic:   pbsubscribe.Topic_ServiceHealth,
		Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: key}},
	})
	require.NoError(t, err)
	slowFinished := make(chan struct{})
	if cleanupBlocked {
		event, err := handle.Recv()
		require.NoError(t, err)
		require.True(t, event.GetEndOfSnapshot())
		go func() {
			defer close(slowFinished)
			sub, err := publisher.Subscribe(&stream.SubscribeRequest{
				Topic: state.EventTopicServiceHealth, Subject: state.EventSubjectService{Key: "blocked"},
			})
			if err == nil {
				<-release
				sub.Unsubscribe()
			}
		}()
	} else {
		close(slowFinished)
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("snapshot did not enter its barrier")
	}
	start := time.Now()
	backend.watcher.check(time.Now().Add(maxLeaderContactAge + time.Second))
	select {
	case err := <-returned:
		require.Equal(t, codes.Unavailable, status.Code(err))
	case <-time.After(2 * time.Second):
		t.Errorf("cancellation cannot return while the snapshot barrier remains closed (waited %s)", time.Since(start))
		unblock()
		select {
		case err := <-returned:
			require.Equal(t, codes.Unavailable, status.Code(err))
		case <-ctx.Done():
			t.Fatal("handler did not return after snapshot release")
		}
	}
	unblock()
	select {
	case <-slowFinished:
	case <-ctx.Done():
		t.Fatal("unrelated snapshot did not clean up")
	}
	_, err = handle.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
}
