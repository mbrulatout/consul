// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
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
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_ClosureEndsBlockedSend(t *testing.T) {
	for _, action := range []string{"acl invalidation", "token revoked", "force refresh", "publisher shutdown"} {
		for _, watchLeader := range []bool{true, false} {
			backendName := "plain backend"
			if watchLeader {
				backendName = "leader contact"
			}
			for _, blocked := range []bool{false, true} {
				name := action + "/" + backendName + "/unblocked"
				if blocked {
					name = action + "/" + backendName + "/blocked"
				}
				t.Run(name, func(t *testing.T) { testSubscriptionCloseEndsSend(t, action, watchLeader, blocked) })
			}
		}
	}
}

func testSubscriptionCloseEndsSend(t *testing.T, action string, watchLeader, blocked bool) {
	t.Helper()
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	publisher := stream.NewEventPublisher(0)
	tag := "small"
	if blocked {
		tag = strings.Repeat("x", 512*1024)
	}
	require.NoError(t, publisher.RegisterHandler(state.EventTopicServiceHealth, func(req stream.SubscribeRequest, app stream.SnapshotAppender) (uint64, error) {
		app.Append([]stream.Event{{
			Topic: req.Topic, Index: 10,
			Payload: state.EventPayloadCheckServiceNode{
				Op: pbsubscribe.CatalogOp_Register,
				Value: &structs.CheckServiceNode{
					Node:    &structs.Node{Node: "node1", Address: "127.0.0.1"},
					Service: &structs.NodeService{ID: "web", Service: "web", Tags: []string{tag}},
				},
			},
		}})
		return 10, nil
	}, false))
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()
	pubDone := make(chan struct{})
	go func() { defer close(pubDone); publisher.Run(pubCtx) }()
	var revoked atomic.Bool
	raft := &fakeRaft{}
	raft.leader.Store(true)
	var resolutions atomic.Int32
	base := &testBackend{
		publisher: publisher,
		resolveTokenAndDefaultMeta: func(string, *acl.EnterpriseMeta, *acl.AuthorizerContext) (acl.Authorizer, error) {
			resolutions.Add(1)
			if revoked.Load() {
				return nil, status.Error(codes.Unauthenticated, "token revoked")
			}
			return acl.AllowAll(), nil
		},
	}
	watcher := NewLeaderContactWatcher(raft.isLeader, raft.lastContact)
	var backend Backend = base
	if watchLeader {
		backend = &leaderContactBackend{testBackend: base, watcher: watcher}
	}
	guard, err := publisher.Subscribe(&stream.SubscribeRequest{Topic: state.EventTopicServiceHealth, Subject: state.EventSubjectService{Key: "web"}, Token: "reader"})
	require.NoError(t, err)
	defer guard.Unsubscribe()
	for {
		event, err := guard.Next(ctx)
		require.NoError(t, err)
		if event.IsEndOfSnapshot() {
			break
		}
	}
	entered := make(chan struct{}, 2)
	sent := make(chan error, 2)
	returned := make(chan error, 1)
	streamContext := make(chan context.Context, 1)
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		streamContext <- ss.Context()
		err := handler(srv, &observedSendStream{ServerStream: ss, entered: entered, sent: sent})
		returned <- err
		return err
	}))
	defer server.Stop()
	pbsubscribe.RegisterStateChangeSubscriptionServer(server, NewServer(backend, hclog.NewNullLogger()))
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///subscription-close",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithStaticStreamWindowSize(64*1024),
	)
	require.NoError(t, err)
	defer conn.Close()
	handle, err := pbsubscribe.NewStateChangeSubscriptionClient(conn).Subscribe(ctx, &pbsubscribe.SubscribeRequest{
		Topic: pbsubscribe.Topic_ServiceHealth, Token: "reader",
		Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}},
	})
	require.NoError(t, err)
	var transportCtx context.Context
	select {
	case transportCtx = <-streamContext:
	case <-ctx.Done():
		t.Fatal("stream context unavailable")
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("Send not entered")
		}
		if i == 0 || !blocked {
			select {
			case err := <-sent:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("snapshot Send did not complete")
			}
		}
	}
	if blocked {
		select {
		case err := <-sent:
			t.Fatalf("second Send did not block: %v", err)
		default:
		}
	}
	want := codes.Aborted
	switch action {
	case "acl invalidation", "token revoked":
		if action == "token revoked" {
			revoked.Store(true)
			want = codes.Unauthenticated
		}
		publisher.Publish([]stream.Event{stream.NewCloseSubscriptionEvent([]string{"reader"})})
		_, err = guard.Next(ctx)
		require.ErrorIs(t, err, stream.ErrACLChanged)
	case "force refresh":
		require.NoError(t, publisher.RefreshTopic(state.EventTopicServiceHealth))
	case "publisher shutdown":
		want = codes.Unknown
		pubCancel()
		select {
		case <-pubDone:
		case <-ctx.Done():
			t.Fatal("publisher shutdown timed out")
		}
	}
	require.NoError(t, watcher.Context().Err(), "closure must be independent of healthy leader contact")
	select {
	case err := <-returned:
		require.Equal(t, want, status.Code(err))
	case <-time.After(2 * time.Second):
		t.Fatal("closed subscription did not end handler while client was not reading")
	}
	if blocked {
		select {
		case err := <-sent:
			require.Error(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Send survived handler return")
		}
	}
	select {
	case <-transportCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("handler return did not cancel original transport context")
	}
	err = nil
	for err == nil {
		_, err = handle.Recv()
	}
	require.Equal(t, want, status.Code(err))
	if action == "acl invalidation" || action == "token revoked" {
		require.Equal(t, int32(3), resolutions.Load(), "ACL termination must resolve the token once")
	}
}
