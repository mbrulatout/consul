// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"errors"
	"net"
	"strings"
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

	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_CancellationEndsBlockedSend(t *testing.T) {
	for _, leaderContact := range []bool{true, false} {
		name := "client cancellation"
		if leaderContact {
			name = "leader contact expires"
		}
		t.Run(name, func(t *testing.T) { testSubscribeCancellationEndsBlockedSend(t, leaderContact) })
	}
}

func testSubscribeCancellationEndsBlockedSend(t *testing.T, leaderContact bool) {
	t.Helper()
	ignore := goleak.IgnoreCurrent()
	t.Cleanup(func() { goleak.VerifyNone(t, ignore) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	raft := &fakeRaft{}
	raft.contact.Store(time.Now().UnixNano())
	backend := &blockedSendBackend{
		leaderContactBackend: &leaderContactBackend{
			testBackend: newTestBackend(t),
			watcher:     NewLeaderContactWatcher(raft.isLeader, raft.lastContact),
		},
		subscribed: make(chan *stream.Subscription, 1),
	}
	require.NoError(t, backend.store.EnsureRegistration(10, &structs.RegisterRequest{
		Node: "node1", Address: "127.0.0.1",
		Service: &structs.NodeService{
			ID: "web", Service: "web", Port: 8080,
			Tags: []string{strings.Repeat("x", 512*1024)},
		},
	}))

	entered := make(chan struct{}, 2)
	sent := make(chan error, 2)
	returned := make(chan error, 1)
	server := grpc.NewServer(grpc.StreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, &observedSendStream{ServerStream: ss, entered: entered, sent: sent})
		returned <- err
		return err
	}))
	defer server.Stop()
	pbsubscribe.RegisterStateChangeSubscriptionServer(server, NewServer(backend, hclog.NewNullLogger()))
	listener := bufconn.Listen(1024 * 1024)
	defer listener.Close()
	go server.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///blocked-send",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithStaticStreamWindowSize(64*1024),
	)
	require.NoError(t, err)
	defer conn.Close()
	handle, err := pbsubscribe.NewStateChangeSubscriptionClient(conn).Subscribe(ctx, &pbsubscribe.SubscribeRequest{
		Topic:   pbsubscribe.Topic_ServiceHealth,
		Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}},
	})
	require.NoError(t, err)
	var sub *stream.Subscription
	select {
	case sub = <-backend.subscribed:
	case <-ctx.Done():
		t.Fatal("subscription was not registered")
	}

	// The unread oversized event exhausts the fixed stream window and write
	// quota, so the end-of-snapshot Send cannot complete.
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("snapshot did not reach Send")
		}
		if i == 0 {
			select {
			case err := <-sent:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("first Send did not enqueue the oversized event")
			}
		}
	}
	select {
	case err := <-sent:
		t.Fatalf("flow control did not block the second Send: %v", err)
	default:
	}
	wantStatus := codes.Canceled
	if leaderContact {
		wantStatus = codes.Unavailable
		backend.watcher.check(time.Now().Add(maxLeaderContactAge + time.Second))
	} else {
		cancel()
	}
	select {
	case err := <-returned:
		if leaderContact {
			require.Equal(t, wantStatus, status.Code(err))
		} else {
			require.True(t, errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled, "handler returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not end the handler while Send was blocked")
	}
	select {
	case err := <-sent:
		require.Error(t, err, "handler return must cancel the transport's blocked Send")
	case <-time.After(2 * time.Second):
		t.Fatal("blocked Send goroutine survived handler return")
	}
	_, err = sub.Next(ctx)
	require.ErrorContains(t, err, "closed by unsubscribe")
	err = nil
	for err == nil {
		_, err = handle.Recv()
	}
	require.Equal(t, wantStatus, status.Code(err))
}

type blockedSendBackend struct {
	*leaderContactBackend
	subscribed chan *stream.Subscription
}

func (b *blockedSendBackend) Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error) {
	sub, err := b.leaderContactBackend.Subscribe(req)
	if err == nil {
		b.subscribed <- sub
	}
	return sub, err
}

type observedSendStream struct {
	grpc.ServerStream
	entered chan struct{}
	sent    chan error
}

func (s *observedSendStream) SendMsg(message interface{}) error {
	s.entered <- struct{}{}
	err := s.ServerStream.SendMsg(message)
	s.sent <- err
	return err
}
