// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

func TestServer_Subscribe_WorkerPanicIsRecovered(t *testing.T) {
	if os.Getenv("CONSUL_SUBSCRIBE_PANIC_CHILD") != "1" {
		// A panic escaping the worker would terminate the test process itself.
		cmd := exec.Command(os.Args[0], "-test.run=^TestServer_Subscribe_WorkerPanicIsRecovered$", "-test.timeout=15s")
		cmd.Env = append(os.Environ(), "CONSUL_SUBSCRIBE_PANIC_CHILD=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}

	raft := &fakeRaft{}
	raft.contact.Store(time.Now().UnixNano())
	backend := &leaderContactBackend{
		testBackend: newTestBackend(t),
		watcher:     NewLeaderContactWatcher(raft.isLeader, raft.lastContact),
	}
	backend.authorizer = func(string, *acl.EnterpriseMeta) acl.Authorizer {
		return panicAuthorizer{Authorizer: acl.AllowAll()}
	}
	require.NoError(t, backend.store.EnsureRegistration(10, &structs.RegisterRequest{
		Node: "node1", Address: "127.0.0.1",
		Service: &structs.NodeService{ID: "web", Service: "web", Port: 8080},
	}))
	addr := runTestServer(t, NewServer(backend, hclog.NewNullLogger()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(addr.String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	handle, err := pbsubscribe.NewStateChangeSubscriptionClient(conn).Subscribe(ctx, &pbsubscribe.SubscribeRequest{
		Topic:   pbsubscribe.Topic_ServiceHealth,
		Subject: &pbsubscribe.SubscribeRequest_NamedSubject{NamedSubject: &pbsubscribe.NamedSubject{Key: "web"}},
	})
	require.NoError(t, err)
	_, err = handle.Recv()
	require.Equal(t, codes.Internal, status.Code(err))
	require.Contains(t, err.Error(), "panic serving request")
}

type panicAuthorizer struct{ acl.Authorizer }

func (panicAuthorizer) ServiceRead(string, *acl.AuthorizerContext) acl.EnforcementDecision {
	panic("subscription authorization panic")
}
