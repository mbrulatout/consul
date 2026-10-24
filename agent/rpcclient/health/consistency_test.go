// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package health_test

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent"
	"github.com/hashicorp/consul/testrpc"
)

func TestServiceNodes_ConsistentBlockingQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("starts a real agent")
	}
	a := agent.NewTestAgent(t, `
		use_streaming_backend = true
		rpc { enable_streaming = true }
	`)
	testrpc.WaitForLeader(t, a.RPC, "dc1")
	client := &http.Client{Timeout: 10 * time.Second}
	for _, tc := range []struct {
		name, query, backend string
	}{
		{"default", "index=1&wait=5s", "streaming"},
		{"consistent", "consistent&index=1&wait=5s", "blocking-query"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := client.Get("http://" + a.HTTPAddr() + "/v1/health/service/consul?" + tc.query)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s", body)
			require.Equal(t, tc.backend, resp.Header.Get("X-Consul-Query-Backend"))
			if tc.name == "consistent" {
				require.Equal(t, "consistent", resp.Header.Get("X-Consul-Effective-Consistency"))
			}
		})
	}
}
