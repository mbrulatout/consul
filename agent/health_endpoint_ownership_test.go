// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package agent

import (
	"context"
	"fmt"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/hashicorp/consul/agent/cache"
	"github.com/hashicorp/consul/agent/config"
	"github.com/hashicorp/consul/agent/rpcclient"
	healthclient "github.com/hashicorp/consul/agent/rpcclient/health"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/agent/submatview"
	"github.com/hashicorp/consul/proto/private/pbservice"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHealthServiceNodes_ResponseOwnership(t *testing.T) {
	for _, streaming := range []bool{true, false} {
		for _, tags := range [][]string{{"ordinary"}, nil} {
			name := fmt.Sprintf("streaming=%t/service-tags=%v", streaming, tags)
			t.Run(name, func(t *testing.T) {
				handler, source := newHealthAliasFixture(t, streaming, tags, 1)
				before := source.cached.Nodes.DeepCopy()
				out, err := handler.HealthServiceNodes(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/health/service/web?cached&token=reader", nil))
				require.NoError(t, err)
				nodes := out.(structs.CheckServiceNodes)
				require.Len(t, nodes, 1)
				require.NotNil(t, nodes[0].Checks[0].ServiceTags)
				require.Equal(t, 8080, nodes[0].Service.Port)
				fresh := source.view.Result(10).(*structs.IndexedCheckServiceNodes)
				assert.Nil(t, fresh.Nodes[0].Checks[0].ServiceTags, "HTTP normalization mutated the materialized node check")
				assert.Zero(t, fresh.Nodes[0].Service.Port, "HTTP default-port normalization mutated the materialized service")
				assert.Equal(t, before, source.cached.Nodes, "HTTP normalization mutated a previously returned/cached result")
			})
		}
	}
}

func TestHealthServiceNodes_ConcurrentResponseNormalization(t *testing.T) {
	for _, streaming := range []bool{true, false} {
		t.Run(fmt.Sprintf("streaming=%t", streaming), func(t *testing.T) {
			handler, source := newHealthAliasFixture(t, streaming, []string{"ordinary"}, 32)
			const readers = 64
			source.ready = make(chan struct{}, readers)
			source.start = make(chan struct{})
			errors := make(chan error, readers)
			for i := 0; i < readers; i++ {
				go func() {
					_, err := handler.HealthServiceNodes(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/health/service/web?cached&token=reader", nil))
					errors <- err
				}()
			}
			for i := 0; i < readers; i++ {
				<-source.ready
			}
			close(source.start)
			for i := 0; i < readers; i++ {
				require.NoError(t, <-errors)
			}
		})
	}
}

type healthAliasSource struct {
	mu     sync.Mutex
	view   *healthclient.HealthView
	cached *structs.IndexedCheckServiceNodes
	ready  chan struct{}
	start  chan struct{}
}

func (s *healthAliasSource) result(streaming bool) *structs.IndexedCheckServiceNodes {
	s.mu.Lock()
	result := s.cached
	if streaming {
		result = s.view.Result(10).(*structs.IndexedCheckServiceNodes)
	}
	s.mu.Unlock()
	if s.ready != nil {
		s.ready <- struct{}{}
		<-s.start
	}
	return result
}

type healthAliasStore struct{ source *healthAliasSource }

func (s healthAliasStore) Get(context.Context, submatview.Request) (submatview.Result, error) {
	return submatview.Result{Index: 10, Cached: true, Value: s.source.result(true)}, nil
}
func (s healthAliasStore) NotifyCallback(context.Context, submatview.Request, string, cache.Callback) error {
	panic("unused")
}

type healthAliasCache struct{ source *healthAliasSource }

func (s healthAliasCache) Get(context.Context, string, cache.Request) (interface{}, cache.ResultMeta, error) {
	return s.source.result(false), cache.ResultMeta{Index: 10, Hit: true}, nil
}
func (s healthAliasCache) NotifyCallback(context.Context, string, cache.Request, string, cache.Callback) error {
	panic("unused")
}

func newHealthAliasFixture(t *testing.T, streaming bool, tags []string, count int) (*HTTPHandlers, *healthAliasSource) {
	t.Helper()
	view, err := healthclient.NewHealthView(structs.ServiceSpecificRequest{ServiceName: "web"})
	require.NoError(t, err)
	for i := 0; i < count; i++ {
		node := &structs.CheckServiceNode{
			Node: &structs.Node{Node: fmt.Sprintf("node%d", i), Address: "127.0.0.1"},
			Service: &structs.NodeService{
				ID: "web", Service: "web", Tags: tags,
				Ports: structs.ServicePorts{{Name: "http", Port: 8080, Default: true}},
			},
			Checks: structs.HealthChecks{{Node: fmt.Sprintf("node%d", i), CheckID: "serfHealth", Status: "passing"}},
		}
		require.NoError(t, view.Update([]*pbsubscribe.Event{{
			Index: 10,
			Payload: &pbsubscribe.Event_ServiceHealth{ServiceHealth: &pbsubscribe.ServiceHealthUpdate{
				Op: pbsubscribe.CatalogOp_Register, CheckServiceNode: pbservice.NewCheckServiceNodeFromStructs(node),
			}},
		}}))
	}
	source := &healthAliasSource{view: view, cached: view.Result(10).(*structs.IndexedCheckServiceNodes)}
	cfg := &config.RuntimeConfig{Datacenter: "dc1", HTTPUseCache: true}
	client := &healthclient.Client{Client: rpcclient.Client{
		UseStreamingBackend: streaming,
		ViewStore:           healthAliasStore{source}, Cache: healthAliasCache{source},
		QueryOptionDefaults: config.ApplyDefaultQueryOptions(cfg),
	}}
	return &HTTPHandlers{agent: &Agent{config: cfg, rpcClientHealth: client}}, source
}
