// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package router_test

import (
	"net"
	"sync"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/metadata"
	"github.com/hashicorp/consul/agent/router"
)

type rebalancePingFunc func(string, string, net.Addr) (bool, error)

func (f rebalancePingFunc) Ping(dc, node string, addr net.Addr) (bool, error) {
	return f(dc, node, addr)
}

func TestManager_RebalanceServers_PreservesReadersSnapshot(t *testing.T) {
	servers := []*metadata.Server{
		{Name: "s1", Addr: &fauxAddr{"s1"}},
		{Name: "s2", Addr: &fauxAddr{"s2"}},
		{Name: "s3", Addr: &fauxAddr{"s3"}},
	}
	var manager *router.Manager
	manager = router.New(hclog.NewNullLogger(), make(chan struct{}), &fauxSerf{}, rebalancePingFunc(func(_, _ string, selected net.Addr) (bool, error) {
		for _, server := range servers {
			if server.Addr.String() != selected.String() {
				manager.RemoveServer(server)
				break
			}
		}
		return true, nil
	}), "", noopRebalancer)
	for _, server := range servers {
		manager.AddServer(server)
	}

	entered, resume := make(chan struct{}), make(chan struct{})
	observed := make(chan []string, 1)
	go func() {
		var once sync.Once
		var snapshot []string
		manager.CheckServers(func(server *metadata.Server) bool {
			once.Do(func() {
				close(entered)
				<-resume
			})
			if server == nil {
				snapshot = append(snapshot, "<nil>")
			} else {
				snapshot = append(snapshot, server.Name)
			}
			return true
		})
		observed <- snapshot
	}()
	<-entered
	manager.RebalanceServers()
	close(resume)

	var names []string
	for _, server := range servers {
		names = append(names, server.Name)
	}
	require.ElementsMatch(t, names, <-observed, "an active reader must retain its complete original server snapshot")
	require.Equal(t, 2, manager.NumServers())
	require.NotNil(t, manager.FindServer())
}

func TestManager_RebalanceServers_ConcurrentReaders(t *testing.T) {
	manager := router.New(hclog.NewNullLogger(), make(chan struct{}), &fauxSerf{}, &fauxConnPool{}, "", noopRebalancer)
	for _, name := range []string{"s1", "s2", "s3"} {
		manager.AddServer(&metadata.Server{Name: name})
	}
	start, done := make(chan struct{}), make(chan struct{})
	go func() {
		<-start
		for i := 0; i < 1000; i++ {
			manager.RebalanceServers()
		}
		close(done)
	}()
	close(start)
	for {
		select {
		case <-done:
			return
		default:
			require.NotNil(t, manager.FindServer())
		}
	}
}
