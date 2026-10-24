// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package netutil

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/require"
)

func TestAgentBindAddrCacheConcurrent(t *testing.T) {
	previous := getCachedBindAddr()
	t.Cleanup(func() { SetAgentBindAddr(&net.IPAddr{IP: previous}) })
	addresses := []net.IP{net.ParseIP("192.0.2.1"), net.ParseIP("2001:db8::1")}
	SetAgentBindAddr(&net.IPAddr{IP: addresses[0]})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"Config":{"BindAddr":"192.0.2.1"}}`)
	}))
	defer server.Close()
	config := api.DefaultConfig()
	config.Address = server.URL
	dtoConfig := api.DefaultConfig()
	dtoConfig.Address = server.URL
	getters := []func() (net.IP, error){
		func() (net.IP, error) { return GetAgentBindAddr(nil, true) },
		func() (net.IP, error) { return GetAgentBindAddrWithDTO(&IPStackRequestDTO{Cached: true}) },
		func() (net.IP, error) { return GetAgentBindAddr(config, false) },
		func() (net.IP, error) { return GetAgentBindAddrWithDTO(&IPStackRequestDTO{Config: dtoConfig}) },
	}
	start := make(chan struct{})
	errors := make(chan error, 4)
	var workers sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 50; i++ {
				SetAgentBindAddr(&net.IPAddr{IP: addresses[i%len(addresses)]})
			}
		}()
	}
	for _, get := range getters {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for i := 0; i < 50; i++ {
				ip, err := get()
				if err != nil {
					errors <- err
					return
				}
				if !ip.Equal(addresses[0]) && !ip.Equal(addresses[1]) {
					errors <- fmt.Errorf("cache returned an incomplete address: %v", ip)
					return
				}
			}
		}()
	}
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}

func TestAgentBindAddrCacheOwnsSlices(t *testing.T) {
	previous := getCachedBindAddr()
	t.Cleanup(func() { SetAgentBindAddr(&net.IPAddr{IP: previous}) })
	for _, tc := range []struct {
		name string
		get  func() (net.IP, error)
	}{
		{"config", func() (net.IP, error) { return GetAgentBindAddr(nil, true) }},
		{"DTO", func() (net.IP, error) { return GetAgentBindAddrWithDTO(&IPStackRequestDTO{Cached: true}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := net.IP{192, 0, 2, 1}
			SetAgentBindAddr(&net.IPAddr{IP: input})
			input[0] = 10
			ip, err := tc.get()
			require.NoError(t, err)
			require.Equal(t, "192.0.2.1", ip.String(), "the setter must not retain the caller's slice")
			ip[0] = 172
			ip, err = tc.get()
			require.NoError(t, err)
			require.Equal(t, "192.0.2.1", ip.String(), "the getter must not expose the cached slice")
		})
	}
}

func TestAgentBindAddrCacheFetchedSlices(t *testing.T) {
	previous := getCachedBindAddr()
	t.Cleanup(func() { SetAgentBindAddr(&net.IPAddr{IP: previous}) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"Config":{"BindAddr":"192.0.2.1"}}`)
	}))
	defer server.Close()
	config := &api.Config{Address: server.URL}
	for _, tc := range []struct {
		name string
		get  func() (net.IP, error)
	}{
		{"config", func() (net.IP, error) { return GetAgentBindAddr(config, false) }},
		{"DTO", func() (net.IP, error) { return GetAgentBindAddrWithDTO(&IPStackRequestDTO{Config: config}) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ip, err := tc.get()
			require.NoError(t, err)
			require.Equal(t, "192.0.2.1", ip.String())
			ip[0] = 10
			ip, err = GetAgentBindAddr(nil, true)
			require.NoError(t, err)
			require.Equal(t, "192.0.2.1", ip.String(), "fetched results must not expose the cached slice")
		})
	}
}
