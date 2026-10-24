// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package rate

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/hashicorp/consul/agent/consul/multilimiter"
)

type registrationStatusProvider struct {
	leader bool
}

func (p registrationStatusProvider) IsLeader() bool       { return p.leader }
func (p registrationStatusProvider) IsServer(string) bool { return false }

func newRegistrationTestHandler(t *testing.T) *Handler {
	t.Helper()
	limiter := multilimiter.NewMockRateLimiter(t)
	limiter.On("UpdateConfig", mock.Anything, mock.Anything).Return()
	limiter.On("Allow", mock.Anything).Return(false)
	return NewHandlerWithLimiter(HandlerConfig{
		GlobalLimitConfig: GlobalLimitConfig{Mode: ModeEnforcing},
	}, limiter, hclog.NewNullLogger())
}

func TestHandlerConcurrentRegistration(t *testing.T) {
	handler := newRegistrationTestHandler(t)
	op := Operation{Name: "Catalog.Register", Type: OperationTypeWrite, SourceAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}}
	require.NoError(t, handler.Allow(op), "RPCs can arrive before startup registers server status")

	start := make(chan struct{})
	errors := make(chan error, 1)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 500; i++ {
			handler.Register(registrationStatusProvider{leader: i%2 == 0})
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for i := 0; i < 500; i++ {
			err := handler.Allow(op)
			if err != nil && err != ErrRetryLater && err != ErrRetryElsewhere {
				errors <- fmt.Errorf("unexpected rate-limit decision: %w", err)
				return
			}
		}
	}()
	close(start)
	workers.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
	handler.Register(registrationStatusProvider{leader: true})
	require.ErrorIs(t, handler.Allow(op), ErrRetryLater)
	handler.Register(registrationStatusProvider{})
	require.ErrorIs(t, handler.Allow(op), ErrRetryElsewhere)
	handler.Register(nil)
	require.NoError(t, handler.Allow(op), "unregistering must retain the startup bypass behavior")
}

type pausedStatusProvider struct {
	entered chan struct{}
	release chan struct{}
}

func (p pausedStatusProvider) IsLeader() bool { return true }
func (p pausedStatusProvider) IsServer(string) bool {
	close(p.entered)
	<-p.release
	return false
}

func TestHandlerRegistrationDuringDecision(t *testing.T) {
	handler := newRegistrationTestHandler(t)
	provider := pausedStatusProvider{entered: make(chan struct{}), release: make(chan struct{})}
	handler.Register(provider)
	decision := make(chan error, 1)
	go func() {
		decision <- handler.Allow(Operation{
			Name: "Catalog.Register", Type: OperationTypeWrite,
			SourceAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234},
		})
	}()
	defer close(provider.release)
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("rate-limit decision did not reach server status")
	}
	registered := make(chan struct{})
	go func() {
		handler.Register(registrationStatusProvider{})
		close(registered)
	}()
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("Register must not wait for provider callbacks")
	}
	provider.release <- struct{}{}
	require.ErrorIs(t, <-decision, ErrRetryLater,
		"an operation must use the same registered provider for its entire decision")
}
