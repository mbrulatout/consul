// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: BUSL-1.1

package subscribe

import (
	"context"
	"errors"

	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/state"
	"github.com/hashicorp/consul/agent/consul/stream"
	agentmiddleware "github.com/hashicorp/consul/agent/grpc-middleware"
	"github.com/hashicorp/consul/agent/structs"
	"github.com/hashicorp/consul/proto/private/pbsubscribe"
)

// Server implements a StateChangeSubscriptionServer for accepting SubscribeRequests,
// and sending events to the subscription topic.
type Server struct {
	Backend Backend
	Logger  Logger
}

func NewServer(backend Backend, logger Logger) *Server {
	return &Server{Backend: backend, Logger: logger}
}

type Logger interface {
	Trace(msg string, args ...interface{})
	With(args ...interface{}) hclog.Logger
}

var _ pbsubscribe.StateChangeSubscriptionServer = (*Server)(nil)

type Backend interface {
	ResolveTokenAndDefaultMeta(token string, entMeta *acl.EnterpriseMeta, authzContext *acl.AuthorizerContext) (acl.Authorizer, error)
	Forward(info structs.RPCInfo, f func(*grpc.ClientConn) error) (handled bool, err error)
	Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error)
}

// leaderContacter is implemented by backends that can tell whether this
// server's state is fresh.
type leaderContacter interface {
	// LeaderContact returns a context that is canceled once this server has
	// gone too long without contact with the raft leader.
	LeaderContact() context.Context
}

func (h *Server) Subscribe(req *pbsubscribe.SubscribeRequest, serverStream pbsubscribe.StateChangeSubscription_SubscribeServer) error {
	logger := newLoggerForRequest(h.Logger, req)
	handled, err := h.Backend.Forward(req, forwardToDC(req, serverStream, logger))
	if handled || err != nil {
		return err
	}

	logger.Trace("new subscription")
	defer logger.Trace("subscription closed")

	entMeta := req.EnterpriseMeta()
	authz, err := h.Backend.ResolveTokenAndDefaultMeta(req.Token, &entMeta, nil)
	if err != nil {
		return err
	}

	subReq, err := state.PBToStreamSubscribeRequest(req, entMeta)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	ctx := serverStream.Context()
	lc, watchLeader := h.Backend.(leaderContacter)
	if watchLeader {
		fresh := lc.LeaderContact()
		if fresh.Err() != nil {
			return status.Error(codes.Unavailable, errNoLeaderContact.Error())
		}
		var cancel context.CancelCauseFunc
		ctx, cancel = context.WithCancelCause(ctx)
		defer cancel(nil)
		stop := context.AfterFunc(fresh, func() { cancel(errNoLeaderContact) })
		defer stop()
	}

	sub, err := h.Backend.Subscribe(subReq)
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	// Invalidation before registration cannot close this subscription.
	authz, err = h.Backend.ResolveTokenAndDefaultMeta(req.Token, &entMeta, nil)
	if err != nil {
		return err
	}

	subscriptionError := func(err error) error {
		switch {
		case errors.Is(err, stream.ErrSubForceClosed):
			logger.Trace("subscription reset by server")
			return status.Error(codes.Aborted, err.Error())
		case errors.Is(err, stream.ErrACLChanged):
			logger.Trace("ACL change occurred; re-authenticating")
			_, authzErr := h.Backend.ResolveTokenAndDefaultMeta(req.Token, &entMeta, nil)
			if authzErr != nil {
				return authzErr
			}
			// Otherwise, abort the stream so the client re-subscribes.
			return status.Error(codes.Aborted, err.Error())
		case errors.Is(context.Cause(ctx), errNoLeaderContact):
			return status.Error(codes.Unavailable, errNoLeaderContact.Error())
		case errors.Is(err, stream.ErrSnapshotFailed):
			return status.Error(codes.Internal, stream.ErrSnapshotFailed.Error())
		default:
			return err
		}
	}

	sendEvents := func() error {
		elog := &eventLogger{logger: logger}
		for {
			event, err := sub.Next(ctx)
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}

			if !event.Payload.HasReadPermission(authz) {
				continue
			}

			elog.Trace(event)

			// TODO: This conversion could be cached if needed
			e := event.Payload.ToSubscriptionEvent(event.Index)
			if err := serverStream.Send(e); err != nil {
				return err
			}
		}
	}
	// Returning the handler cancels gRPC's transport context, which interrupts
	// Send even when client flow control has stopped delivery.
	result := make(chan error, 1)
	go func() {
		var err error
		panicked := true
		defer func() {
			// The server's recovery interceptor runs on the handler goroutine.
			if p := recover(); p != nil || panicked {
				err = agentmiddleware.NewPanicHandler(logger.With())(p)
			}
			result <- err
		}()
		err = sendEvents()
		panicked = false
	}()
	select {
	case err := <-result:
		return subscriptionError(err)
	case <-sub.Done():
		return subscriptionError(sub.Err())
	case <-ctx.Done():
		return subscriptionError(ctx.Err())
	}
}

func forwardToDC(
	req *pbsubscribe.SubscribeRequest,
	serverStream pbsubscribe.StateChangeSubscription_SubscribeServer,
	logger Logger,
) func(conn *grpc.ClientConn) error {
	return func(conn *grpc.ClientConn) error {
		logger.Trace("forwarding to another DC")
		defer logger.Trace("forwarded stream closed")

		client := pbsubscribe.NewStateChangeSubscriptionClient(conn)
		streamHandle, err := client.Subscribe(serverStream.Context(), req)
		if err != nil {
			return err
		}

		for {
			event, err := streamHandle.Recv()
			if err != nil {
				return err
			}
			if err := serverStream.Send(event); err != nil {
				return err
			}
		}
	}
}
