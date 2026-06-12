package main

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	sgpv1 "github.com/restrukt-ai/sessiongraphprotocol/gen/sgp/v1"
	"github.com/restrukt-ai/sessiongraphprotocol/gen/sgp/v1/sgpv1connect"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/sgp"
	pg "github.com/restrukt-ai/sessiongraphprotocol/pkg/store/pg"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/store/sgpd/convert"
)

type harnessHandler struct {
	sgpv1connect.UnimplementedSGPHarnessServiceHandler
	store *pg.PGStore
}

func (h *harnessHandler) AppendEvent(
	ctx context.Context,
	req *connect.Request[sgpv1.AppendEventRequest],
) (*connect.Response[sgpv1.AppendEventResponse], error) {
	if req.Msg.SessionId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("session_id is required"))
	}
	if req.Msg.Event == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("event is required"))
	}

	event := convert.EventFromProto(req.Msg.Event)
	if err := h.store.AppendEvent(ctx, sgp.ID(req.Msg.SessionId), event); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("append event: %w", err))
	}
	return connect.NewResponse(&sgpv1.AppendEventResponse{}), nil
}

func (h *harnessHandler) LoadEvents(
	ctx context.Context,
	req *connect.Request[sgpv1.LoadEventsRequest],
) (*connect.Response[sgpv1.LoadEventsResponse], error) {
	if req.Msg.SessionId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("session_id is required"))
	}

	events, err := h.store.LoadEvents(ctx, sgp.ID(req.Msg.SessionId))
	if err != nil {
		if errors.Is(err, sgp.ErrGraphNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	pbEvents := make([]*sgpv1.Event, len(events))
	for i, e := range events {
		pbEvents[i] = convert.EventToProto(e)
	}
	return connect.NewResponse(&sgpv1.LoadEventsResponse{Events: pbEvents}), nil
}
