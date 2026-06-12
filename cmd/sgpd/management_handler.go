package main

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	sgpv1 "github.com/restrukt-ai/sessiongraphprotocol/gen/sgp/v1"
	"github.com/restrukt-ai/sessiongraphprotocol/gen/sgp/v1/sgpv1connect"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/sgp"
	pg "github.com/restrukt-ai/sessiongraphprotocol/pkg/store/pg"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/store/sgpd/convert"
)

type managementHandler struct {
	sgpv1connect.UnimplementedSGPManagementServiceHandler
	store *pg.PGStore
}

func (h *managementHandler) ListSessions(
	ctx context.Context,
	req *connect.Request[sgpv1.ListSessionsRequest],
) (*connect.Response[sgpv1.ListSessionsResponse], error) {
	sessions, nextToken, err := h.store.ListSessions(ctx, int(req.Msg.Limit), req.Msg.PageToken)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	pbSessions := make([]*sgpv1.Session, len(sessions))
	for i, s := range sessions {
		pbSessions[i] = convert.SessionToProto(s)
	}
	return connect.NewResponse(&sgpv1.ListSessionsResponse{
		Sessions:      pbSessions,
		NextPageToken: nextToken,
	}), nil
}

func (h *managementHandler) GetSession(
	ctx context.Context,
	req *connect.Request[sgpv1.GetSessionRequest],
) (*connect.Response[sgpv1.GetSessionResponse], error) {
	if req.Msg.SessionId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("session_id is required"))
	}
	sess, headID, status, err := h.store.GetSession(ctx, sgp.ID(req.Msg.SessionId))
	if err != nil {
		if errors.Is(err, sgp.ErrGraphNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	pbStatus := sgpv1.SessionStatus_SESSION_STATUS_OPEN
	if status == pg.SessionStatusClosed {
		pbStatus = sgpv1.SessionStatus_SESSION_STATUS_CLOSED
	}
	return connect.NewResponse(&sgpv1.GetSessionResponse{
		Session:    convert.SessionToProto(sess),
		HeadNodeId: string(headID),
		Status:     pbStatus,
	}), nil
}

func (h *managementHandler) GetNode(
	ctx context.Context,
	req *connect.Request[sgpv1.GetNodeRequest],
) (*connect.Response[sgpv1.GetNodeResponse], error) {
	if req.Msg.NodeId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}
	node, err := h.store.GetNode(ctx, sgp.ID(req.Msg.NodeId))
	if err != nil {
		if errors.Is(err, sgp.ErrNodeNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&sgpv1.GetNodeResponse{Node: convert.NodeToProto(node)}), nil
}

func (h *managementHandler) GetResumeContext(
	ctx context.Context,
	req *connect.Request[sgpv1.GetResumeContextRequest],
) (*connect.Response[sgpv1.GetResumeContextResponse], error) {
	if req.Msg.NodeId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("node_id is required"))
	}
	nodes, err := h.store.GetResumeContext(ctx, sgp.ID(req.Msg.NodeId))
	if err != nil {
		if errors.Is(err, sgp.ErrNodeNotFound) {
			return nil, connect.NewError(connect.CodeNotFound, err)
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	pbNodes := make([]*sgpv1.Node, len(nodes))
	pbMsgs := make([]*sgpv1.Message, len(nodes))
	for i, n := range nodes {
		pbNodes[i] = convert.NodeToProto(n)
		pbMsgs[i] = convert.MessageToProto(n.Message)
	}
	return connect.NewResponse(&sgpv1.GetResumeContextResponse{
		Nodes:    pbNodes,
		Messages: pbMsgs,
	}), nil
}

func (h *managementHandler) GetSessionGraph(
	ctx context.Context,
	req *connect.Request[sgpv1.GetSessionGraphRequest],
) (*connect.Response[sgpv1.GetSessionGraphResponse], error) {
	if req.Msg.SessionId == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("session_id is required"))
	}
	nodes, edges, err := h.store.GetSessionGraph(ctx, sgp.ID(req.Msg.SessionId))
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	pbNodes := make([]*sgpv1.Node, len(nodes))
	for i, n := range nodes {
		pbNodes[i] = convert.NodeToProto(n)
	}
	pbEdges := make([]*sgpv1.NodeEdge, len(edges))
	for i, e := range edges {
		pbEdges[i] = &sgpv1.NodeEdge{FromId: string(e.FromID), ToId: string(e.ToID), Kind: e.Kind}
	}
	return connect.NewResponse(&sgpv1.GetSessionGraphResponse{Nodes: pbNodes, Edges: pbEdges}), nil
}

func (h *managementHandler) WatchSession(
	ctx context.Context,
	req *connect.Request[sgpv1.WatchSessionRequest],
	stream *connect.ServerStream[sgpv1.SessionObservation],
) error {
	if req.Msg.SessionId == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("session_id is required"))
	}
	sessionID := sgp.ID(req.Msg.SessionId)

	// Subscribe before loading history to avoid missing events.
	ch, cancel := h.store.Subscribe(ctx, sessionID)
	defer cancel()

	var lastSentSeq int64 = -1

	if req.Msg.ReplayHistory {
		rows, err := h.store.LoadEventsWithSeq(ctx, sessionID)
		if err != nil && !errors.Is(err, sgp.ErrGraphNotFound) {
			return connect.NewError(connect.CodeInternal, err)
		}
		for _, row := range rows {
			if err := stream.Send(eventRowToObservation(row)); err != nil {
				return err
			}
			lastSentSeq = row.Seq
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case ob, ok := <-ch:
			if !ok {
				return nil
			}
			if ob.Seq <= lastSentSeq {
				continue
			}
			if err := stream.Send(observationToProto(ob)); err != nil {
				return err
			}
			lastSentSeq = ob.Seq
		}
	}
}

func eventRowToObservation(row pg.EventRow) *sgpv1.SessionObservation {
	var headID sgp.ID
	if row.Event.Node != nil {
		headID = row.Event.Node.ID
	} else if row.Event.TerminalNodeID != "" {
		headID = row.Event.TerminalNodeID
	}
	pbStatus := sgpv1.SessionStatus_SESSION_STATUS_OPEN
	pbReason := sgpv1.EndReason_END_REASON_UNSPECIFIED
	if row.Event.Kind == sgp.EventKindSessionEnded {
		pbStatus = sgpv1.SessionStatus_SESSION_STATUS_CLOSED
		pbReason = protoEndReason(row.Event.Reason)
	}
	return &sgpv1.SessionObservation{
		Event:      convert.EventToProto(row.Event),
		HeadNodeId: string(headID),
		Status:     pbStatus,
		EndReason:  pbReason,
	}
}

func observationToProto(ob pg.Observation) *sgpv1.SessionObservation {
	pbStatus := sgpv1.SessionStatus_SESSION_STATUS_OPEN
	if ob.Status == pg.SessionStatusClosed {
		pbStatus = sgpv1.SessionStatus_SESSION_STATUS_CLOSED
	}
	return &sgpv1.SessionObservation{
		Event:      convert.EventToProto(ob.Event),
		HeadNodeId: string(ob.HeadID),
		Status:     pbStatus,
		EndReason:  protoEndReason(ob.EndReason),
		NodeCount:  ob.NodeCount,
	}
}

func protoEndReason(r sgp.EndReason) sgpv1.EndReason {
	switch r {
	case sgp.EndReasonComplete:
		return sgpv1.EndReason_END_REASON_COMPLETE
	case sgp.EndReasonFailed:
		return sgpv1.EndReason_END_REASON_FAILED
	default:
		return sgpv1.EndReason_END_REASON_UNSPECIFIED
	}
}

var _ sgpv1connect.SGPManagementServiceHandler = (*managementHandler)(nil)
