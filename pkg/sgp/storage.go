package sgp

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

var (
	// ErrGraphNotFound indicates that a persisted graph could not be located.
	ErrGraphNotFound = errors.New("graph not found")
)

// Store persists SGP session events as an append-only log.
//
// AppendEvent appends a single event to the named session's log. Implementations
// must return [ErrGraphNotFound] from LoadEvents when no events have been recorded
// for the given session ID.
//
// The Store interface makes no concurrency guarantees for writes to the same
// session. Callers are responsible for serialising concurrent AppendEvent calls
// targeting the same sessionID.
//
// Implementations must restore the [Event.Kind] field on events returned by
// LoadEvents using [ClassifyEvent].
type Store interface {
	AppendEvent(ctx context.Context, sessionID ID, event Event) error
	LoadEvents(ctx context.Context, sessionID ID) ([]Event, error)
}

// ClassifyEvent determines the EventKind for an event using field presence.
// It is robust to custom EventNames because it never compares event name strings.
// Store implementations should call ClassifyEvent to restore Event.Kind on events
// loaded from persistent storage (Kind is not serialised).
func ClassifyEvent(event Event) EventKind {
	if event.Node != nil {
		if len(event.Node.SynthesizedFrom) > 0 {
			return EventKindHistoryRewritten
		}
		return EventKindNodeAppended
	}
	if event.Reason != "" || event.TerminalNodeID != "" {
		return EventKindSessionEnded
	}
	return EventKindSessionStart
}

// RestoreFromEvents reconstructs an in-memory [Graph] from a persisted event log.
// events must be ordered by emission time, as returned by [Store.LoadEvents].
//
// EventNames are inferred from the event name strings observed in the log; any
// kind not represented falls back to [DefaultEventNames]. The restored graph uses
// the inferred names for all future [Graph.Append], [Graph.Rewrite], and
// [Graph.End] calls, preserving custom event name configuration across restarts.
//
// Returns [ErrGraphNotFound] if events is empty.
func RestoreFromEvents(events []Event) (*Graph, error) {
	if len(events) == 0 {
		return nil, ErrGraphNotFound
	}

	eventNames := DefaultEventNames()

	graph := &Graph{
		nodes:    make(map[ID]Node),
		children: make(map[ID][]ID),
		idGenerator: func() ID {
			return ID(uuid.NewString())
		},
	}

	for i, event := range events {
		kind := ClassifyEvent(event)
		event.Kind = kind

		switch kind {
		case EventKindSessionStart:
			if graph.started {
				return nil, fmt.Errorf("event at index %d: unexpected second session.start event", i)
			}
			graph.session.ID = event.SessionID
			graph.session.Timestamp = event.Timestamp
			graph.session.SpawnedFrom = copySpawnReference(event.SpawnedFrom)
			graph.started = true
			eventNames.SessionStart = event.Event

		case EventKindNodeAppended, EventKindHistoryRewritten:
			if event.Node == nil {
				return nil, fmt.Errorf("event at index %d: missing node", i)
			}
			node := copyNode(*event.Node)
			if node.ID == "" {
				return nil, fmt.Errorf("event at index %d: node id is required", i)
			}
			if node.SessionID == "" || node.SessionID != graph.session.ID {
				return nil, fmt.Errorf("event at index %d: node %s has session id %q, expected %q", i, node.ID, node.SessionID, graph.session.ID)
			}
			for _, parentID := range node.ParentIDs {
				if _, exists := graph.nodes[parentID]; !exists {
					return nil, fmt.Errorf("event at index %d: %w: parent %s missing for node %s", i, ErrNodeNotFound, parentID, node.ID)
				}
				graph.children[parentID] = append(graph.children[parentID], node.ID)
			}
			for _, sourceID := range node.SynthesizedFrom {
				if _, exists := graph.nodes[sourceID]; !exists {
					return nil, fmt.Errorf("event at index %d: %w: synthesized source %s missing for node %s", i, ErrNodeNotFound, sourceID, node.ID)
				}
			}
			graph.nodes[node.ID] = node
			graph.headID = node.ID
			if kind == EventKindNodeAppended {
				eventNames.NodeAppended = event.Event
			} else {
				eventNames.HistoryRewritten = event.Event
			}

		case EventKindSessionEnded:
			graph.closed = true
			graph.terminalNodeID = event.TerminalNodeID
			graph.endReason = event.Reason
			eventNames.SessionEnded = event.Event
		}

		graph.events = append(graph.events, copyEvent(event))
	}

	if !graph.started {
		return nil, errors.New("event log missing session.start event")
	}

	graph.eventNames = eventNames

	return graph, nil
}
