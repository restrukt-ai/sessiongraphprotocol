package sgp

import (
	"errors"
	"testing"
)

func TestSnapshotUsesCurrentVersion(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "node-a")))
	if _, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}}); err != nil {
		t.Fatalf("append root: %v", err)
	}

	snapshot := graph.Snapshot()
	if got, want := snapshot.Version, uint32(CurrentGraphSnapshotVersion); got != want {
		t.Fatalf("expected snapshot version %d, got %d", want, got)
	}
}

func TestUpgradeSnapshotAcceptsCurrentVersion(t *testing.T) {
	t.Parallel()

	snapshot := GraphSnapshot{
		Version: CurrentGraphSnapshotVersion,
		Session: Session{ID: "session-1"},
		Nodes: []Node{{
			ID:        "node-a",
			SessionID: "session-1",
			Message:   Message{System: &SystemMessage{Text: "sys"}},
		}},
		Events: []Event{{
			Event:     DefaultEventNames().SessionStart,
			SessionID: "session-1",
		}},
		HeadID: "node-a",
	}

	upgradedSnapshot, err := UpgradeSnapshot(snapshot)
	if err != nil {
		t.Fatalf("upgrade snapshot: %v", err)
	}

	if got, want := upgradedSnapshot.Version, uint32(CurrentGraphSnapshotVersion); got != want {
		t.Fatalf("expected upgraded version %d, got %d", want, got)
	}

	if got, want := upgradedSnapshot.Version, snapshot.Version; got != want {
		t.Fatalf("expected upgraded version %d, got %d", want, got)
	}
}

func TestUpgradeSnapshotRejectsMissingVersion(t *testing.T) {
	t.Parallel()

	_, err := UpgradeSnapshot(GraphSnapshot{Session: Session{ID: "session-1"}})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestUpgradeSnapshotRejectsFutureVersion(t *testing.T) {
	t.Parallel()

	_, err := UpgradeSnapshot(GraphSnapshot{Version: CurrentGraphSnapshotVersion + 1, Session: Session{ID: "session-1"}})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestRestoreGraphWithCurrentVersionSnapshot(t *testing.T) {
	t.Parallel()

	snapshot := GraphSnapshot{
		Version: CurrentGraphSnapshotVersion,
		Session: Session{ID: "session-1"},
		Nodes: []Node{
			{ID: "node-a", SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}},
			{ID: "node-b", SessionID: "session-1", ParentIDs: []ID{"node-a"}, Message: Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "ask"}}}}}},
		},
		Events: []Event{
			{Event: DefaultEventNames().SessionStart, SessionID: "session-1"},
			{Event: DefaultEventNames().NodeAppended, SessionID: "session-1", Node: &Node{ID: "node-a", SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}}},
			{Event: DefaultEventNames().NodeAppended, SessionID: "session-1", Node: &Node{ID: "node-b", SessionID: "session-1", ParentIDs: []ID{"node-a"}, Message: Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "ask"}}}}}}},
		},
		HeadID: "node-b",
	}

	graph, err := RestoreGraph(snapshot)
	if err != nil {
		t.Fatalf("restore graph: %v", err)
	}

	messages, err := graph.ResumeMessages("node-b")
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}

	if got, want := len(messages), 2; got != want {
		t.Fatalf("expected %d messages, got %d", want, got)
	}

	events := graph.Events()
	if got, want := events[0].Kind, EventKindSessionStart; got != want {
		t.Fatalf("expected restored start kind %d, got %d", want, got)
	}
}

func TestRestoreGraphRejectsMissingVersion(t *testing.T) {
	t.Parallel()

	_, err := RestoreGraph(GraphSnapshot{Session: Session{ID: "session-1"}})
	if !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestRestoreGraphRoundTripsSnapshot(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "node-a", "node-b")),
		WithEventNames(EventNames{
			SessionStart:     "sgp.session.started",
			NodeAppended:     "sgp.node.appended",
			HistoryRewritten: "sgp.history.rewritten",
			SessionEnded:     "sgp.session.ended",
		}),
	)

	root, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	assistantNode, _, err := graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "answer"}}}}}, root.ID)
	if err != nil {
		t.Fatalf("append assistant: %v", err)
	}

	if _, err = graph.End(EndReasonComplete); err != nil {
		t.Fatalf("end graph: %v", err)
	}

	restored, err := RestoreGraph(graph.Snapshot())
	if err != nil {
		t.Fatalf("restore graph: %v", err)
	}

	messages, err := restored.ResumeMessages(assistantNode.ID)
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	events := restored.Events()
	if got, want := events[0].Kind, EventKindSessionStart; got != want {
		t.Fatalf("expected restored start kind %d, got %d", want, got)
	}

	if got, want := events[0].Event, "sgp.session.started"; got != want {
		t.Fatalf("expected restored custom event name %q, got %q", want, got)
	}

	if _, _, err = restored.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "late"}}}}}, assistantNode.ID); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("expected restored closed graph to reject appends, got %v", err)
	}
}

func TestUpgradeV1ToV2BackfillsEndReasonOnClosedSnapshot(t *testing.T) {
	t.Parallel()

	v1Snapshot := GraphSnapshot{
		Version: GraphSnapshotVersion1,
		Session: Session{ID: "session-1"},
		Nodes: []Node{{
			ID:        "node-a",
			SessionID: "session-1",
			Message:   Message{System: &SystemMessage{Text: "sys"}},
		}},
		Events: []Event{
			{Event: DefaultEventNames().SessionStart, SessionID: "session-1"},
			{Event: DefaultEventNames().NodeAppended, SessionID: "session-1"},
			{Event: DefaultEventNames().SessionEnded, SessionID: "session-1", TerminalNodeID: "node-a"},
		},
		HeadID:         "node-a",
		TerminalNodeID: "node-a",
		Closed:         true,
	}

	upgraded, err := UpgradeSnapshot(v1Snapshot)
	if err != nil {
		t.Fatalf("upgrade snapshot: %v", err)
	}

	if got, want := upgraded.Version, GraphSnapshotVersion2; got != want {
		t.Fatalf("expected version %d, got %d", want, got)
	}

	if got, want := upgraded.EndReason, EndReasonComplete; got != want {
		t.Fatalf("expected end reason %q, got %q", want, got)
	}
}

func TestUpgradeV1ToV2LeavesEndReasonEmptyOnOpenSnapshot(t *testing.T) {
	t.Parallel()

	v1Snapshot := GraphSnapshot{
		Version: GraphSnapshotVersion1,
		Session: Session{ID: "session-1"},
		Nodes: []Node{{
			ID:        "node-a",
			SessionID: "session-1",
			Message:   Message{System: &SystemMessage{Text: "sys"}},
		}},
		Events: []Event{
			{Event: DefaultEventNames().SessionStart, SessionID: "session-1"},
			{Event: DefaultEventNames().NodeAppended, SessionID: "session-1"},
		},
		HeadID: "node-a",
		Closed: false,
	}

	upgraded, err := UpgradeSnapshot(v1Snapshot)
	if err != nil {
		t.Fatalf("upgrade snapshot: %v", err)
	}

	if got := upgraded.EndReason; got != "" {
		t.Fatalf("expected empty end reason on open snapshot, got %q", got)
	}
}

func TestSnapshotRoundTripsEndReason(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "node-a")))
	if _, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}}); err != nil {
		t.Fatalf("append root: %v", err)
	}

	if _, err := graph.End(EndReasonFailed); err != nil {
		t.Fatalf("end graph: %v", err)
	}

	restored, err := RestoreGraph(graph.Snapshot())
	if err != nil {
		t.Fatalf("restore graph: %v", err)
	}

	restoredSnapshot := restored.Snapshot()
	if got, want := restoredSnapshot.EndReason, EndReasonFailed; got != want {
		t.Fatalf("expected end reason %q after round-trip, got %q", want, got)
	}
}

func TestRestoreGraphRejectsInvalidSnapshots(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name     string
		snapshot GraphSnapshot
	}{
		{
			name:     "missing session id",
			snapshot: GraphSnapshot{Version: CurrentGraphSnapshotVersion},
		},
		{
			name: "missing node id",
			snapshot: GraphSnapshot{
				Version: CurrentGraphSnapshotVersion,
				Session: Session{ID: "session-1"},
				Nodes:   []Node{{SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}}},
			},
		},
		{
			name: "session mismatch",
			snapshot: GraphSnapshot{
				Version: CurrentGraphSnapshotVersion,
				Session: Session{ID: "session-1"},
				Nodes:   []Node{{ID: "node-a", SessionID: "session-2", Message: Message{System: &SystemMessage{Text: "sys"}}}},
			},
		},
		{
			name: "missing parent",
			snapshot: GraphSnapshot{
				Version: CurrentGraphSnapshotVersion,
				Session: Session{ID: "session-1"},
				Nodes:   []Node{{ID: "node-a", SessionID: "session-1", ParentIDs: []ID{"missing"}, Message: Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "sys"}}}}}}},
			},
		},
		{
			name: "missing synthesized source",
			snapshot: GraphSnapshot{
				Version: CurrentGraphSnapshotVersion,
				Session: Session{ID: "session-1"},
				Nodes: []Node{
					{ID: "node-a", SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}},
					{ID: "node-b", SessionID: "session-1", ParentIDs: []ID{"node-a"}, SynthesizedFrom: []ID{"missing"}, Message: Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "merged"}}}}}},
				},
			},
		},
		{
			name: "missing head",
			snapshot: GraphSnapshot{
				Version: CurrentGraphSnapshotVersion,
				Session: Session{ID: "session-1"},
				Nodes:   []Node{{ID: "node-a", SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}}},
				HeadID:  "missing",
			},
		},
		{
			name: "missing terminal",
			snapshot: GraphSnapshot{
				Version:        CurrentGraphSnapshotVersion,
				Session:        Session{ID: "session-1"},
				Nodes:          []Node{{ID: "node-a", SessionID: "session-1", Message: Message{System: &SystemMessage{Text: "sys"}}}},
				TerminalNodeID: "missing",
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			_, err := RestoreGraph(testCase.snapshot)
			if !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
			}
		})
	}
}
