package sgp

import (
	"errors"
	"testing"
)

func TestNewGraphEmitsConfigurableSessionStart(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1")),
		WithEventNames(EventNames{
			SessionStart:     "sgp.session.started",
			NodeAppended:     "sgp.node.appended",
			HistoryRewritten: "sgp.history.rewritten",
			SessionEnded:     "sgp.session.ended",
		}),
	)

	events := graph.Events()
	if len(events) != 1 {
		t.Fatalf("expected a single session start event, got %d", len(events))
	}

	if got, want := events[0].Event, "sgp.session.started"; got != want {
		t.Fatalf("expected custom start event %q, got %q", want, got)
	}

	if got, want := events[0].SessionID, ID("session-1"); got != want {
		t.Fatalf("expected session id %q, got %q", want, got)
	}
}

func TestResumeMessagesReturnsCanonicalLineage(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "node-a", "node-b", "node-c")))
	root, _, err := graph.Append(Message{System: &SystemMessage{Text: "system"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	userNode, _, err := graph.Append(Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "hello"}}}}}, root.ID)
	if err != nil {
		t.Fatalf("append user: %v", err)
	}

	assistantNode, _, err := graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "world"}}}}}, userNode.ID)
	if err != nil {
		t.Fatalf("append assistant: %v", err)
	}

	messages, err := graph.ResumeMessages(assistantNode.ID)
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}

	if len(messages) != 3 {
		t.Fatalf("expected 3 messages, got %d", len(messages))
	}

	if got, want := messages[0].TextContent(), "system"; got != want {
		t.Fatalf("expected first message %q, got %v", want, got)
	}

	if got, want := messages[2].TextContent(), "world"; got != want {
		t.Fatalf("expected final message %q, got %v", want, got)
	}
}

func TestRewriteKeepsBranchHistoryOutOfCanonicalResume(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs(
			"session-1",
			"a",
			"b",
			"c",
			"d1",
			"d2",
			"e1",
			"f",
		)),
	)

	root, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	userNode, _, err := graph.Append(Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "user"}}}}}, root.ID)
	if err != nil {
		t.Fatalf("append user: %v", err)
	}

	canonicalNode, _, err := graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "think"}}}}}, userNode.ID)
	if err != nil {
		t.Fatalf("append canonical: %v", err)
	}

	branchOne, _, err := graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "branch one"}}}}}, canonicalNode.ID)
	if err != nil {
		t.Fatalf("append branch one: %v", err)
	}

	branchTwo, _, err := graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "branch two"}}}}}, canonicalNode.ID)
	if err != nil {
		t.Fatalf("append branch two: %v", err)
	}

	rewriteNode, event, err := graph.Rewrite(
		Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "merged"}}}}},
		canonicalNode.ID,
		branchOne.ID,
		branchTwo.ID,
	)
	if err != nil {
		t.Fatalf("rewrite: %v", err)
	}

	if got, want := event.Event, DefaultEventNames().HistoryRewritten; got != want {
		t.Fatalf("expected rewrite event %q, got %q", want, got)
	}

	lineage, err := graph.ResumeNodes(rewriteNode.ID)
	if err != nil {
		t.Fatalf("resume nodes: %v", err)
	}

	if len(lineage) != 4 {
		t.Fatalf("expected canonical lineage length 4, got %d", len(lineage))
	}

	if got, want := lineage[3].Message.TextContent(), "merged"; got != want {
		t.Fatalf("expected rewrite message %q, got %v", want, got)
	}

	if len(lineage[3].SynthesizedFrom) != 2 {
		t.Fatalf("expected rewrite node to preserve synthesized sources, got %d", len(lineage[3].SynthesizedFrom))
	}
}

func TestNeedsResponseOnlyForDanglingUserOrToolLeaves(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "node-a", "node-b", "node-c")))
	root, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	userNode, _, err := graph.Append(Message{User: &UserMessage{Parts: []ContentPart{{Text: &TextPart{Text: "ask"}}}}}, root.ID)
	if err != nil {
		t.Fatalf("append user: %v", err)
	}

	needsResponse, err := graph.NeedsResponse(userNode.ID)
	if err != nil {
		t.Fatalf("needs response before assistant: %v", err)
	}

	if !needsResponse {
		t.Fatal("expected dangling user leaf to require a response")
	}

	_, _, err = graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "answer"}}}}}, userNode.ID)
	if err != nil {
		t.Fatalf("append assistant: %v", err)
	}

	needsResponse, err = graph.NeedsResponse(userNode.ID)
	if err != nil {
		t.Fatalf("needs response after assistant: %v", err)
	}

	if needsResponse {
		t.Fatal("expected non-leaf user node to stop requiring a response")
	}
}

func TestEndUsesCurrentHead(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "node-a")))
	root, _, err := graph.Append(Message{System: &SystemMessage{Text: "sys"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	event, err := graph.End()
	if err != nil {
		t.Fatalf("end graph: %v", err)
	}

	if got, want := event.TerminalNodeID, root.ID; got != want {
		t.Fatalf("expected terminal node %q, got %q", want, got)
	}

	if _, _, err = graph.Append(Message{Assistant: &AssistantMessage{Parts: []ContentPart{{Text: &TextPart{Text: "late"}}}}}, root.ID); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got %v", err)
	}
}

func sequenceIDs(ids ...ID) IDGenerator {
	index := 0

	return func() ID {
		if index >= len(ids) {
			panic("sequenceIDs exhausted")
		}

		id := ids[index]
		index++

		return id
	}
}
