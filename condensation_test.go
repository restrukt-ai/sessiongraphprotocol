package sessiongraphprotocol

import (
	"errors"
	"testing"
)

func TestTriggerCondensationNoRegisteredCondenser(t *testing.T) {
	t.Parallel()

	graph := NewGraph(WithIDGenerator(sequenceIDs("session-1", "root")))
	if _, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil); err != nil {
		t.Fatalf("append root: %v", err)
	}

	_, _, condensed, err := graph.TriggerCondensation(CondenseTriggerToolCallSequenceCompleted)
	if err != nil {
		t.Fatalf("trigger condensation: %v", err)
	}

	if condensed {
		t.Fatal("expected no condensation when no condenser is registered")
	}
}

func TestTriggerCondensationNoOpDecision(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "root")),
		WithCondenser(CondenseTriggerManual, func(input CondenseInput) (CondenseDecision, error) {
			if input.Trigger != CondenseTriggerManual {
				return CondenseDecision{}, errors.New("unexpected trigger")
			}

			return CondenseDecision{Condense: false}, nil
		}),
	)
	if _, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil); err != nil {
		t.Fatalf("append root: %v", err)
	}

	_, _, condensed, err := graph.TriggerCondensation(CondenseTriggerManual)
	if err != nil {
		t.Fatalf("trigger condensation: %v", err)
	}

	if condensed {
		t.Fatal("expected no condensation for no-op decision")
	}
}

func TestTriggerCondensationToolCallSequenceRewrite(t *testing.T) {
	t.Parallel()

	var userNodeID ID
	var toolResultOneID ID
	var toolResultTwoID ID

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "root", "user", "tool-call-1", "tool-result-1", "tool-call-2", "tool-result-2", "condensed")),
		WithCondenser(CondenseTriggerToolCallSequenceCompleted, func(input CondenseInput) (CondenseDecision, error) {
			if input.Trigger != CondenseTriggerToolCallSequenceCompleted {
				return CondenseDecision{}, errors.New("unexpected trigger")
			}

			if input.Head.ID != toolResultTwoID {
				return CondenseDecision{}, errors.New("unexpected head")
			}

			return CondenseDecision{
				Condense:        true,
				ParentID:        userNodeID,
				SynthesizedFrom: []ID{toolResultOneID, toolResultTwoID},
				Message: Message{
					Role:    MessageRoleAssistant,
					Content: "summarized tool results",
				},
				Metadata: map[string]any{"strategy": "tool-batch-summary"},
			}, nil
		}),
	)

	rootNode, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil)
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	userNode, _, err := graph.Append(Message{Role: MessageRoleUser, Content: "do work"}, nil, rootNode.ID)
	if err != nil {
		t.Fatalf("append user: %v", err)
	}
	userNodeID = userNode.ID

	toolCallOne, _, err := graph.Append(Message{Role: MessageRoleAssistant, Content: "tool_use(1)"}, nil, userNode.ID)
	if err != nil {
		t.Fatalf("append tool call one: %v", err)
	}

	toolResultOne, _, err := graph.Append(Message{Role: MessageRoleTool, Content: "tool_result(1)"}, nil, toolCallOne.ID)
	if err != nil {
		t.Fatalf("append tool result one: %v", err)
	}
	toolResultOneID = toolResultOne.ID

	toolCallTwo, _, err := graph.Append(Message{Role: MessageRoleAssistant, Content: "tool_use(2)"}, nil, toolResultOne.ID)
	if err != nil {
		t.Fatalf("append tool call two: %v", err)
	}

	toolResultTwo, _, err := graph.Append(Message{Role: MessageRoleTool, Content: "tool_result(2)"}, nil, toolCallTwo.ID)
	if err != nil {
		t.Fatalf("append tool result two: %v", err)
	}
	toolResultTwoID = toolResultTwo.ID

	condensedNode, event, condensed, err := graph.TriggerCondensation(CondenseTriggerToolCallSequenceCompleted)
	if err != nil {
		t.Fatalf("trigger condensation: %v", err)
	}

	if !condensed {
		t.Fatal("expected condensation to be applied")
	}

	if got, want := event.Kind, EventKindHistoryRewritten; got != want {
		t.Fatalf("expected rewrite event kind %d, got %d", want, got)
	}

	if got, want := event.Event, DefaultEventNames().HistoryRewritten; got != want {
		t.Fatalf("expected rewrite event name %q, got %q", want, got)
	}

	if got, want := condensedNode.ParentIDs, []ID{userNode.ID}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("expected condensed parent %v, got %v", want, got)
	}

	messages, err := graph.ResumeMessages(condensedNode.ID)
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}

	if got, want := len(messages), 3; got != want {
		t.Fatalf("expected condensed lineage length %d, got %d", want, got)
	}

	if got, want := messages[2].Content, "summarized tool results"; got != want {
		t.Fatalf("expected condensed message %q, got %v", want, got)
	}
}

func TestTriggerCondensationPropagatesCondenserError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("condense failed")
	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "root")),
		WithCondenser(CondenseTriggerBeforePersist, func(input CondenseInput) (CondenseDecision, error) {
			return CondenseDecision{}, expectedErr
		}),
	)
	if _, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil); err != nil {
		t.Fatalf("append root: %v", err)
	}

	_, _, _, err := graph.TriggerCondensation(CondenseTriggerBeforePersist)
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected condenser error %v, got %v", expectedErr, err)
	}
}

func TestTriggerCondensationRequiresHarnessSelectedParentID(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "root")),
		WithCondenser(CondenseTriggerManual, func(input CondenseInput) (CondenseDecision, error) {
			return CondenseDecision{
				Condense:        true,
				SynthesizedFrom: []ID{input.Head.ID},
				Message:         Message{Role: MessageRoleAssistant, Content: "summary"},
			}, nil
		}),
	)

	if _, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil); err != nil {
		t.Fatalf("append root: %v", err)
	}

	_, _, _, err := graph.TriggerCondensation(CondenseTriggerManual)
	if !errors.Is(err, ErrCondensationInvalidDecision) {
		t.Fatalf("expected invalid decision error, got %v", err)
	}
}

func TestTriggerCondensationRequiresHarnessSelectedSources(t *testing.T) {
	t.Parallel()

	graph := NewGraph(
		WithIDGenerator(sequenceIDs("session-1", "root")),
		WithCondenser(CondenseTriggerManual, func(input CondenseInput) (CondenseDecision, error) {
			return CondenseDecision{
				Condense: true,
				ParentID: input.Head.ID,
				Message:  Message{Role: MessageRoleAssistant, Content: "summary"},
			}, nil
		}),
	)

	if _, _, err := graph.Append(Message{Role: MessageRoleSystem, Content: "sys"}, nil); err != nil {
		t.Fatalf("append root: %v", err)
	}

	_, _, _, err := graph.TriggerCondensation(CondenseTriggerManual)
	if !errors.Is(err, ErrCondensationInvalidDecision) {
		t.Fatalf("expected invalid decision error, got %v", err)
	}
}
