package sessiongraphprotocol

import (
	"errors"
	"fmt"
)

var (
	// ErrCondensationUnavailable indicates that no condenser is configured for a trigger.
	ErrCondensationUnavailable = errors.New("condensation unavailable for trigger")
	// ErrCondensationChangedHead indicates the graph head changed during condensation planning.
	ErrCondensationChangedHead = errors.New("graph head changed during condensation")
	// ErrCondensationInvalidDecision indicates the harness returned an incomplete rewrite plan.
	ErrCondensationInvalidDecision = errors.New("condensation decision is invalid")
)

// CondenseTrigger defines harness-level events that may trigger context condensation.
type CondenseTrigger uint8

const (
	// CondenseTriggerManual allows callers to request condensation explicitly.
	CondenseTriggerManual CondenseTrigger = iota + 1
	// CondenseTriggerToolCallSequenceCompleted indicates a multi-tool-call sequence completed.
	CondenseTriggerToolCallSequenceCompleted
	// CondenseTriggerBeforePersist indicates condensation before persisting context.
	CondenseTriggerBeforePersist
)

// CondenseInput contains context provided to a Condenser.
type CondenseInput struct {
	Trigger CondenseTrigger
	Session Session
	Head    Node
	Lineage []Node
	Events  []Event
}

// CondenseDecision describes the rewrite to apply when condensing context.
type CondenseDecision struct {
	Condense bool
	// ParentID must be set by the harness; Graph does not choose rewrite parents.
	ParentID ID
	// SynthesizedFrom must be set by the harness; Graph does not choose merge sources.
	SynthesizedFrom []ID
	Message         Message
	Metadata        map[string]any
}

// Condenser decides whether and how to condense context for a trigger.
type Condenser func(input CondenseInput) (CondenseDecision, error)

// WithCondenser registers a condenser for a specific trigger.
func WithCondenser(trigger CondenseTrigger, condenser Condenser) Option {
	return func(cfg *config) {
		if trigger == 0 || condenser == nil {
			return
		}

		if cfg.condensers == nil {
			cfg.condensers = make(map[CondenseTrigger]Condenser)
		}

		cfg.condensers[trigger] = condenser
	}
}

func copyCondensers(condensers map[CondenseTrigger]Condenser) map[CondenseTrigger]Condenser {
	if len(condensers) == 0 {
		return nil
	}

	cloned := make(map[CondenseTrigger]Condenser, len(condensers))
	for trigger, condenser := range condensers {
		cloned[trigger] = condenser
	}

	return cloned
}

// TriggerCondensation invokes the configured condenser for a trigger.
//
// If no condenser is configured, this returns condensed=false and no error.
func (graph *Graph) TriggerCondensation(trigger CondenseTrigger) (Node, Event, bool, error) {
	if trigger == 0 {
		return Node{}, Event{}, false, fmt.Errorf("%w: trigger is required", ErrCondensationUnavailable)
	}

	graph.mu.RLock()
	condenser, exists := graph.condensers[trigger]
	if !exists {
		graph.mu.RUnlock()
		return Node{}, Event{}, false, nil
	}

	if graph.headID == "" {
		graph.mu.RUnlock()
		return Node{}, Event{}, false, nil
	}

	headID := graph.headID
	headNode := copyNode(graph.nodes[headID])
	lineage, err := graph.resumeNodes(headID)
	if err != nil {
		graph.mu.RUnlock()
		return Node{}, Event{}, false, err
	}

	input := CondenseInput{
		Trigger: trigger,
		Session: Session{
			ID:          graph.session.ID,
			Timestamp:   graph.session.Timestamp,
			SpawnedFrom: copySpawnReference(graph.session.SpawnedFrom),
		},
		Head:    headNode,
		Lineage: lineage,
		Events:  copyEvents(graph.events),
	}
	graph.mu.RUnlock()

	decision, err := condenser(input)
	if err != nil {
		return Node{}, Event{}, false, err
	}

	if !decision.Condense {
		return Node{}, Event{}, false, nil
	}

	if decision.ParentID == "" {
		return Node{}, Event{}, false, fmt.Errorf("%w: parent_id is required", ErrCondensationInvalidDecision)
	}

	if len(decision.SynthesizedFrom) == 0 {
		return Node{}, Event{}, false, fmt.Errorf("%w: synthesized_from is required", ErrCondensationInvalidDecision)
	}

	graph.mu.Lock()
	defer graph.mu.Unlock()

	if graph.headID != headID {
		return Node{}, Event{}, false, ErrCondensationChangedHead
	}

	node, event, err := graph.appendNode(
		EventKindHistoryRewritten,
		decision.Message,
		decision.Metadata,
		[]ID{decision.ParentID},
		decision.SynthesizedFrom,
	)
	if err != nil {
		return Node{}, Event{}, false, err
	}

	return copyNode(node), copyEvent(event), true, nil
}
