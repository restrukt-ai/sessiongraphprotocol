package jsonstore

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/restrukt-ai/sessiongraphprotocol/pkg/sgp"
)

func sequenceIDs(ids ...sgp.ID) sgp.IDGenerator {
	index := 0

	return func() sgp.ID {
		if index >= len(ids) {
			panic("sequenceIDs exhausted")
		}

		id := ids[index]
		index++

		return id
	}
}

func TestNewJSONFileStoreRequiresBaseDir(t *testing.T) {
	t.Parallel()

	_, err := NewJSONFileStore("   ")
	if err == nil {
		t.Fatal("expected error for blank base dir")
	}
}

func TestJSONFileStoreSaveRejectsNilGraph(t *testing.T) {
	t.Parallel()

	store, err := NewJSONFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	if err = store.Save(context.Background(), nil); !errors.Is(err, sgp.ErrNilGraph) {
		t.Fatalf("expected ErrNilGraph, got %v", err)
	}
}

func TestJSONFileStoreHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	store, err := NewJSONFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	graph := sgp.NewGraph(sgp.WithIDGenerator(sequenceIDs("session-1", "node-a")))
	if _, _, err = graph.Append(sgp.Message{System: &sgp.SystemMessage{Text: "sys"}}); err != nil {
		t.Fatalf("append root: %v", err)
	}

	if err = store.Save(ctx, graph); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation from save, got %v", err)
	}

	if _, err = store.Load(ctx, graph.Session().ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation from load, got %v", err)
	}
}

func TestJSONFileStoreWritesVersionedJSON(t *testing.T) {
	t.Parallel()

	baseDir := filepath.Join(t.TempDir(), "graphs")
	store, err := NewJSONFileStore(baseDir)
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	graph := sgp.NewGraph(sgp.WithIDGenerator(sequenceIDs("session-1", "node-a")))
	if _, _, err = graph.Append(sgp.Message{System: &sgp.SystemMessage{Text: "sys"}}); err != nil {
		t.Fatalf("append root: %v", err)
	}

	if err = store.Save(context.Background(), graph); err != nil {
		t.Fatalf("save graph: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(baseDir, "session-1.json"))
	if err != nil {
		t.Fatalf("read saved graph: %v", err)
	}

	var snapshot sgp.GraphSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		t.Fatalf("unmarshal saved snapshot: %v", err)
	}

	if got, want := snapshot.Version, uint32(sgp.CurrentGraphSnapshotVersion); got != want {
		t.Fatalf("expected saved version %d, got %d", want, got)
	}
}

func TestJSONFileStoreRejectsSnapshotWithoutVersion(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	store, err := NewJSONFileStore(baseDir)
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	snapshotWithoutVersion := sgp.GraphSnapshot{
		Session: sgp.Session{ID: "legacy/session"},
		Nodes: []sgp.Node{{
			ID:        "node-a",
			SessionID: "legacy/session",
			Message:   sgp.Message{System: &sgp.SystemMessage{Text: "sys"}},
		}},
		Events: []sgp.Event{{Event: sgp.DefaultEventNames().SessionStart, SessionID: "legacy/session"}},
		HeadID: "node-a",
	}

	data, err := json.MarshalIndent(snapshotWithoutVersion, "", "  ")
	if err != nil {
		t.Fatalf("marshal snapshot without version: %v", err)
	}

	if err = os.WriteFile(filepath.Join(baseDir, "legacy%2Fsession.json"), data, 0o644); err != nil {
		t.Fatalf("write snapshot without version: %v", err)
	}

	_, err = store.Load(context.Background(), "legacy/session")
	if !errors.Is(err, sgp.ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}

func TestJSONFileStoreRoundTrip(t *testing.T) {
	t.Parallel()

	store, err := NewJSONFileStore(filepath.Join(t.TempDir(), "graphs"))
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	graph := sgp.NewGraph(sgp.WithIDGenerator(sequenceIDs("session-1", "node-a", "node-b")))
	root, _, err := graph.Append(sgp.Message{System: &sgp.SystemMessage{Text: "sys"}})
	if err != nil {
		t.Fatalf("append root: %v", err)
	}

	userNode, _, err := graph.Append(sgp.Message{User: &sgp.UserMessage{Parts: []sgp.ContentPart{{Text: &sgp.TextPart{Text: "hello"}}}}}, root.ID)
	if err != nil {
		t.Fatalf("append user: %v", err)
	}

	ctx := context.Background()
	if err = store.Save(ctx, graph); err != nil {
		t.Fatalf("save graph: %v", err)
	}

	restored, err := store.Load(ctx, graph.Session().ID)
	if err != nil {
		t.Fatalf("load graph: %v", err)
	}

	needsResponse, err := restored.NeedsResponse(userNode.ID)
	if err != nil {
		t.Fatalf("needs response: %v", err)
	}

	if !needsResponse {
		t.Fatal("expected persisted dangling user node to still require a response")
	}

	messages, err := restored.ResumeMessages(userNode.ID)
	if err != nil {
		t.Fatalf("resume messages: %v", err)
	}

	if got, want := len(messages), 2; got != want {
		t.Fatalf("expected %d messages, got %d", want, got)
	}
}

func TestJSONFileStoreMissingGraph(t *testing.T) {
	t.Parallel()

	store, err := NewJSONFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	_, err = store.Load(context.Background(), "missing")
	if !errors.Is(err, sgp.ErrGraphNotFound) {
		t.Fatalf("expected ErrGraphNotFound, got %v", err)
	}
}

func TestJSONFileStoreRejectsInvalidJSON(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	store, err := NewJSONFileStore(baseDir)
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	if err = os.WriteFile(filepath.Join(baseDir, "broken.json"), []byte("{not-json"), 0o644); err != nil {
		t.Fatalf("write invalid json: %v", err)
	}

	_, err = store.Load(context.Background(), "broken")
	if err == nil {
		t.Fatal("expected invalid json load to fail")
	}
}

func TestJSONFileStoreRejectsInvalidSnapshotFile(t *testing.T) {
	t.Parallel()

	baseDir := t.TempDir()
	store, err := NewJSONFileStore(baseDir)
	if err != nil {
		t.Fatalf("new json file store: %v", err)
	}

	invalidSnapshot := sgp.GraphSnapshot{
		Version: sgp.CurrentGraphSnapshotVersion,
		Session: sgp.Session{ID: "broken"},
		HeadID:  "missing",
	}

	data, err := json.Marshal(invalidSnapshot)
	if err != nil {
		t.Fatalf("marshal invalid snapshot: %v", err)
	}

	if err = os.WriteFile(filepath.Join(baseDir, "broken.json"), data, 0o644); err != nil {
		t.Fatalf("write invalid snapshot: %v", err)
	}

	_, err = store.Load(context.Background(), "broken")
	if !errors.Is(err, sgp.ErrInvalidSnapshot) {
		t.Fatalf("expected ErrInvalidSnapshot, got %v", err)
	}
}
