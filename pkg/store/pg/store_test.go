package pg_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/sgp"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/store/pg"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}

	t.Cleanup(pool.Close)

	return pool
}

func testStore(t *testing.T) *pg.Store {
	t.Helper()

	dsn := os.Getenv("TEST_DATABASE_URL")
	pool := testPool(t)

	err := pg.Migrate(context.Background(), dsn)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	return pg.NewStore(pool, nil)
}

func startSession(t *testing.T, store *pg.Store) (*sgp.Graph, sgp.ID) {
	t.Helper()

	ctx := context.Background()
	g := sgp.NewGraph()
	sid := g.Session().ID

	ev, err := g.Start()
	if err != nil {
		t.Fatalf("graph start: %v", err)
	}

	err = store.AppendEvent(ctx, sid, ev)
	if err != nil {
		t.Fatalf("append start: %v", err)
	}

	return g, sid
}

func appendUserNode(t *testing.T, store *pg.Store, g *sgp.Graph, parentIDs []sgp.ID) sgp.ID {
	t.Helper()

	ctx := context.Background()
	sid := g.Session().ID

	_, ev, err := g.Append(sgp.Message{User: &sgp.UserMessage{
		Parts: []sgp.ContentPart{{Text: &sgp.TextPart{Text: "hello"}}},
	}}, parentIDs...)
	if err != nil {
		t.Fatalf("graph append: %v", err)
	}

	err = store.AppendEvent(ctx, sid, ev)
	if err != nil {
		t.Fatalf("append node: %v", err)
	}

	return ev.Node.ID
}

func TestAppendAndLoadEvents(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	g, sid := startSession(t, store)

	id1 := appendUserNode(t, store, g, nil)
	appendUserNode(t, store, g, []sgp.ID{id1})

	events, err := store.LoadEvents(ctx, sid)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
}

func TestGetResumeContextLinearChain(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	g, _ := startSession(t, store)

	id1 := appendUserNode(t, store, g, nil)
	id2 := appendUserNode(t, store, g, []sgp.ID{id1})
	id3 := appendUserNode(t, store, g, []sgp.ID{id2})

	nodes, err := store.GetResumeContext(ctx, id3)
	if err != nil {
		t.Fatalf("get resume context: %v", err)
	}

	if len(nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(nodes))
	}

	// root-first order: id1, id2, id3
	if nodes[0].ID != id1 {
		t.Errorf("expected root %s, got %s", id1, nodes[0].ID)
	}

	if nodes[2].ID != id3 {
		t.Errorf("expected leaf %s, got %s", id3, nodes[2].ID)
	}
}

func TestGetResumeContextSingleNode(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	g, _ := startSession(t, store)

	id1 := appendUserNode(t, store, g, nil)

	nodes, err := store.GetResumeContext(ctx, id1)
	if err != nil {
		t.Fatalf("get resume context: %v", err)
	}

	if len(nodes) != 1 || nodes[0].ID != id1 {
		t.Fatalf("expected single node %s", id1)
	}
}

func TestGetSessionGraph(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	g, sid := startSession(t, store)

	id1 := appendUserNode(t, store, g, nil)
	appendUserNode(t, store, g, []sgp.ID{id1})

	nodes, edges, err := store.GetSessionGraph(ctx, sid)
	if err != nil {
		t.Fatalf("get session graph: %v", err)
	}

	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	if len(edges) != 1 {
		t.Fatalf("expected 1 edge, got %d", len(edges))
	}

	if edges[0].Kind != "parent" {
		t.Errorf("expected edge kind 'parent', got %q", edges[0].Kind)
	}
}

func TestListSessionsNoPagination(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()

	for range 3 {
		startSession(t, store)
	}

	sessions, nextToken, err := store.ListSessions(ctx, 50, "")
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}

	if len(sessions) < 3 {
		t.Fatalf("expected at least 3 sessions, got %d", len(sessions))
	}

	if nextToken != "" {
		t.Errorf("expected empty next token, got %q", nextToken)
	}
}

func TestListSessionsWithPagination(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()

	// Create 5 known sessions and collect their IDs for verification.
	for range 5 {
		startSession(t, store)
	}

	// Page with limit=2.
	page1, tok1, err := store.ListSessions(ctx, 2, "")
	if err != nil {
		t.Fatalf("list sessions page1: %v", err)
	}

	if len(page1) != 2 {
		t.Fatalf("page1 expected 2, got %d", len(page1))
	}

	if tok1 == "" {
		t.Fatal("expected non-empty next token after page1")
	}

	page2, _, err := store.ListSessions(ctx, 2, tok1)
	if err != nil {
		t.Fatalf("list sessions page2: %v", err)
	}

	if len(page2) == 0 {
		t.Fatal("expected at least 1 session on page2")
	}

	// No overlap.
	seen := make(map[sgp.ID]bool)
	for _, s := range page1 {
		seen[s.ID] = true
	}

	for _, s := range page2 {
		if seen[s.ID] {
			t.Errorf("duplicate session %s across pages", s.ID)
		}
	}
}

func TestGetNode(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	g, _ := startSession(t, store)

	nodeID := appendUserNode(t, store, g, nil)

	node, err := store.GetNode(ctx, nodeID)
	if err != nil {
		t.Fatalf("get node: %v", err)
	}

	if node.ID != nodeID {
		t.Errorf("expected node id %s, got %s", nodeID, node.ID)
	}
}
