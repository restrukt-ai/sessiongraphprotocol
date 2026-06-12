package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/restrukt-ai/sessiongraphprotocol/pkg/sgp"
)

// Edge is a directed graph edge returned by GetSessionGraph.
type Edge struct {
	FromID sgp.ID
	ToID   sgp.ID
	Kind   string // "parent" | "synthesized_from" | "spawned_from"
}

// EventRow pairs a sequence number with its deserialized event, used for
// gap-safe history replay in WatchSession.
type EventRow struct {
	Seq   int64
	Event sgp.Event
}

// PGStore implements sgp.Store and exposes extended graph query methods backed
// by Postgres with Apache AGE.
type PGStore struct {
	pool   *pgxpool.Pool
	broker *NotifyBroker
}

var _ sgp.Store = (*PGStore)(nil)

// NewPGStore creates a PGStore. The pool's AfterConnect hook must already
// install AGE (LOAD 'age'; SET search_path = ...) on every connection.
func NewPGStore(pool *pgxpool.Pool, broker *NotifyBroker) *PGStore {
	return &PGStore{pool: pool, broker: broker}
}

// AppendEvent appends the event to the event log and mirrors it into the AGE
// graph, then notifies live subscribers via pg_notify.
func (s *PGStore) AppendEvent(ctx context.Context, sessionID sgp.ID, event sgp.Event) error {
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Advisory lock serialises writes to the same session.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, string(sessionID)); err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}

		var seq int64
		if err := tx.QueryRow(ctx,
			`INSERT INTO sgp_events (session_id, event_json) VALUES ($1, $2) RETURNING seq`,
			string(sessionID), eventJSON).Scan(&seq); err != nil {
			return fmt.Errorf("insert event: %w", err)
		}

		if err := s.applyAGE(ctx, tx, sessionID, event); err != nil {
			return fmt.Errorf("age write: %w", err)
		}

		if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`,
			"sgp:"+string(sessionID), strconv.FormatInt(seq, 10)); err != nil {
			return fmt.Errorf("pg_notify: %w", err)
		}

		return nil
	})
}

func (s *PGStore) applyAGE(ctx context.Context, tx pgx.Tx, sessionID sgp.ID, event sgp.Event) error {
	switch event.Kind {
	case sgp.EventKindSessionStart:
		if err := execCypher(ctx, tx,
			`CREATE (:Session {id: $id, timestamp: $ts})`,
			map[string]any{
				"id": string(sessionID),
				"ts": event.Timestamp.Format(time.RFC3339),
			}); err != nil {
			return err
		}
		if event.SpawnedFrom != nil {
			// Link new session to the parent node it was spawned from.
			if err := execCypher(ctx, tx, `
				MATCH (sess:Session {id: $sessID}), (n:Node {id: $nodeID})
				CREATE (sess)-[:SPAWNED_FROM]->(n)`,
				map[string]any{
					"sessID": string(sessionID),
					"nodeID": string(event.SpawnedFrom.NodeID),
				}); err != nil {
				return err
			}
		}

	case sgp.EventKindNodeAppended, sgp.EventKindHistoryRewritten:
		n := event.Node
		if n == nil {
			return nil
		}
		if err := execCypher(ctx, tx,
			`CREATE (:Node {id: $id, session_id: $sid, role: $role})`,
			map[string]any{
				"id":   string(n.ID),
				"sid":  string(n.SessionID),
				"role": string(n.Message.Role()),
			}); err != nil {
			return err
		}
		for _, parentID := range n.ParentIDs {
			if err := execCypher(ctx, tx, `
				MATCH (child:Node {id: $cid}), (parent:Node {id: $pid})
				CREATE (child)-[:PARENT]->(parent)`,
				map[string]any{"cid": string(n.ID), "pid": string(parentID)}); err != nil {
				return err
			}
		}
		for _, srcID := range n.SynthesizedFrom {
			if err := execCypher(ctx, tx, `
				MATCH (child:Node {id: $cid}), (src:Node {id: $sid})
				CREATE (child)-[:SYNTHESIZED_FROM]->(src)`,
				map[string]any{"cid": string(n.ID), "sid": string(srcID)}); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadEvents returns all events for sessionID ordered by seq.
// Returns sgp.ErrGraphNotFound if no events exist.
func (s *PGStore) LoadEvents(ctx context.Context, sessionID sgp.ID) ([]sgp.Event, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_json FROM sgp_events WHERE session_id = $1 ORDER BY seq`,
		string(sessionID))
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	var events []sgp.Event
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("unmarshal event: %w", err)
		}
		event.Kind = sgp.ClassifyEvent(event)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}

	if len(events) == 0 {
		return nil, fmt.Errorf("%w: %s", sgp.ErrGraphNotFound, sessionID)
	}
	return events, nil
}

// LoadEventsWithSeq returns events paired with their sequence numbers, ordered
// by seq. Used by WatchSession for gap-safe history replay.
func (s *PGStore) LoadEventsWithSeq(ctx context.Context, sessionID sgp.ID) ([]EventRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT seq, event_json FROM sgp_events WHERE session_id = $1 ORDER BY seq`,
		string(sessionID))
	if err != nil {
		return nil, fmt.Errorf("query events with seq: %w", err)
	}
	defer rows.Close()

	var result []EventRow
	for rows.Next() {
		var seq int64
		var data []byte
		if err := rows.Scan(&seq, &data); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("unmarshal event row: %w", err)
		}
		event.Kind = sgp.ClassifyEvent(event)
		result = append(result, EventRow{Seq: seq, Event: event})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event rows: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("%w: %s", sgp.ErrGraphNotFound, sessionID)
	}
	return result, nil
}

// GetResumeContext returns the canonical lineage (root → nodeID) by traversing
// PARENT edges in AGE, then hydrating nodes from the event log.
func (s *PGStore) GetResumeContext(ctx context.Context, nodeID sgp.ID) ([]sgp.Node, error) {
	// AGE traversal: walk PARENT edges from nodeID up to root, return IDs root→node.
	rows, err := s.pool.Query(ctx, `
		SELECT id::text
		FROM ag_catalog.cypher('sgp', $$
			MATCH p = (n:Node {id: $nodeID})-[:PARENT*0..]->(root:Node)
			WHERE NOT EXISTS { MATCH (root)-[:PARENT]->() }
			UNWIND reverse([x IN nodes(p) | x.id]) AS id
			RETURN id
		$$, $1) AS (id ag_catalog.agtype)
	`, fmt.Sprintf(`{"nodeID": %q}`, string(nodeID)))
	if err != nil {
		return nil, fmt.Errorf("age lineage query: %w", err)
	}
	defer rows.Close()

	var orderedIDs []string
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan lineage id: %w", err)
		}
		orderedIDs = append(orderedIDs, stripAgtypeQuotes(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read lineage ids: %w", err)
	}

	if len(orderedIDs) == 0 {
		return nil, fmt.Errorf("%w: %s", sgp.ErrNodeNotFound, nodeID)
	}

	// Fetch node data from event log.
	nodeMap, err := s.fetchNodesByIDs(ctx, orderedIDs)
	if err != nil {
		return nil, err
	}

	result := make([]sgp.Node, 0, len(orderedIDs))
	for _, id := range orderedIDs {
		n, ok := nodeMap[id]
		if !ok {
			return nil, fmt.Errorf("%w: %s", sgp.ErrNodeNotFound, id)
		}
		result = append(result, n)
	}
	return result, nil
}

// GetSessionGraph returns all nodes and edges for a session.
func (s *PGStore) GetSessionGraph(ctx context.Context, sessionID sgp.ID) ([]sgp.Node, []Edge, error) {
	nodes, err := s.loadSessionNodes(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	edges, err := s.loadSessionEdges(ctx, sessionID)
	if err != nil {
		return nil, nil, err
	}

	return nodes, edges, nil
}

func (s *PGStore) loadSessionNodes(ctx context.Context, sessionID sgp.ID) ([]sgp.Node, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT event_json FROM sgp_events
		 WHERE session_id = $1 AND event_json ? 'node'
		 ORDER BY seq`,
		string(sessionID))
	if err != nil {
		return nil, fmt.Errorf("query session nodes: %w", err)
	}
	defer rows.Close()

	var nodes []sgp.Node
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan node event: %w", err)
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, fmt.Errorf("unmarshal node event: %w", err)
		}
		if event.Node != nil {
			nodes = append(nodes, *event.Node)
		}
	}
	return nodes, rows.Err()
}

func (s *PGStore) loadSessionEdges(ctx context.Context, sessionID sgp.ID) ([]Edge, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT from_id::text, to_id::text, kind::text
		FROM ag_catalog.cypher('sgp', $$
			MATCH (a:Node {session_id: $sid})-[e]->(b:Node)
			RETURN a.id AS from_id, b.id AS to_id, type(e) AS kind
		$$, $1) AS (from_id ag_catalog.agtype, to_id ag_catalog.agtype, kind ag_catalog.agtype)
	`, fmt.Sprintf(`{"sid": %q}`, string(sessionID)))
	if err != nil {
		return nil, fmt.Errorf("age edge query: %w", err)
	}
	defer rows.Close()

	var edges []Edge
	for rows.Next() {
		var fromRaw, toRaw, kindRaw string
		if err := rows.Scan(&fromRaw, &toRaw, &kindRaw); err != nil {
			return nil, fmt.Errorf("scan edge: %w", err)
		}
		edges = append(edges, Edge{
			FromID: sgp.ID(stripAgtypeQuotes(fromRaw)),
			ToID:   sgp.ID(stripAgtypeQuotes(toRaw)),
			Kind:   strings.ToLower(stripAgtypeQuotes(kindRaw)),
		})
	}
	return edges, rows.Err()
}

// ListSessions returns sessions ordered by first event time, with keyset pagination.
// pageToken is the last seen session_id.
func (s *PGStore) ListSessions(ctx context.Context, limit int, pageToken string) ([]sgp.Session, string, error) {
	if limit <= 0 {
		limit = 50
	}

	var rows pgx.Rows
	var err error
	if pageToken == "" {
		rows, err = s.pool.Query(ctx, `
			SELECT DISTINCT ON (session_id) session_id, event_json, created_at
			FROM sgp_events
			WHERE event_json->>'event' LIKE 'session%start%' OR event_json ? 'session_id'
			ORDER BY session_id, seq
			LIMIT $1
		`, limit+1)
	} else {
		rows, err = s.pool.Query(ctx, `
			WITH first_events AS (
				SELECT DISTINCT ON (session_id) session_id, event_json, created_at
				FROM sgp_events
				ORDER BY session_id, seq
			)
			SELECT session_id, event_json, created_at
			FROM first_events
			WHERE session_id > $1
			ORDER BY session_id
			LIMIT $2
		`, pageToken, limit+1)
	}
	if err != nil {
		return nil, "", fmt.Errorf("list sessions query: %w", err)
	}
	defer rows.Close()

	var sessions []sgp.Session
	for rows.Next() {
		var sid string
		var data []byte
		var createdAt time.Time
		if err := rows.Scan(&sid, &data, &createdAt); err != nil {
			return nil, "", fmt.Errorf("scan session row: %w", err)
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			continue
		}
		sess := sgp.Session{
			ID:          sgp.ID(sid),
			Timestamp:   event.Timestamp,
			SpawnedFrom: event.SpawnedFrom,
		}
		sessions = append(sessions, sess)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sessions: %w", err)
	}

	var nextToken string
	if len(sessions) > limit {
		sessions = sessions[:limit]
		nextToken = string(sessions[len(sessions)-1].ID)
	}
	return sessions, nextToken, nil
}

// GetSession returns session metadata and current HEAD node id.
func (s *PGStore) GetSession(ctx context.Context, sessionID sgp.ID) (sgp.Session, sgp.ID, SessionStatus, error) {
	events, err := s.LoadEvents(ctx, sessionID)
	if err != nil {
		return sgp.Session{}, "", 0, err
	}

	graph, err := sgp.RestoreFromEvents(events)
	if err != nil {
		return sgp.Session{}, "", 0, fmt.Errorf("restore graph: %w", err)
	}

	sess := graph.Session()
	head, _ := graph.Head()

	// Determine status from last event.
	status := SessionStatusOpen
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == sgp.EventKindSessionEnded {
			status = SessionStatusClosed
			break
		}
	}

	return sess, head.ID, status, nil
}

// GetNode fetches a single node by ID from the event log.
func (s *PGStore) GetNode(ctx context.Context, nodeID sgp.ID) (sgp.Node, error) {
	nodeMap, err := s.fetchNodesByIDs(ctx, []string{string(nodeID)})
	if err != nil {
		return sgp.Node{}, err
	}
	n, ok := nodeMap[string(nodeID)]
	if !ok {
		return sgp.Node{}, fmt.Errorf("%w: %s", sgp.ErrNodeNotFound, nodeID)
	}
	return n, nil
}

// Subscribe returns a channel that receives Observations for sessionID.
func (s *PGStore) Subscribe(ctx context.Context, sessionID sgp.ID) (<-chan Observation, func()) {
	return s.broker.Subscribe(ctx, string(sessionID))
}

// fetchNodesByIDs fetches nodes from the event log by ID.
func (s *PGStore) fetchNodesByIDs(ctx context.Context, ids []string) (map[string]sgp.Node, error) {
	if len(ids) == 0 {
		return map[string]sgp.Node{}, nil
	}

	// Build the IN clause with positional args.
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	rows, err := s.pool.Query(ctx,
		fmt.Sprintf(`
			SELECT event_json FROM sgp_events
			WHERE event_json->>'id' IS NULL
			  AND event_json->'node'->>'id' = ANY(ARRAY[%s])
			ORDER BY seq
		`, strings.Join(placeholders, ",")),
		args...)
	if err != nil {
		// Fallback: scan all session events if the above fails.
		return s.fetchNodesByIDsSlow(ctx, ids)
	}
	defer rows.Close()

	result := make(map[string]sgp.Node, len(ids))
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, fmt.Errorf("scan node row: %w", err)
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			continue
		}
		if event.Node != nil {
			result[string(event.Node.ID)] = *event.Node
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *PGStore) fetchNodesByIDsSlow(ctx context.Context, ids []string) (map[string]sgp.Node, error) {
	idSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		idSet[id] = struct{}{}
	}

	rows, err := s.pool.Query(ctx,
		`SELECT event_json FROM sgp_events WHERE event_json ? 'node' ORDER BY seq`)
	if err != nil {
		return nil, fmt.Errorf("slow fetch nodes: %w", err)
	}
	defer rows.Close()

	result := make(map[string]sgp.Node, len(ids))
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			continue
		}
		var event sgp.Event
		if err := json.Unmarshal(data, &event); err != nil {
			continue
		}
		if event.Node != nil {
			if _, wanted := idSet[string(event.Node.ID)]; wanted {
				result[string(event.Node.ID)] = *event.Node
			}
		}
	}
	return result, rows.Err()
}
