-- name: InsertEvent :one
INSERT INTO sgp_events (session_id, event_json)
VALUES (@session_id, @event_json)
RETURNING seq;

-- name: InsertNode :exec
INSERT INTO sgp_nodes (id, session_id, role, parent_ids, synth_from)
VALUES (@id, @session_id, @role, @parent_ids, @synth_from)
ON CONFLICT (id) DO NOTHING;

-- name: AcquireSessionLock :exec
SELECT pg_advisory_xact_lock(hashtext(@session_id::text));

-- name: NotifySession :exec
SELECT pg_notify(@channel, @payload);

-- name: LoadEventsBySession :many
SELECT event_json
FROM sgp_events
WHERE session_id = @session_id
ORDER BY seq;

-- name: LoadEventsWithSeq :many
SELECT seq, event_json
FROM sgp_events
WHERE session_id = @session_id
ORDER BY seq;

-- name: FetchEventBySeq :one
SELECT event_json
FROM sgp_events
WHERE session_id = @session_id
  AND seq = @seq;

-- name: CountNodesBySession :one
SELECT count(*)::int
FROM sgp_events
WHERE session_id = @session_id
  AND event_json ? 'node';

-- name: ListSessionsFirst :many
SELECT DISTINCT ON (session_id) session_id, event_json, created_at
FROM sgp_events
ORDER BY session_id, seq
LIMIT @lim;

-- name: ListSessionsAfter :many
WITH first_events AS (
    SELECT DISTINCT ON (session_id) session_id, event_json, created_at
    FROM sgp_events
    ORDER BY session_id, seq
)
SELECT session_id, event_json, created_at
FROM first_events
WHERE session_id > @page_token
ORDER BY session_id
LIMIT @lim;

-- name: GetLineage :many
WITH RECURSIVE lineage(id, parent_ids, depth) AS (
    SELECT sgp_nodes.id, sgp_nodes.parent_ids, 0
    FROM sgp_nodes
    WHERE sgp_nodes.id = @node_id
    UNION ALL
    SELECT n.id, n.parent_ids, l.depth + 1
    FROM sgp_nodes n
    INNER JOIN lineage l ON n.id = l.parent_ids[1]
    WHERE array_length(l.parent_ids, 1) > 0
)
SELECT id FROM lineage ORDER BY depth DESC;

-- name: GetEdgesBySession :many
SELECT sgp_nodes.id AS from_id, unnest(parent_ids) AS to_id, 'parent'::text AS kind
FROM sgp_nodes
WHERE sgp_nodes.session_id = @session_id
  AND array_length(parent_ids, 1) > 0
UNION ALL
SELECT sgp_nodes.id AS from_id, unnest(synth_from) AS to_id, 'synthesized_from'::text AS kind
FROM sgp_nodes
WHERE sgp_nodes.session_id = @session_id
  AND array_length(synth_from, 1) > 0;

-- name: GetNodesBySession :many
SELECT event_json
FROM sgp_events
WHERE session_id = @session_id
  AND event_json ? 'node'
ORDER BY seq;

-- name: FetchNodesByIDs :many
SELECT event_json
FROM sgp_events
WHERE event_json->'node'->>'id' = ANY(@ids::text[])
ORDER BY seq;
