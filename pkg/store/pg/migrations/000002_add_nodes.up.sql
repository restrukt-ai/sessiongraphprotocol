CREATE TABLE IF NOT EXISTS sgp_nodes (
    id         TEXT   NOT NULL,
    session_id TEXT   NOT NULL,
    role       TEXT   NOT NULL,
    parent_ids TEXT[] NOT NULL DEFAULT '{}',
    synth_from TEXT[] NOT NULL DEFAULT '{}'
);
CREATE UNIQUE INDEX IF NOT EXISTS sgp_nodes_id_idx ON sgp_nodes (id);
CREATE INDEX IF NOT EXISTS sgp_nodes_session_id_idx ON sgp_nodes (session_id);
