CREATE TABLE sgp_events (
    session_id  TEXT        NOT NULL,
    seq         BIGINT      NOT NULL GENERATED ALWAYS AS IDENTITY,
    event_json  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, seq)
);

CREATE TABLE sgp_nodes (
    id         TEXT   NOT NULL PRIMARY KEY,
    session_id TEXT   NOT NULL,
    role       TEXT   NOT NULL,
    parent_ids TEXT[] NOT NULL DEFAULT '{}',
    synth_from TEXT[] NOT NULL DEFAULT '{}'
);
