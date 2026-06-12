-- +goose Up

CREATE EXTENSION IF NOT EXISTS age;

CREATE TABLE IF NOT EXISTS sgp_events (
    session_id  TEXT        NOT NULL,
    seq         BIGINT      NOT NULL GENERATED ALWAYS AS IDENTITY,
    event_json  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, seq)
);
CREATE INDEX IF NOT EXISTS sgp_events_session_id_idx ON sgp_events (session_id);
CREATE INDEX IF NOT EXISTS sgp_events_created_at_idx ON sgp_events (created_at);

-- +goose Down
DROP TABLE IF EXISTS sgp_events;
