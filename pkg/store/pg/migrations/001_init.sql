-- +goose Up

CREATE EXTENSION IF NOT EXISTS age;
LOAD 'age';
SET search_path = ag_catalog, "$user", public;

CREATE TABLE sgp_events (
    session_id  TEXT        NOT NULL,
    seq         BIGINT      NOT NULL GENERATED ALWAYS AS IDENTITY,
    event_json  JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, seq)
);
CREATE INDEX ON sgp_events (session_id);
CREATE INDEX ON sgp_events (created_at);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ag_catalog.ag_graph WHERE name = 'sgp') THEN
        PERFORM ag_catalog.create_graph('sgp');
    END IF;
END $$;

-- +goose Down
DROP TABLE IF EXISTS sgp_events;
