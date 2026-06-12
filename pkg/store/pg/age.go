package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// execCypher executes a Cypher query against the 'sgp' AGE graph via the
// ag_catalog.cypher() SQL function. params is serialised to JSON and passed
// as the third argument to cypher().
func execCypher(ctx context.Context, tx pgx.Tx, cypher string, params map[string]any) error {
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal cypher params: %w", err)
	}
	_, err = tx.Exec(ctx,
		`SELECT * FROM ag_catalog.cypher('sgp', $1, $2) AS (result ag_catalog.agtype)`,
		cypher, string(paramsJSON))
	return err
}

// stripAgtypeQuotes removes the surrounding double-quotes that AGE adds to
// agtype string values when cast to ::text (e.g. `"some-uuid"` → `some-uuid`).
func stripAgtypeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}
