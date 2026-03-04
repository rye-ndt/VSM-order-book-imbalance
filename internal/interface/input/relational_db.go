package input

import (
	"context"
	"database/sql"
)

// RelationalDB defines the input port for connecting to a relational database.
// Adapters (e.g. Postgres) should implement this interface in the internal/modules package.
type RelationalDB interface {
	// Connect establishes a connection to the underlying relational database and
	// returns an initialized *sql.DB. Implementations should ensure the
	// connection is validated (for example with PingContext) before returning.
	Connect(ctx context.Context) (*sql.DB, error)
}

