package modules

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
)

// PostgresRelationalDB is a Postgres implementation of the RelationalDB input port.
type PostgresRelationalDB struct {
	cfg config.DBConfig
}

// NewPostgresRelationalDB builds a new Postgres adapter using DB configuration.
func NewPostgresRelationalDB(cfg config.DBConfig) input.RelationalDB {
	return &PostgresRelationalDB{
		cfg: cfg,
	}
}

// Connect opens and validates a connection to Postgres using the configured secrets.
func (p *PostgresRelationalDB) Connect(ctx context.Context) (*sql.DB, error) {
	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		p.cfg.User,
		p.cfg.Password,
		p.cfg.Host,
		p.cfg.Port,
		p.cfg.Name,
		p.cfg.SSLMode,
	)

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return db, nil
}
