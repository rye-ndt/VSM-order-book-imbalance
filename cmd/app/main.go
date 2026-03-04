package main

import (
	"context"
	"flag"
	"log"
	"net/http"

	"github.com/robfig/cron/v3"
	// pgx stdlib adapter registers the "pgx" driver name with database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/job"
	"github.com/example/order-book-imbalance/internal/modules"
	"github.com/example/order-book-imbalance/internal/server"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// -----------------------------------------------------------------------
	// Database
	// -----------------------------------------------------------------------
	dbAdapter := modules.NewPostgresRelationalDB(cfg.DB)
	db, err := dbAdapter.Connect(context.Background())
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer db.Close()

	// -----------------------------------------------------------------------
	// Dependency injection
	//
	// Both dependencies are expressed as interfaces (the output/input ports).
	// Swap either constructor below to change the storage backend or data
	// source without touching any other file.
	//
	//   stockClient  →  input.StockDataClient
	//   store        →  output.MarketStore
	// -----------------------------------------------------------------------
	stockClient := modules.NewSSIStockClient(cfg.SSI)
	store := modules.NewPostgresMarketStore(db)

	// Run schema migration once at startup before the first job execution.
	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("migrate market tables: %v", err)
	}

	// -----------------------------------------------------------------------
	// Cron scheduler – runs market data fetch every day at 03:30
	// -----------------------------------------------------------------------
	c := cron.New()
	if _, err := c.AddJob("30 3 * * *", job.NewMarketDataJob(stockClient, store)); err != nil {
		log.Fatalf("register market data cron job: %v", err)
	}
	c.Start()
	defer c.Stop()

	// -----------------------------------------------------------------------
	// HTTP server
	// -----------------------------------------------------------------------
	srv := server.NewHTTPServer(cfg)
	log.Printf("starting HTTP server on %s", cfg.HTTPListenAddr)
	if err := http.ListenAndServe(cfg.HTTPListenAddr, srv.Router()); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
