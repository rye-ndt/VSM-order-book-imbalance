package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/robfig/cron/v3"

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
		log.Fatalf("failed to load config: %v", err)
	}

	// -----------------------------------------------------------------------
	// Dependency injection
	//
	// NewSSIStockClient returns input.StockDataClient (the port interface).
	// To swap the data source, replace this line with any other constructor
	// that satisfies the same interface — nothing else in the app changes.
	// -----------------------------------------------------------------------
	stockClient := modules.NewSSIStockClient(cfg.SSI)

	// -----------------------------------------------------------------------
	// Cron scheduler – runs market data fetch every day at 03:30
	// -----------------------------------------------------------------------
	c := cron.New()
	if _, err := c.AddJob("30 3 * * *", job.NewMarketDataJob(stockClient)); err != nil {
		log.Fatalf("failed to register market data cron job: %v", err)
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
