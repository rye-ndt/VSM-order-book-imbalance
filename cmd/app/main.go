package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/robfig/cron/v3"
	// pgx stdlib adapter registers the "pgx" driver name with database/sql.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
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
	obClient := modules.NewSSIOrderBookClient(cfg.SSI)

	tgBot, err := modules.NewTelegramBot(cfg.Telegram, store)
	if err != nil {
		log.Printf("telegram bot disabled: %v", err)
	}

	var notifier output.Notifier
	if tgBot != nil {
		notifier = tgBot
		go tgBot.Run(context.Background())
	}

	var socialPoster output.SocialPoster
	xPoster, err := modules.NewXPoster(cfg.Twitter)
	if err != nil {
		log.Printf("x (twitter) poster disabled: %v", err)
	} else {
		socialPoster = xPoster
	}
	_ = socialPoster // wire to jobs/handlers as needed

	// Run schema migration once at startup before the first job execution.
	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("migrate market tables: %v", err)
	}

	// -----------------------------------------------------------------------
	// Cron scheduler – all times are Vietnam local time (ICT, UTC+7)
	// -----------------------------------------------------------------------
	ict, err := time.LoadLocation("Asia/Ho_Chi_Minh")
	if err != nil {
		log.Fatalf("load Asia/Ho_Chi_Minh timezone: %v", err)
	}

	c := cron.New(cron.WithLocation(ict))

	// 03:30 ICT – fetch previous day's market data and compute stock metrics.
	if _, err := c.AddJob("30 3 * * *", job.NewMarketDataJob(stockClient, store, cfg.Signal)); err != nil {
		log.Fatalf("register market data cron job: %v", err)
	}

	// 09:00 ICT – monitor ATO order book for stocks flagged overnight.
	// The job self-terminates at 09:15 ICT via an internal context deadline.
	if _, err := c.AddJob("0 9 * * *", job.NewATOMonitorJob(store, obClient, notifier, cfg.Signal)); err != nil {
		log.Fatalf("register ATO monitor cron job: %v", err)
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
