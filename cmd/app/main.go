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
	"github.com/example/order-book-imbalance/internal/interface/input"
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

	dbAdapter := modules.NewPostgresRelationalDB(cfg.DB)
	db, err := dbAdapter.Connect(context.Background())
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer db.Close()

	stockClient := modules.NewSSIStockClient(cfg.SSI)
	store := modules.NewPostgresMarketStore(db)

	var obClient input.OrderBookClient
	if cfg.SignalMode != "swing" {
		obClient = modules.NewSSIOrderBookClient(cfg.SSI)
	}

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

	var aiClient output.AI
	if cfg.OpenAI.APIKey != "" {
		aiClient, err = modules.NewOpenAIClient(cfg.OpenAI)
		if err != nil {
			log.Printf("openai client disabled: %v", err)
		} else {
			log.Printf("AI signal interpretation enabled (model: %s)", cfg.OpenAI.Model)
		}
	}

	if err := store.Migrate(context.Background()); err != nil {
		log.Fatalf("migrate market tables: %v", err)
	}

	ict, err := time.LoadLocation(cfg.ATO.Timezone)
	if err != nil {
		log.Fatalf("load timezone %s: %v", cfg.ATO.Timezone, err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer pingCancel()
	if err := stockClient.Ping(pingCtx); err != nil {
		log.Printf("[startup] SSI REST API ping failed: %v", err)
	} else {
		log.Printf("[startup] SSI REST API: OK")
	}
	if cfg.SignalMode != "swing" {
		if err := obClient.Ping(pingCtx); err != nil {
			log.Printf("[startup] SSI IDS ping failed: %v", err)
		} else {
			log.Printf("[startup] SSI IDS: OK")
		}
	}

	marketDataJob := job.NewMarketDataJob(stockClient, store, cfg.Signal, ict)

	today := time.Now().In(ict)
	crawled, err := store.IsTodayMarketDataCrawled(context.Background(), today)
	if err != nil {
		log.Printf("[startup] check market data crawl status: %v", err)
	} else if !crawled {
		log.Printf("[startup] market data not crawled today, running pipeline now")
		marketDataJob.Run()
	}

	c := cron.New(cron.WithLocation(ict))

	if _, err := c.AddJob(cfg.Cron.MarketData, marketDataJob); err != nil {
		log.Fatalf("register market data cron job: %v", err)
	}

	if cfg.SignalMode != "swing" {
		atoMonitorJob := job.NewATOMonitorJob(store, obClient, notifier, socialPoster, aiClient, cfg.Signal, cfg.ATO)

		monitored, err := store.IsTodayATOMonitored(context.Background(), today)
		if err != nil {
			log.Printf("[startup] check ATO monitor status: %v", err)
		} else if !monitored {
			log.Printf("[startup] ATO session not monitored today, running now")
			go atoMonitorJob.Run()
		}

		if monitored && aiClient != nil && notifier != nil {
			summarySent, err := store.IsSessionSummarySent(context.Background(), today)
			if err != nil {
				log.Printf("[startup] check session summary sent: %v", err)
			} else if !summarySent {
				log.Printf("[startup] session summary not yet sent — sending from DB")
				go atoMonitorJob.SendSessionSummaryFromDB(context.Background(), today)
			}
		}

		if _, err := c.AddJob(cfg.Cron.ATOMonitor, atoMonitorJob); err != nil {
			log.Fatalf("register ATO monitor cron job: %v", err)
		}
	}

	if cfg.SignalMode != "ato" {
		swingSignalJob := job.NewSwingSignalJob(store, notifier, aiClient, cfg.Signal, cfg.Swing, ict)

		sent, err := store.IsSwingSignalSent(context.Background(), today)
		if err != nil {
			log.Printf("[startup] check swing signal sent: %v", err)
		} else if !sent {
			log.Printf("[startup] swing signal not yet sent today, running now")
			go swingSignalJob.Run()
		}

		if _, err := c.AddJob(cfg.Swing.Cron, swingSignalJob); err != nil {
			log.Fatalf("register swing signal cron job: %v", err)
		}
	}

	c.Start()
	defer c.Stop()

	srv := server.NewHTTPServer(cfg)
	log.Printf("starting HTTP server on %s", cfg.HTTPListenAddr)
	if err := http.ListenAndServe(cfg.HTTPListenAddr, srv.Router()); err != nil {
		log.Fatalf("server exited: %v", err)
	}
}
