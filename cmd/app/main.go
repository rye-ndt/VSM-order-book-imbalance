package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/server"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to YAML config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	srv := server.NewHTTPServer(cfg)

	log.Printf("starting HTTP server on %s\n", cfg.HTTPListenAddr)
	if err := http.ListenAndServe(cfg.HTTPListenAddr, srv.Router()); err != nil {
		log.Fatalf("server exited with error: %v", err)
	}
}

