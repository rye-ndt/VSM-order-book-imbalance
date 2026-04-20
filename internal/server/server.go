package server

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/example/order-book-imbalance/internal/config"
)

// HTTPServer holds HTTP-related dependencies.
type HTTPServer struct {
	mux    *http.ServeMux
	config *config.Config
}

// NewHTTPServer constructs a new HTTPServer with routes configured.
func NewHTTPServer(cfg *config.Config) *HTTPServer {
	mux := http.NewServeMux()

	s := &HTTPServer{
		mux:    mux,
		config: cfg,
	}

	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/imbalance", s.handleImbalance)

	return s
}

// Router exposes the underlying http.Handler for use in http servers.
func (s *HTTPServer) Router() http.Handler {
	return s.mux
}

func (s *HTTPServer) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleImbalance is a placeholder endpoint for future order book logic.
func (s *HTTPServer) handleImbalance(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"message": "order book imbalance endpoint"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("write json response: %v", err)
	}
}
