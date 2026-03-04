# Order Book Imbalance

Minimal Go boilerplate for an HTTP service to compute order book imbalance.

## Requirements

- Go 1.22+

## Getting started

Install dependencies (will populate `go.sum`):

```bash
go mod tidy
```

Run the app:

```bash
make run
```

The server listens on `:8080` with:

- `GET /healthz` – health check
- `GET /imbalance` – placeholder endpoint for order book imbalance logic

## Project layout

- `cmd/app` – main application entrypoint
- `internal/server` – HTTP server wiring and handlers

# stock-trading
