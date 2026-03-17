package input

import (
	"context"
	"errors"
)

// ErrNoSnapshot is returned by FetchOrderBook when no order book data has
// been received yet for the requested symbol.
var ErrNoSnapshot = errors.New("order book: no snapshot available yet")

// OrderBookClient is the input port for real-time order book snapshots.
//
// The interface is designed for a polling caller: the caller drives the
// poll interval (e.g. every 2 s) and calls FetchOrderBook on demand.
// Implementations connect to a streaming source (e.g. the SSI IDS WebSocket)
// once via Subscribe, cache the latest snapshot per symbol, and return that
// cached value on each FetchOrderBook call.
type OrderBookClient interface {
	// Ping verifies that the SSI IDS endpoint is reachable and a SignalR
	// connection token can be obtained.
	Ping(ctx context.Context) error

	// Subscribe connects to the data source and begins receiving order book
	// updates for the given symbols. The provided context governs the lifetime
	// of the underlying connection; cancel it to tear down the connection
	// cleanly. Must be called before FetchOrderBook.
	Subscribe(ctx context.Context, symbols []string) error

	// FetchOrderBook returns the most recent order book snapshot for symbol,
	// with IndicatedPrice merged from the latest Trade message.
	// Returns ErrNoSnapshot if no Quote data has been received yet.
	// This call is non-blocking and safe for concurrent use.
	FetchOrderBook(symbol string) (OrderBookSnapshot, error)
}
