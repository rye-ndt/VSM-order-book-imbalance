package input

import "context"

// OrderBookLevel is a single price/volume entry at one depth level.
type OrderBookLevel struct {
	Price  float64
	Volume float64
}

// OrderBook is a 10-level bid/ask snapshot delivered by the SSI
// FastConnectData IDS streaming service (Quote message, RType "Quote").
// Fields map directly to the spec's BidPrice1-10 / BidVol1-10 and
// AskPrice1-10 / AskVol1-10 elements; index 0 is the best (tightest) level.
type OrderBook struct {
	Symbol      string
	Exchange    string
	TradingDate string // DD/MM/YYYY
	TradingTime string // HH:MM:SS
	Bids        [10]OrderBookLevel
	Asks        [10]OrderBookLevel
}

// SSIFastConnect is the input port for subscribing to real-time order book
// data from the SSI FastConnectData IDS streaming service.
// Adapters (e.g. a WebSocket client) should implement this interface in
// internal/modules and authenticate using the Bearer token obtained from
// POST https://fc-data.ssi.com.vn/api/v2/Market/AccessToken.
type SSIFastConnect interface {
	// Subscribe starts an IDS subscription for the given symbols and delivers
	// inbound order book snapshots on the returned channel. Symbols follow the
	// SSI convention (e.g. "SSI", "VN30F2104"); pass a single "ALL" entry to
	// subscribe to all available symbols. The returned channel is closed when
	// ctx is cancelled or the underlying connection is lost.
	Subscribe(ctx context.Context, symbols []string) (<-chan OrderBook, error)
}
