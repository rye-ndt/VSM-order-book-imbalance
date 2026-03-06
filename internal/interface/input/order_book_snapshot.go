package input

import "time"

// PriceLevel represents one depth level in an order book.
type PriceLevel struct {
	Price  float64
	Volume float64
	// TODO: OrderCount (number of separate orders at this price level) is not
	// provided by the SSI IDS Quote message (spec section 3.2). Add a fetcher
	// for this field when a data source that exposes per-level order count is
	// identified.
	OrderCount int
}

// OrderBookSnapshot is a point-in-time capture of a symbol's order book,
// enriched with the latest price metadata from Trade messages.
// Empty price levels (Price == 0) are excluded from BidLevels and AskLevels.
// Fields sourced from Trade messages are 0 until the first Trade arrives.
type OrderBookSnapshot struct {
	Symbol         string
	CapturedAt     time.Time
	BidLevels      []PriceLevel // bid depth, best price first
	AskLevels      []PriceLevel // ask depth, best price first
	IndicatedPrice float64      // EstMatchedPrice — estimated ATO clearing price
	CeilingPrice   float64      // Ceiling (tran) price for the day
	RefPrice       float64      // reference price (prior day close)
}
