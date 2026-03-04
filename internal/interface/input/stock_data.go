package input

import (
	"context"
	"time"
)

// OHLCV holds one trading day of Open/High/Low/Close/Volume data for a security
// or index. Values are in the native currency unit returned by SSI (VND).
type OHLCV struct {
	Symbol      string
	Market      string
	TradingDate string // DD/MM/YYYY
	Open        float64
	High        float64
	Low         float64
	Close       float64
	Volume      float64 // matched volume
	Value       float64 // matched value (VND)
}

// ForeignFlow captures net foreign buy/sell activity for one security on a
// single trading day, derived from the SSI DailyStockPrice endpoint.
type ForeignFlow struct {
	Symbol      string
	TradingDate string  // DD/MM/YYYY
	BuyVolume   float64 // foreignbuyvoltotal
	SellVolume  float64 // foreignsellvoltotal
	BuyValue    float64 // foreignbuyvaltotal  (VND)
	SellValue   float64 // foreignsellvaltotal (VND)
	NetVolume   float64 // BuyVolume - SellVolume
	NetValue    float64 // BuyValue  - SellValue  (VND)
}

// StockDataClient is the input port for fetching historical market data from
// the SSI FastConnectData REST API (v2). Adapters implementing this interface
// live in internal/modules and authenticate via Bearer token obtained from
// POST https://fc-data.ssi.com.vn/api/v2/Market/AccessToken.
//
// All methods accept an explicit [from, to] date window so callers (e.g. the
// cron job) can request only the days that are actually missing from the store,
// avoiding redundant API calls for already-persisted data.
type StockDataClient interface {
	// FetchAllStocksOHLCV returns daily OHLCV for every listed equity in the
	// [from, to] date range, paginating through all results from DailyOhlc.
	FetchAllStocksOHLCV(ctx context.Context, from, to time.Time) ([]OHLCV, error)

	// FetchForeignFlow returns net foreign buy/sell volume and value for every
	// equity in the [from, to] date range, using the DailyStockPrice endpoint.
	FetchForeignFlow(ctx context.Context, from, to time.Time) ([]ForeignFlow, error)

	// FetchVNIndexOHLCV returns daily OHLCV for the VN Index (symbol "VNINDEX")
	// in the [from, to] date range, using the DailyOhlc endpoint.
	FetchVNIndexOHLCV(ctx context.Context, from, to time.Time) ([]OHLCV, error)
}
