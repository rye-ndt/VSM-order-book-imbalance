package input

import "context"

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
type StockDataClient interface {
	// FetchAllStocksOHLCV returns daily OHLCV for every listed equity over
	// the most recent 20 calendar days, paginating through all available
	// results from the DailyOhlc endpoint.
	FetchAllStocksOHLCV(ctx context.Context) ([]OHLCV, error)

	// FetchForeignFlow returns net foreign buy/sell volume and value for
	// every equity on the most recent completed trading day, using data
	// from the DailyStockPrice endpoint.
	FetchForeignFlow(ctx context.Context) ([]ForeignFlow, error)

	// FetchVNIndexOHLCV returns daily OHLCV for the VN Index (symbol
	// "VNINDEX") over at least the given number of calendar days ending
	// today, using the DailyOhlc endpoint.
	FetchVNIndexOHLCV(ctx context.Context, days int) ([]OHLCV, error)
}
