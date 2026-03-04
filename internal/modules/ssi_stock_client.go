package modules

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
)

const (
	pathAccessToken     = "/api/v2/Market/AccessToken"
	pathDailyOHLC       = "/api/v2/Market/DailyOhlc"
	pathDailyStockPrice = "/api/v2/Market/DailyStockPrice"

	// ssiDateFormat matches the DD/MM/YYYY format used by all SSI endpoints.
	ssiDateFormat = "02/01/2006"

	// ssiPageSize is the maximum records per page supported by the SSI API.
	ssiPageSize = 1000

	// tokenTTL is how long we reuse a cached access token before refreshing.
	// The SSI spec does not publish an expiry; 23 h is conservative.
	tokenTTL = 23 * time.Hour
)

// SSIStockClient implements input.StockDataClient using the SSI
// FastConnectData REST API v2.  Credentials are read from config.SSIConfig.
type SSIStockClient struct {
	cfg         config.SSIConfig
	httpClient  *http.Client
	mu          sync.Mutex
	cachedToken string
	tokenExpiry time.Time
}

// NewSSIStockClient constructs a ready-to-use SSIStockClient.
func NewSSIStockClient(cfg config.SSIConfig) input.StockDataClient {
	return &SSIStockClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// -------------------------------------------------------------------------
// Auth
// -------------------------------------------------------------------------

type ssiAuthRequest struct {
	ConsumerID     string `json:"consumerID"`
	ConsumerSecret string `json:"consumerSecret"`
}

type ssiAuthResponse struct {
	ResponseCode int    `json:"responseCode"`
	Message      string `json:"message"`
	Token        string `json:"token"`
}

// bearerToken returns a valid access token, fetching a new one when the cache
// has expired. It is safe for concurrent use.
func (c *SSIStockClient) bearerToken(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Now().Before(c.tokenExpiry) {
		return c.cachedToken, nil
	}

	body, _ := json.Marshal(ssiAuthRequest{
		ConsumerID:     c.cfg.ConsumerID,
		ConsumerSecret: c.cfg.ConsumerSecret,
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.BaseURL+pathAccessToken, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("ssi auth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ssi auth: %w", err)
	}
	defer resp.Body.Close()

	var ar ssiAuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return "", fmt.Errorf("ssi auth: decode response: %w", err)
	}
	if ar.ResponseCode != 0 {
		return "", fmt.Errorf("ssi auth: %s", ar.Message)
	}

	c.cachedToken = ar.Token
	c.tokenExpiry = time.Now().Add(tokenTTL)
	return c.cachedToken, nil
}

// -------------------------------------------------------------------------
// Generic paginated GET helper
// -------------------------------------------------------------------------

type ssiListResponse struct {
	DataList    json.RawMessage `json:"dataList"`
	Message     string          `json:"message"`
	Status      string          `json:"status"`
	TotalRecord int             `json:"totalrecord"`
}

func (c *SSIStockClient) getList(ctx context.Context, path string, params url.Values) (ssiListResponse, error) {
	token, err := c.bearerToken(ctx)
	if err != nil {
		return ssiListResponse{}, err
	}

	u, err := url.Parse(c.cfg.BaseURL + path)
	if err != nil {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: parse url: %w", path, err)
	}
	u.RawQuery = params.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: build request: %w", path, err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: %w", path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: read body: %w", path, err)
	}

	var result ssiListResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: decode: %w", path, err)
	}
	if result.Status != "SUCCESS" && result.Status != "Success" {
		return ssiListResponse{}, fmt.Errorf("ssi get %s: %s", path, result.Message)
	}
	return result, nil
}

// -------------------------------------------------------------------------
// Daily OHLC  (Section 4.6 – GET /api/v2/Market/DailyOhlc)
// -------------------------------------------------------------------------

// ssiOHLCRecord mirrors the JSON object inside the "dataList" array returned
// by the DailyOhlc endpoint.  All numeric fields arrive as strings.
type ssiOHLCRecord struct {
	Symbol      string `json:"symbol"`
	Market      string `json:"market"`
	TradingDate string `json:"tradingdate"`
	Open        string `json:"open"`
	High        string `json:"high"`
	Low         string `json:"low"`
	Close       string `json:"close"`
	Volume      string `json:"volume"`
	Value       string `json:"value"`
}

func (c *SSIStockClient) fetchOHLCPage(
	ctx context.Context,
	symbol, fromDate, toDate string,
	page int,
) ([]ssiOHLCRecord, int, error) {
	params := url.Values{
		"FromDate":  {fromDate},
		"ToDate":    {toDate},
		"PageIndex": {strconv.Itoa(page)},
		"PageSize":  {strconv.Itoa(ssiPageSize)},
		"ascending": {"true"},
	}
	if symbol != "" {
		params.Set("Symbol", symbol)
	}

	res, err := c.getList(ctx, pathDailyOHLC, params)
	if err != nil {
		return nil, 0, err
	}

	if len(res.DataList) == 0 || string(res.DataList) == "null" {
		return nil, 0, nil
	}

	var records []ssiOHLCRecord
	if err := json.Unmarshal(res.DataList, &records); err != nil {
		return nil, 0, fmt.Errorf("ssi ohlc: decode records: %w", err)
	}
	return records, res.TotalRecord, nil
}

func (c *SSIStockClient) fetchAllOHLC(ctx context.Context, symbol, fromDate, toDate string) ([]input.OHLCV, error) {
	var all []input.OHLCV
	for page := 1; ; page++ {
		records, total, err := c.fetchOHLCPage(ctx, symbol, fromDate, toDate, page)
		if err != nil {
			return nil, err
		}
		for _, r := range records {
			all = append(all, input.OHLCV{
				Symbol:      r.Symbol,
				Market:      r.Market,
				TradingDate: r.TradingDate,
				Open:        parseFloat(r.Open),
				High:        parseFloat(r.High),
				Low:         parseFloat(r.Low),
				Close:       parseFloat(r.Close),
				Volume:      parseFloat(r.Volume),
				Value:       parseFloat(r.Value),
			})
		}
		if len(all) >= total || len(records) < ssiPageSize {
			break
		}
	}
	return all, nil
}

// FetchAllStocksOHLCV returns daily OHLCV for all listed equities over the
// most recent 20 calendar days (DailyOhlc, no symbol filter).
func (c *SSIStockClient) FetchAllStocksOHLCV(ctx context.Context) ([]input.OHLCV, error) {
	now := time.Now()
	return c.fetchAllOHLC(ctx, "",
		now.AddDate(0, 0, -20).Format(ssiDateFormat),
		now.Format(ssiDateFormat),
	)
}

// FetchVNIndexOHLCV returns daily OHLCV for the VN Index over at least the
// given number of calendar days ending today (DailyOhlc, symbol="VNINDEX").
func (c *SSIStockClient) FetchVNIndexOHLCV(ctx context.Context, days int) ([]input.OHLCV, error) {
	now := time.Now()
	return c.fetchAllOHLC(ctx, "VNINDEX",
		now.AddDate(0, 0, -days).Format(ssiDateFormat),
		now.Format(ssiDateFormat),
	)
}

// -------------------------------------------------------------------------
// Daily Stock Price  (Section 4.9 – GET /api/v2/Market/DailyStockPrice)
// -------------------------------------------------------------------------

// ssiStockPriceRecord mirrors the subset of fields from DailyStockPrice that
// are needed to compute foreign flow.  All numeric fields arrive as strings.
type ssiStockPriceRecord struct {
	Symbol              string `json:"symbol"`
	TradingDate         string `json:"tradingdate"`
	ForeignBuyVolTotal  string `json:"foreignbuyvoltotal"`
	ForeignSellVolTotal string `json:"foreignsellvoltotal"`
	ForeignBuyValTotal  string `json:"foreignbuyvaltotal"`
	ForeignSellValTotal string `json:"foreignsellvaltotal"`
}

func (c *SSIStockClient) fetchStockPricePage(
	ctx context.Context,
	fromDate, toDate string,
	page int,
) ([]ssiStockPriceRecord, int, error) {
	params := url.Values{
		"FromDate":  {fromDate},
		"ToDate":    {toDate},
		"PageIndex": {strconv.Itoa(page)},
		"PageSize":  {strconv.Itoa(ssiPageSize)},
	}

	res, err := c.getList(ctx, pathDailyStockPrice, params)
	if err != nil {
		return nil, 0, err
	}

	if len(res.DataList) == 0 || string(res.DataList) == "null" {
		return nil, 0, nil
	}

	var records []ssiStockPriceRecord
	if err := json.Unmarshal(res.DataList, &records); err != nil {
		return nil, 0, fmt.Errorf("ssi stock price: decode records: %w", err)
	}
	return records, res.TotalRecord, nil
}

// FetchForeignFlow returns net foreign buy/sell activity for every equity on
// the previous calendar day (DailyStockPrice, no symbol filter).
func (c *SSIStockClient) FetchForeignFlow(ctx context.Context) ([]input.ForeignFlow, error) {
	yesterday := time.Now().AddDate(0, 0, -1).Format(ssiDateFormat)

	var all []input.ForeignFlow
	for page := 1; ; page++ {
		records, total, err := c.fetchStockPricePage(ctx, yesterday, yesterday, page)
		if err != nil {
			return nil, err
		}
		for _, r := range records {
			buyVol := parseFloat(r.ForeignBuyVolTotal)
			sellVol := parseFloat(r.ForeignSellVolTotal)
			buyVal := parseFloat(r.ForeignBuyValTotal)
			sellVal := parseFloat(r.ForeignSellValTotal)
			all = append(all, input.ForeignFlow{
				Symbol:      r.Symbol,
				TradingDate: r.TradingDate,
				BuyVolume:   buyVol,
				SellVolume:  sellVol,
				BuyValue:    buyVal,
				SellValue:   sellVal,
				NetVolume:   buyVol - sellVol,
				NetValue:    buyVal - sellVal,
			})
		}
		if len(all) >= total || len(records) < ssiPageSize {
			break
		}
	}
	return all, nil
}

// -------------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------------

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
