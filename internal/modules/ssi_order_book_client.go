package modules

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
)

const (
	// ssiIDSMarketDataChannel is the subscription prefix for market data
	// (spec section 3.2). One X subscription delivers both Trade and Quote
	// messages; DataType in the envelope tells them apart.
	ssiIDSMarketDataChannel = "X"

	ssiIDSDataTypeQuote = "Quote"
	ssiIDSDataTypeTrade = "Trade"
)

// ssiIDSEnvelope is the outer wrapper of every IDS message.
// DataType identifies the message kind; Content is a JSON-encoded string
// that must be decoded a second time to obtain the actual payload.
// Go's json decoder matches field names case-insensitively, so this struct
// handles both "DataType" (Quote) and "datatype" (Trade) from the spec.
type ssiIDSEnvelope struct {
	DataType string `json:"DataType"`
	Content  string `json:"Content"`
}

// ssiIDSSubRequest is sent to the IDS server to subscribe to a data channel.
// Params follows the SSI convention: "<channel>:<sym1>,<sym2>".
// The exact wire format (JSON vs plain text) must be confirmed against the spec.
type ssiIDSSubRequest struct {
	Action string `json:"action"` // "sub"
	Params string `json:"params"`
}

// ssiIDSQuote is the payload inside a Quote envelope (spec section 3.2).
// Prices arrive as float64; volumes arrive as quoted strings.
type ssiIDSQuote struct {
	Symbol      string `json:"Symbol"`
	Exchange    string `json:"Exchange"`
	TradingDate string `json:"TradingDate"` // DD/MM/YYYY
	TradingTime string `json:"TradingTime"` // HH:MM:SS

	BidPrice1  float64 `json:"BidPrice1"`
	BidVol1    string  `json:"BidVol1"`
	BidPrice2  float64 `json:"BidPrice2"`
	BidVol2    string  `json:"BidVol2"`
	BidPrice3  float64 `json:"BidPrice3"`
	BidVol3    string  `json:"BidVol3"`
	BidPrice4  float64 `json:"BidPrice4"`
	BidVol4    string  `json:"BidVol4"`
	BidPrice5  float64 `json:"BidPrice5"`
	BidVol5    string  `json:"BidVol5"`
	BidPrice6  float64 `json:"BidPrice6"`
	BidVol6    string  `json:"BidVol6"`
	BidPrice7  float64 `json:"BidPrice7"`
	BidVol7    string  `json:"BidVol7"`
	BidPrice8  float64 `json:"BidPrice8"`
	BidVol8    string  `json:"BidVol8"`
	BidPrice9  float64 `json:"BidPrice9"`
	BidVol9    string  `json:"BidVol9"`
	BidPrice10 float64 `json:"BidPrice10"`
	BidVol10   string  `json:"BidVol10"`

	AskPrice1  float64 `json:"AskPrice1"`
	AskVol1    string  `json:"AskVol1"`
	AskPrice2  float64 `json:"AskPrice2"`
	AskVol2    string  `json:"AskVol2"`
	AskPrice3  float64 `json:"AskPrice3"`
	AskVol3    string  `json:"AskVol3"`
	AskPrice4  float64 `json:"AskPrice4"`
	AskVol4    string  `json:"AskVol4"`
	AskPrice5  float64 `json:"AskPrice5"`
	AskVol5    string  `json:"AskVol5"`
	AskPrice6  float64 `json:"AskPrice6"`
	AskVol6    string  `json:"AskVol6"`
	AskPrice7  float64 `json:"AskPrice7"`
	AskVol7    string  `json:"AskVol7"`
	AskPrice8  float64 `json:"AskPrice8"`
	AskVol8    string  `json:"AskVol8"`
	AskPrice9  float64 `json:"AskPrice9"`
	AskVol9    string  `json:"AskVol9"`
	AskPrice10 float64 `json:"AskPrice10"`
	AskVol10   string  `json:"AskVol10"`
}

// ssiIDSTrade carries the price metadata fields from a Trade envelope
// (spec section 3.2). All three fields are Number type in the spec.
type ssiIDSTrade struct {
	Symbol          string  `json:"Symbol"`
	Ceiling         float64 `json:"Ceiling"`
	RefPrice        float64 `json:"RefPrice"`
	EstMatchedPrice float64 `json:"EstMatchedPrice"`
}

// SSIOrderBookClient implements input.OrderBookClient via the SSI IDS WebSocket.
// It subscribes to the X channel, tracks EstMatchedPrice from Trade messages,
// and merges it into each Quote snapshot stored in the per-symbol cache.
type SSIOrderBookClient struct {
	cfg        config.SSIConfig
	httpClient *http.Client

	// token cache — independent from SSIStockClient
	mu          sync.Mutex
	cachedToken string
	tokenExpiry time.Time

	// per-symbol price metadata from Trade messages (all guarded by priceMu)
	priceMu             sync.RWMutex
	lastEstMatchedPrice map[string]float64
	lastCeilingPrice    map[string]float64
	lastRefPrice        map[string]float64

	// per-symbol latest order book snapshot (Quote + merged IndicatedPrice)
	snapMu sync.RWMutex
	cache  map[string]input.OrderBookSnapshot
}

func NewSSIOrderBookClient(cfg config.SSIConfig) input.OrderBookClient {
	return &SSIOrderBookClient{
		cfg:                 cfg,
		httpClient:          &http.Client{Timeout: 30 * time.Second},
		lastEstMatchedPrice: make(map[string]float64),
		lastCeilingPrice:    make(map[string]float64),
		lastRefPrice:        make(map[string]float64),
		cache:               make(map[string]input.OrderBookSnapshot),
	}
}

// -------------------------------------------------------------------------
// Auth (same token endpoint as SSIStockClient, independent cache)
// -------------------------------------------------------------------------

func (c *SSIOrderBookClient) bearerToken(ctx context.Context) (string, error) {
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
		return "", fmt.Errorf("ssi ob auth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ssi ob auth: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var ar ssiAuthResponse
	if err := json.Unmarshal(raw, &ar); err != nil {
		return "", fmt.Errorf("ssi ob auth: decode: %w", err)
	}
	if ar.Status != 200 || ar.Data.AccessToken == "" {
		return "", fmt.Errorf("ssi ob auth: %s", ar.Message)
	}

	c.cachedToken = ar.Data.AccessToken
	c.tokenExpiry = time.Now().Add(tokenTTL)
	return c.cachedToken, nil
}

func (c *SSIOrderBookClient) Ping(ctx context.Context) error {
	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}
	_, err = c.signalRNegotiate(ctx, token)
	return err
}

// -------------------------------------------------------------------------
// Subscribe
// -------------------------------------------------------------------------

type ssiSignalRNegotiateResponse struct {
	ConnectionToken string `json:"ConnectionToken"`
}

func (c *SSIOrderBookClient) signalRNegotiate(ctx context.Context, token string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.cfg.StreamURL+"/negotiate", nil)
	if err != nil {
		return "", fmt.Errorf("ssi ob negotiate: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("ssi ob negotiate: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var nr ssiSignalRNegotiateResponse
	if err := json.Unmarshal(raw, &nr); err != nil {
		return "", fmt.Errorf("ssi ob negotiate: decode: %w", err)
	}
	if nr.ConnectionToken == "" {
		return "", fmt.Errorf("ssi ob negotiate: empty connection token (body: %s)", string(raw))
	}
	return nr.ConnectionToken, nil
}

func (c *SSIOrderBookClient) Subscribe(ctx context.Context, symbols []string) error {
	if c.cfg.StreamURL == "" {
		return fmt.Errorf("ssi ob: stream_url is not configured")
	}

	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}

	connToken, err := c.signalRNegotiate(ctx, token)
	if err != nil {
		return err
	}

	encodedToken := url.QueryEscape(connToken)

	// stream_url is https://...; replace scheme for WebSocket dial.
	wsBase := strings.NewReplacer("https://", "wss://", "http://", "ws://").Replace(c.cfg.StreamURL)
	wsURL := wsBase + "/connect?transport=webSockets&clientProtocol=1.2&connectionToken=" + encodedToken

	hdr := http.Header{"Authorization": {"Bearer " + token}}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, hdr)
	if err != nil {
		return fmt.Errorf("ssi ob: websocket dial: %w", err)
	}

	startReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.cfg.StreamURL+"/start?transport=webSockets&clientProtocol=1.2&connectionToken="+encodedToken, nil)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ssi ob: build /start request: %w", err)
	}
	startReq.Header.Set("Authorization", "Bearer "+token)
	startResp, err := c.httpClient.Do(startReq)
	if err != nil {
		conn.Close()
		return fmt.Errorf("ssi ob: /start: %w", err)
	}
	startResp.Body.Close()
	if startResp.StatusCode != 200 {
		conn.Close()
		return fmt.Errorf("ssi ob: /start returned HTTP %d", startResp.StatusCode)
	}

	subMsg := ssiIDSSubRequest{
		Action: "sub",
		Params: ssiIDSMarketDataChannel + ":" + strings.Join(symbols, ","),
	}
	if err := conn.WriteJSON(subMsg); err != nil {
		conn.Close()
		return fmt.Errorf("ssi ob: send subscribe: %w", err)
	}

	log.Printf("[ssi-ob] subscribed to %d symbols", len(symbols))

	go c.readLoop(ctx, conn)

	return nil
}

func (c *SSIOrderBookClient) readLoop(ctx context.Context, conn *websocket.Conn) {
	defer conn.Close()

	go func() {
		<-ctx.Done()
		_ = conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "session ended"),
		)
	}()

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[ssi-ob] read error: %v", err)
			return
		}

		var env ssiIDSEnvelope
		if err := json.Unmarshal(raw, &env); err != nil {
			log.Printf("[ssi-ob] parse envelope: %v", err)
			continue
		}

		switch env.DataType {
		case ssiIDSDataTypeTrade:
			c.handleTrade(env.Content)
		case ssiIDSDataTypeQuote:
			c.handleQuote(env.Content)
		}
	}
}

func (c *SSIOrderBookClient) handleTrade(content string) {
	var t ssiIDSTrade
	if err := json.Unmarshal([]byte(content), &t); err != nil {
		log.Printf("[ssi-ob] parse trade: %v", err)
		return
	}
	if t.Symbol == "" {
		return
	}
	c.priceMu.Lock()
	prev, seen := c.lastEstMatchedPrice[t.Symbol]
	changed := !seen || prev != t.EstMatchedPrice
	c.lastEstMatchedPrice[t.Symbol] = t.EstMatchedPrice
	c.lastCeilingPrice[t.Symbol] = t.Ceiling
	c.lastRefPrice[t.Symbol] = t.RefPrice
	c.priceMu.Unlock()
	if changed {
		log.Printf("[ssi-ob] trade %s  est=%.0f  ceil=%.0f  ref=%.0f",
			t.Symbol, t.EstMatchedPrice, t.Ceiling, t.RefPrice)
	}
}

func (c *SSIOrderBookClient) handleQuote(content string) {
	var q ssiIDSQuote
	if err := json.Unmarshal([]byte(content), &q); err != nil {
		log.Printf("[ssi-ob] parse quote: %v", err)
		return
	}
	if q.Symbol == "" {
		return
	}

	c.priceMu.RLock()
	indicatedPrice := c.lastEstMatchedPrice[q.Symbol]
	ceilingPrice := c.lastCeilingPrice[q.Symbol]
	refPrice := c.lastRefPrice[q.Symbol]
	c.priceMu.RUnlock()

	snap := quoteToSnapshot(q, indicatedPrice, ceilingPrice, refPrice)

	c.snapMu.Lock()
	c.cache[q.Symbol] = snap
	c.snapMu.Unlock()
}

// -------------------------------------------------------------------------
// FetchOrderBook
// -------------------------------------------------------------------------

func (c *SSIOrderBookClient) FetchOrderBook(symbol string) (input.OrderBookSnapshot, error) {
	c.snapMu.RLock()
	snap, ok := c.cache[symbol]
	c.snapMu.RUnlock()
	if !ok {
		return input.OrderBookSnapshot{}, input.ErrNoSnapshot
	}
	return snap, nil
}

// -------------------------------------------------------------------------
// Conversion
// -------------------------------------------------------------------------

func quoteToSnapshot(q ssiIDSQuote, indicatedPrice, ceilingPrice, refPrice float64) input.OrderBookSnapshot {
	rawBids := [10][2]float64{
		{q.BidPrice1, parseIDSVol(q.BidVol1)},
		{q.BidPrice2, parseIDSVol(q.BidVol2)},
		{q.BidPrice3, parseIDSVol(q.BidVol3)},
		{q.BidPrice4, parseIDSVol(q.BidVol4)},
		{q.BidPrice5, parseIDSVol(q.BidVol5)},
		{q.BidPrice6, parseIDSVol(q.BidVol6)},
		{q.BidPrice7, parseIDSVol(q.BidVol7)},
		{q.BidPrice8, parseIDSVol(q.BidVol8)},
		{q.BidPrice9, parseIDSVol(q.BidVol9)},
		{q.BidPrice10, parseIDSVol(q.BidVol10)},
	}
	rawAsks := [10][2]float64{
		{q.AskPrice1, parseIDSVol(q.AskVol1)},
		{q.AskPrice2, parseIDSVol(q.AskVol2)},
		{q.AskPrice3, parseIDSVol(q.AskVol3)},
		{q.AskPrice4, parseIDSVol(q.AskVol4)},
		{q.AskPrice5, parseIDSVol(q.AskVol5)},
		{q.AskPrice6, parseIDSVol(q.AskVol6)},
		{q.AskPrice7, parseIDSVol(q.AskVol7)},
		{q.AskPrice8, parseIDSVol(q.AskVol8)},
		{q.AskPrice9, parseIDSVol(q.AskVol9)},
		{q.AskPrice10, parseIDSVol(q.AskVol10)},
	}

	bids := make([]input.PriceLevel, 0, 10)
	for _, b := range rawBids {
		if b[0] > 0 {
			bids = append(bids, input.PriceLevel{Price: b[0], Volume: b[1]})
		}
	}
	asks := make([]input.PriceLevel, 0, 10)
	for _, a := range rawAsks {
		if a[0] > 0 {
			asks = append(asks, input.PriceLevel{Price: a[0], Volume: a[1]})
		}
	}

	return input.OrderBookSnapshot{
		Symbol:         q.Symbol,
		CapturedAt:     time.Now(),
		BidLevels:      bids,
		AskLevels:      asks,
		IndicatedPrice: indicatedPrice,
		CeilingPrice:   ceilingPrice,
		RefPrice:       refPrice,
	}
}

// parseIDSVol parses a volume string from the IDS Quote payload.
// Volumes arrive as quoted strings (e.g. "150") per spec section 3.2.
func parseIDSVol(s string) float64 {
	v, _ := strconv.ParseFloat(strings.ReplaceAll(s, ",", ""), 64)
	return v
}
