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
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/input"
)

const (
	ssiIDSMarketDataChannel = "X"
	ssiIDSDataTypeX     = "X"
	ssiIDSDataTypeQuote = "Quote"
	ssiIDSDataTypeTrade = "Trade"

	signalRHub            = "FcMarketDataV2Hub"
	signalRHubLower       = "fcmarketdatav2hub"
	signalRClientProtocol = "1.5"
	signalRSwitchChannels = "SwitchChannels"
	signalRBroadcast      = "broadcast"
)

// ssiIDSEnvelope is the Broadcast payload delivered by FcMarketDataV2Hub.
// DataType identifies the message kind; Content is a JSON-encoded string
// that must be decoded a second time to obtain the actual payload.
// Go's json decoder matches field names case-insensitively, so this struct
// handles both "DataType" (Quote) and "datatype" (Trade) from the spec.
type ssiIDSEnvelope struct {
	DataType string `json:"DataType"`
	Content  string `json:"Content"`
}

// ssiSignalRMessage is the outer SignalR WebSocket frame.
type ssiSignalRMessage struct {
	M []ssiSignalRHubMessage `json:"M"`
}

type ssiSignalRHubMessage struct {
	H string            `json:"H"`
	M string            `json:"M"`
	A []json.RawMessage `json:"A"`
}

// ssiSignalRInvoke is a SignalR hub method invocation sent by the client.
type ssiSignalRInvoke struct {
	H string   `json:"H"`
	M string   `json:"M"`
	A []string `json:"A"`
	I int      `json:"I"`
}

// ssiIDSQuote is the unified X-type market data message from FcMarketDataV2Hub.
// All price and volume fields are float64 on the wire.
type ssiIDSQuote struct {
	Symbol          string  `json:"Symbol"`
	Exchange        string  `json:"Exchange"`
	TradingDate     string  `json:"TradingDate"`
	TradingTime     string  `json:"Time"`
	Ceiling         float64 `json:"Ceiling"`
	RefPrice        float64 `json:"RefPrice"`
	EstMatchedPrice float64 `json:"EstMatchedPrice"`

	BidPrice1  float64 `json:"BidPrice1"`
	BidVol1    float64 `json:"BidVol1"`
	BidPrice2  float64 `json:"BidPrice2"`
	BidVol2    float64 `json:"BidVol2"`
	BidPrice3  float64 `json:"BidPrice3"`
	BidVol3    float64 `json:"BidVol3"`
	BidPrice4  float64 `json:"BidPrice4"`
	BidVol4    float64 `json:"BidVol4"`
	BidPrice5  float64 `json:"BidPrice5"`
	BidVol5    float64 `json:"BidVol5"`
	BidPrice6  float64 `json:"BidPrice6"`
	BidVol6    float64 `json:"BidVol6"`
	BidPrice7  float64 `json:"BidPrice7"`
	BidVol7    float64 `json:"BidVol7"`
	BidPrice8  float64 `json:"BidPrice8"`
	BidVol8    float64 `json:"BidVol8"`
	BidPrice9  float64 `json:"BidPrice9"`
	BidVol9    float64 `json:"BidVol9"`
	BidPrice10 float64 `json:"BidPrice10"`
	BidVol10   float64 `json:"BidVol10"`

	AskPrice1  float64 `json:"AskPrice1"`
	AskVol1    float64 `json:"AskVol1"`
	AskPrice2  float64 `json:"AskPrice2"`
	AskVol2    float64 `json:"AskVol2"`
	AskPrice3  float64 `json:"AskPrice3"`
	AskVol3    float64 `json:"AskVol3"`
	AskPrice4  float64 `json:"AskPrice4"`
	AskVol4    float64 `json:"AskVol4"`
	AskPrice5  float64 `json:"AskPrice5"`
	AskVol5    float64 `json:"AskVol5"`
	AskPrice6  float64 `json:"AskPrice6"`
	AskVol6    float64 `json:"AskVol6"`
	AskPrice7  float64 `json:"AskPrice7"`
	AskVol7    float64 `json:"AskVol7"`
	AskPrice8  float64 `json:"AskPrice8"`
	AskVol8    float64 `json:"AskVol8"`
	AskPrice9  float64 `json:"AskPrice9"`
	AskVol9    float64 `json:"AskVol9"`
	AskPrice10 float64 `json:"AskPrice10"`
	AskVol10   float64 `json:"AskVol10"`
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

	snapMu sync.RWMutex
	cache  map[string]input.OrderBookSnapshot
}

func NewSSIOrderBookClient(cfg config.SSIConfig) input.OrderBookClient {
	return &SSIOrderBookClient{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		cache:      make(map[string]input.OrderBookSnapshot),
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
	params := url.Values{
		"clientProtocol": {signalRClientProtocol},
		"connectionData": {`[{"name":"` + signalRHubLower + `"}]`},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.cfg.StreamURL+"/negotiate?"+params.Encode(), nil)
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

	c.snapMu.Lock()
	c.cache = make(map[string]input.OrderBookSnapshot)
	c.snapMu.Unlock()

	token, err := c.bearerToken(ctx)
	if err != nil {
		return err
	}

	connToken, err := c.signalRNegotiate(ctx, token)
	if err != nil {
		return err
	}

	encodedToken := url.QueryEscape(connToken)
	encodedConnData := url.QueryEscape(`[{"name":"` + signalRHubLower + `"}]`)

	wsBase := strings.NewReplacer("https://", "wss://", "http://", "ws://").Replace(c.cfg.StreamURL)
	wsURL := wsBase + "/connect?transport=webSockets&clientProtocol=" + signalRClientProtocol +
		"&connectionToken=" + encodedToken + "&connectionData=" + encodedConnData

	hdr := http.Header{"Authorization": {"Bearer " + token}}
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(ctx, wsURL, hdr)
	if err != nil {
		return fmt.Errorf("ssi ob: websocket dial: %w", err)
	}

	startReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.cfg.StreamURL+"/start?transport=webSockets&clientProtocol="+signalRClientProtocol+
			"&connectionToken="+encodedToken+"&connectionData="+encodedConnData, nil)
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
	startBody, _ := io.ReadAll(startResp.Body)
	startResp.Body.Close()
	log.Printf("[ssi-ob] /start HTTP %d: %s", startResp.StatusCode, startBody)
	if startResp.StatusCode != 200 {
		conn.Close()
		return fmt.Errorf("ssi ob: /start returned HTTP %d", startResp.StatusCode)
	}

	invoke := ssiSignalRInvoke{
		H: signalRHub,
		M: signalRSwitchChannels,
		A: []string{ssiIDSMarketDataChannel + ":ALL"},
		I: 1,
	}
	if err := conn.WriteJSON(invoke); err != nil {
		conn.Close()
		return fmt.Errorf("ssi ob: send SwitchChannels: %w", err)
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

	totalFrames := 0
	broadcastFrames := 0
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("[ssi-ob] readLoop done — frames: %d total, %d broadcast", totalFrames, broadcastFrames)
				return
			}
			log.Printf("[ssi-ob] WARN connection lost after %d frames (%d broadcast) — no more quotes will be received this session: %v", totalFrames, broadcastFrames, err)
			return
		}
		totalFrames++
		if totalFrames == 1 {
			log.Printf("[ssi-ob] first frame received: %s", raw)
		}

		var outer ssiSignalRMessage
		if err := json.Unmarshal(raw, &outer); err != nil {
			log.Printf("[ssi-ob] parse signalr frame: %v", err)
			continue
		}

		for _, msg := range outer.M {
			if !strings.EqualFold(msg.H, signalRHub) || !strings.EqualFold(msg.M, signalRBroadcast) || len(msg.A) == 0 {
				if msg.H != "" || msg.M != "" {
					log.Printf("[ssi-ob] non-broadcast hub frame: H=%q M=%q", msg.H, msg.M)
				}
				continue
			}
			broadcastFrames++
			// A[0] is a JSON-encoded string containing the envelope JSON.
			var payloadStr string
			if err := json.Unmarshal(msg.A[0], &payloadStr); err != nil {
				log.Printf("[ssi-ob] parse broadcast string: %v", err)
				continue
			}
			var env ssiIDSEnvelope
			if err := json.Unmarshal([]byte(payloadStr), &env); err != nil {
				log.Printf("[ssi-ob] parse broadcast envelope: %v", err)
				continue
			}
			switch env.DataType {
			case ssiIDSDataTypeX, ssiIDSDataTypeQuote, ssiIDSDataTypeTrade:
				c.handleQuote(env.Content)
			default:
				log.Printf("[ssi-ob] unhandled DataType %q", env.DataType)
			}
		}
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

	snap := quoteToSnapshot(q)

	c.snapMu.Lock()
	if snap.IndicatedPrice == 0 {
		if prev, ok := c.cache[q.Symbol]; ok && prev.IndicatedPrice > 0 {
			snap.IndicatedPrice = prev.IndicatedPrice
		}
	}
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

func quoteToSnapshot(q ssiIDSQuote) input.OrderBookSnapshot {
	rawBids := [10][2]float64{
		{q.BidPrice1, q.BidVol1},
		{q.BidPrice2, q.BidVol2},
		{q.BidPrice3, q.BidVol3},
		{q.BidPrice4, q.BidVol4},
		{q.BidPrice5, q.BidVol5},
		{q.BidPrice6, q.BidVol6},
		{q.BidPrice7, q.BidVol7},
		{q.BidPrice8, q.BidVol8},
		{q.BidPrice9, q.BidVol9},
		{q.BidPrice10, q.BidVol10},
	}
	rawAsks := [10][2]float64{
		{q.AskPrice1, q.AskVol1},
		{q.AskPrice2, q.AskVol2},
		{q.AskPrice3, q.AskVol3},
		{q.AskPrice4, q.AskVol4},
		{q.AskPrice5, q.AskVol5},
		{q.AskPrice6, q.AskVol6},
		{q.AskPrice7, q.AskVol7},
		{q.AskPrice8, q.AskVol8},
		{q.AskPrice9, q.AskVol9},
		{q.AskPrice10, q.AskVol10},
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
		IndicatedPrice: q.EstMatchedPrice,
		CeilingPrice:   q.Ceiling,
		RefPrice:       q.RefPrice,
	}
}
