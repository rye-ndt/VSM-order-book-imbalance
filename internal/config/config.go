package config

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// Config holds application configuration loaded from YAML.
type Config struct {
	// HTTP server listen address, e.g. ":8080".
	HTTPListenAddr string         `mapstructure:"http_listen_addr"`
	DB             DBConfig       `mapstructure:"db"`
	SSI            SSIConfig      `mapstructure:"ssi"`
	Telegram       TelegramConfig `mapstructure:"telegram"`
	Twitter        TwitterConfig  `mapstructure:"twitter"`
	OpenAI         OpenAIConfig   `mapstructure:"openai"`
	Signal         SignalConfig   `mapstructure:"signal"`
	Cron           CronConfig     `mapstructure:"cron"`
}

type OpenAIConfig struct {
	APIKey string `mapstructure:"api_key"`
	Model  string `mapstructure:"model"`
}

// CronConfig holds cron schedule expressions for scheduled jobs.
// Expressions use standard 5-field cron syntax (minute hour dom month dow).
// All times are interpreted in ICT (Asia/Ho_Chi_Minh, UTC+7).
type CronConfig struct {
	// MarketData is when to fetch previous-day market data and compute metrics.
	// Default: "30 3 * * *" (03:30 ICT)
	MarketData string `mapstructure:"market_data"`
	// ATOMonitor is when to start monitoring the ATO order book.
	// Default: "0 9 * * *" (09:00 ICT)
	ATOMonitor string `mapstructure:"ato_monitor"`
}

// SignalConfig holds every numeric threshold that controls when an ATO signal
// fires. All values have sensible defaults applied in Load() so the section is
// optional in config.yaml — only override what you want to tune.
type SignalConfig struct {
	// --- Order book snapshot gate ---

	// MinImbalanceRatio: bid/ask volume ratio must be at or above this value
	// across all stability-window snapshots.
	// Suggested: 3.0 (aggressive) … 2.5 (more signals, more noise)
	MinImbalanceRatio float64 `mapstructure:"min_imbalance_ratio"`

	// MaxSpoofFraction: reject snapshot if the single largest bid order
	// accounts for more than this fraction of total bid volume.
	// Suggested: 0.40 — rejects obvious spoofing while allowing natural depth
	MaxSpoofFraction float64 `mapstructure:"max_spoof_fraction"`

	// MinBidLevels: minimum number of distinct bid price levels required.
	// Suggested: 5 — ensures real distributed demand, not a single fat order
	MinBidLevels int `mapstructure:"min_bid_levels"`

	// AskWallFraction: a single ask level is flagged as a "wall" if its volume
	// is at or above this fraction of total ask volume.
	// Suggested: 0.30 — blocks signals when a big seller is sitting overhead
	AskWallFraction float64 `mapstructure:"ask_wall_fraction"`

	// AskWallPriceRange: only check for ask walls up to this multiplier above
	// the indicated price (e.g. 1.03 = within 3%).
	// Suggested: 1.03 — walls further out don't threaten immediate upside
	AskWallPriceRange float64 `mapstructure:"ask_wall_price_range"`

	// StabilityWindow: number of consecutive snapshots that must all pass the
	// order book conditions before a signal fires.
	// Suggested: 3 — ~6 seconds of confirmation at the default 2-second poll
	StabilityWindow int `mapstructure:"stability_window"`

	// --- Signal gate ---

	// MaxGapFromRef: maximum allowed gap between indicated price and the
	// prior-day close (ref price). Expressed as a fraction (0.05 = 5%).
	// Suggested: 0.05 — chasing a 5%+ gap-up at open is high-risk
	MaxGapFromRef float64 `mapstructure:"max_gap_from_ref"`

	// TPRatio: take-profit target as a multiplier of the indicated price
	// (e.g. 1.03 = 3% above entry). Stored in signal_log at fire time.
	// Suggested: 1.03 — aligns with typical T+0 intraday move on HOSE
	TPRatio float64 `mapstructure:"tp_ratio"`

	// --- Regime-adjusted nightly screening thresholds ---
	// Full = enter with full position size; Half = enter with half size.
	// Anything below Half is Skip (not on the watchlist).

	// Bull market thresholds — loosen since trend is your friend.
	// Suggested: Full ≥ 10, Half ≥ 7
	BullFullScore int `mapstructure:"bull_full_score"`
	BullHalfScore int `mapstructure:"bull_half_score"`

	// Choppy / sideways market — tighten slightly.
	// Suggested: Full ≥ 11, Half ≥ 8
	ChoppyFullScore int `mapstructure:"choppy_full_score"`
	ChoppyHalfScore int `mapstructure:"choppy_half_score"`

	// Bear market — only trade the very best setups.
	// Suggested: Full ≥ 12, Half ≥ 9
	BearFullScore int `mapstructure:"bear_full_score"`
	BearHalfScore int `mapstructure:"bear_half_score"`
}

// TwitterConfig holds OAuth 1.0a credentials for posting to X (Twitter).
// Obtain all four values from the X Developer Portal (developer.twitter.com)
// under your app's "Keys and Tokens" section.
type TwitterConfig struct {
	// APIKey is the OAuth 1.0a Consumer Key (also called "API Key").
	APIKey string `mapstructure:"api_key"`
	// APIKeySecret is the OAuth 1.0a Consumer Secret (also called "API Key Secret").
	APIKeySecret string `mapstructure:"api_key_secret"`
	// AccessToken is the per-account OAuth 1.0a access token.
	AccessToken string `mapstructure:"access_token"`
	// AccessTokenSecret is the per-account OAuth 1.0a access token secret.
	AccessTokenSecret string `mapstructure:"access_token_secret"`
}

// TelegramConfig holds credentials for the Telegram notification bot.
// BotToken comes from @BotFather. ChatID is the recipient's Telegram user ID
// (send /start to the bot once, then read it from the bot's getUpdates response).
type TelegramConfig struct {
	BotToken string `mapstructure:"bot_token"`
	ChatID   int64  `mapstructure:"chat_id"`
}

// SSIConfig holds credentials and endpoint URLs for the SSI FastConnectData API.
// BaseURL is used for REST endpoints (fc-data.ssi.com.vn).
// StreamURL is the full WebSocket URL for the IDS streaming service
// (fcmarket.ssi.com.vn); the exact path must be confirmed against the spec.
type SSIConfig struct {
	ConsumerID     string `mapstructure:"consumer_id"`
	ConsumerSecret string `mapstructure:"consumer_secret"`
	BaseURL        string `mapstructure:"base_url"`
	StreamURL      string `mapstructure:"stream_url"`
}

// DBConfig holds relational database configuration and secrets.
type DBConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
	SSLMode  string `mapstructure:"sslmode"`
}

// Load reads configuration from the given YAML file path.
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Ensure mapstructure is used explicitly so it stays imported.
	viperDecoderConfigOption := viper.DecodeHook(mapstructure.StringToTimeDurationHookFunc())
	_ = viperDecoderConfigOption

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	// Provide sensible defaults if not set.
	if cfg.HTTPListenAddr == "" {
		cfg.HTTPListenAddr = ":8080"
	}
	if cfg.Cron.MarketData == "" {
		cfg.Cron.MarketData = "30 3 * * *"
	}
	if cfg.Cron.ATOMonitor == "" {
		cfg.Cron.ATOMonitor = "0 9 * * *"
	}

	s := &cfg.Signal
	if s.MinImbalanceRatio == 0 {
		s.MinImbalanceRatio = 3.0
	}
	if s.MaxSpoofFraction == 0 {
		s.MaxSpoofFraction = 0.40
	}
	if s.MinBidLevels == 0 {
		s.MinBidLevels = 5
	}
	if s.AskWallFraction == 0 {
		s.AskWallFraction = 0.30
	}
	if s.AskWallPriceRange == 0 {
		s.AskWallPriceRange = 1.03
	}
	if s.StabilityWindow == 0 {
		s.StabilityWindow = 3
	}
	if s.MaxGapFromRef == 0 {
		s.MaxGapFromRef = 0.05
	}
	if s.TPRatio == 0 {
		s.TPRatio = 1.03
	}
	if s.BullFullScore == 0 {
		s.BullFullScore = 10
	}
	if s.BullHalfScore == 0 {
		s.BullHalfScore = 7
	}
	if s.ChoppyFullScore == 0 {
		s.ChoppyFullScore = 11
	}
	if s.ChoppyHalfScore == 0 {
		s.ChoppyHalfScore = 8
	}
	if s.BearFullScore == 0 {
		s.BearFullScore = 12
	}
	if s.BearHalfScore == 0 {
		s.BearHalfScore = 9
	}

	if cfg.OpenAI.Model == "" {
		cfg.OpenAI.Model = "gpt-4o-mini"
	}

	return &cfg, nil
}
