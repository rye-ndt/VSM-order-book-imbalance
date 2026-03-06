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

	return &cfg, nil
}
