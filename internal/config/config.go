package config

import (
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

// Config holds application configuration loaded from YAML.
type Config struct {
	// HTTP server listen address, e.g. ":8080".
	HTTPListenAddr string    `mapstructure:"http_listen_addr"`
	DB             DBConfig  `mapstructure:"db"`
	SSI            SSIConfig `mapstructure:"ssi"`
}

// SSIConfig holds credentials and base URL for the SSI FastConnectData API.
type SSIConfig struct {
	ConsumerID     string `mapstructure:"consumer_id"`
	ConsumerSecret string `mapstructure:"consumer_secret"`
	BaseURL        string `mapstructure:"base_url"`
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
