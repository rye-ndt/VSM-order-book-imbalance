package modules

import (
	"context"
	"fmt"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

// TelegramNotifier implements output.Notifier by sending messages to a single
// Telegram chat via a bot token. The recipient must have sent /start to the
// bot at least once before messages can be delivered.
type TelegramNotifier struct {
	bot    *tgbotapi.BotAPI
	chatID int64
}

// NewTelegramNotifier constructs a TelegramNotifier and validates the bot token
// against the Telegram API. Returns an error if the token is invalid or empty.
func NewTelegramNotifier(cfg config.TelegramConfig) (output.Notifier, error) {
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("telegram: bot_token is not configured")
	}
	bot, err := tgbotapi.NewBotAPI(cfg.BotToken)
	if err != nil {
		return nil, fmt.Errorf("telegram: init bot: %w", err)
	}
	return &TelegramNotifier{bot: bot, chatID: cfg.ChatID}, nil
}

func (n *TelegramNotifier) Notify(_ context.Context, message string) error {
	msg := tgbotapi.NewMessage(n.chatID, message)
	_, err := n.bot.Send(msg)
	if err != nil {
		return fmt.Errorf("telegram: send: %w", err)
	}
	return nil
}
