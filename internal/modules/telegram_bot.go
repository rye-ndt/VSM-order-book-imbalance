package modules

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

var _ output.Notifier = (*TelegramBot)(nil)

// TelegramBot handles incoming commands and broadcasts signal alerts to all
// subscribers. It implements output.Notifier so it can replace TelegramNotifier
// as the job's alert delivery mechanism.
type TelegramBot struct {
	bot           *tgbotapi.BotAPI
	store         output.BotStore
	location      *time.Location
	defaultChatID int64
}

func NewTelegramBot(cfg config.TelegramConfig, store output.BotStore, location *time.Location) (*TelegramBot, error) {
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("telegram: bot_token is not configured")
	}
	bot, err := tgbotapi.NewBotAPI(cfg.BotToken)
	if err != nil {
		return nil, fmt.Errorf("telegram: init bot: %w", err)
	}
	return &TelegramBot{bot: bot, store: store, location: location, defaultChatID: cfg.ChatID}, nil
}

// Notify sends message to all subscribers plus the default chat_id (if set).
// Individual send failures are logged but do not stop delivery to other recipients.
func (b *TelegramBot) Notify(ctx context.Context, message string) error {
	chatIDs, err := b.store.LoadSubscribers(ctx)
	if err != nil {
		log.Printf("telegram: load subscribers: %v", err)
	}
	if b.defaultChatID != 0 {
		chatIDs = appendIfMissing(chatIDs, b.defaultChatID)
	}
	for _, id := range chatIDs {
		msg := tgbotapi.NewMessage(id, message)
		if _, err := b.bot.Send(msg); err != nil {
			log.Printf("telegram: send to %d: %v", id, err)
		}
	}
	return nil
}

// Run polls for bot updates and handles commands until ctx is cancelled.
func (b *TelegramBot) Run(ctx context.Context) {
	u := tgbotapi.NewUpdate(0)
	u.Timeout = 60
	updates := b.bot.GetUpdatesChan(u)
	for {
		select {
		case <-ctx.Done():
			b.bot.StopReceivingUpdates()
			return
		case update, ok := <-updates:
			if !ok {
				return
			}
			if update.Message == nil || !update.Message.IsCommand() {
				continue
			}
			b.handleCommand(ctx, update.Message)
		}
	}
}

func (b *TelegramBot) handleCommand(ctx context.Context, msg *tgbotapi.Message) {
	var text string
	switch msg.Command() {
	case "hello", "start":
		text = helloText()
	case "signal":
		text = b.signalText(ctx)
	case "subscribe":
		text = b.subscribe(ctx, msg.Chat.ID)
	case "unsubscribe":
		text = b.unsubscribe(ctx, msg.Chat.ID)
	default:
		return
	}
	reply := tgbotapi.NewMessage(msg.Chat.ID, text)
	if _, err := b.bot.Send(reply); err != nil {
		log.Printf("telegram: reply to %d: %v", msg.Chat.ID, err)
	}
}

func helloText() string {
	return `Xin chào! Tôi là RegimerBot — bot tín hiệu ATO cho thị trường chứng khoán Việt Nam (HOSE/HNX).

Tôi làm gì?
Mỗi sáng từ 9:00–9:15, tôi phân tích sổ lệnh ATO theo thời gian thực. Khi phát hiện dòng tiền mua vào mạnh và đủ điều kiện kỹ thuật, tôi gửi tín hiệu ngay lập tức gồm: mã cổ phiếu, giá vào lệnh đề xuất và mức chốt lời.

Lệnh:
/signal — Xem tín hiệu hôm nay
/subscribe — Đăng ký nhận tín hiệu tự động
/unsubscribe — Hủy đăng ký`
}

func (b *TelegramBot) signalText(ctx context.Context) string {
	signals, err := b.store.LoadTodaySignals(ctx)
	if err != nil {
		log.Printf("telegram: load today signals: %v", err)
		return "Không thể lấy dữ liệu tín hiệu. Vui lòng thử lại sau."
	}
	if len(signals) == 0 {
		return `Hôm nay chưa có tín hiệu ATO nào.

Có thể do:
- Thị trường không đủ điều kiện (Bear/Choppy)
- Sổ lệnh sáng nay không đạt ngưỡng mất cân bằng
- Phiên ATO (9:00–9:15) chưa bắt đầu

Dùng /subscribe để nhận thông báo ngay khi tín hiệu xuất hiện.`
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Tín hiệu ATO hôm nay (%d tín hiệu):\n", len(signals)))
	for _, s := range signals {
		sizeLabel := "Vào đủ"
		if s.PositionSizeFlag == "Half" {
			sizeLabel = "Vào nửa"
		}
		sb.WriteString(fmt.Sprintf(
			"\n[%s] %s\nGiá vào: %.0f  |  Chốt lời: %.0f\nCỡ lệnh: %s\n",
			s.FiredAt.In(b.location).Format("15:04"),
			s.Symbol,
			s.EntryPrice,
			s.TPPrice,
			sizeLabel,
		))
	}
	return strings.TrimSpace(sb.String())
}

func (b *TelegramBot) subscribe(ctx context.Context, chatID int64) string {
	if err := b.store.AddSubscriber(ctx, chatID); err != nil {
		log.Printf("telegram: subscribe %d: %v", chatID, err)
		return "Đăng ký thất bại. Vui lòng thử lại."
	}
	return "Đã đăng ký! Bạn sẽ nhận tín hiệu ATO ngay khi xuất hiện mỗi sáng.\n\nGõ /unsubscribe để hủy bất kỳ lúc nào."
}

func (b *TelegramBot) unsubscribe(ctx context.Context, chatID int64) string {
	if err := b.store.RemoveSubscriber(ctx, chatID); err != nil {
		log.Printf("telegram: unsubscribe %d: %v", chatID, err)
		return "Hủy đăng ký thất bại. Vui lòng thử lại."
	}
	return "Đã hủy đăng ký. Bạn sẽ không còn nhận tín hiệu tự động nữa."
}

func appendIfMissing(ids []int64, id int64) []int64 {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}
