package modules

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
	"github.com/openai/openai-go/shared"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

const xInterpretSystemPrompt = `You are a trading signal writer for a Vietnamese stock market signal account on X (Twitter).

You will receive a JSON object containing a pre-analyzed trading signal. Your job is to turn it into a short, punchy post for retail investors.

## Language
You must write the output in Vietnamese. Natural, conversational Vietnamese — not translated-sounding. Write like a knowledgeable friend posting on X, not like a financial report.

## Rules
- Output a single plain text post. No JSON, no markdown, no hashtag spam.
- Maximum 280 characters.
- You may use 1–2 relevant emojis, placed naturally, not as decoration.
- Do not introduce any information not present in the input JSON.
- Do not use financial jargon. Write so anyone can understand.
- Write in a confident but neutral tone — not hype, not fear.
- The post must include: ticker (if provided), recommendation, entry price, TP, SL, and signal strength.
- If recommendation is "Skip", clearly say to avoid this stock today and state the key_risk in plain language.
- Do not fabricate reasons. Only use what is in reasoning.key_risk, signal_strength, and the prices.

## Format guidance
Lead with the recommendation and ticker.
Follow with the key numbers (entry, TP, SL).
End with one grounding reason from the data.
Keep it under 280 characters total.`

const telegramInterpretSystemPrompt = `You are a trading signal writer for a Vietnamese retail investor Telegram channel.

You will receive a JSON object containing a pre-analyzed trading signal. Your job is to write a single, warm, conversational paragraph that a person with no financial background can read and immediately understand.

## Language
Write entirely in Vietnamese. Sound like a helpful, knowledgeable friend texting in a group chat — not a robot, not a report. Use casual but respectful tone (anh/chị style). A brief natural reaction to the signal is welcome.

## Hallucination rules — strictly enforced
- Every factual claim must come directly from a field in the input JSON.
- Do not invent context, market commentary, or reasons not present in the data.
- You may add a brief human reaction only if it is consistent with confidence and signal_strength in the data.
- If confidence is 1–2, your tone must reflect caution — do not sound encouraging.
- If recommendation is "Skip", lead with that clearly and do not soften it.

## Format
- One paragraph only. No bullet points, no labels, no emoji sections, no headers.
- 4–6 sentences maximum.
- You may use 1–2 emojis inline, naturally placed.
- Must include: ticker, recommendation, entry price, TP, SL, the main reason to act or skip, and one risk or timing note.
- Do not expose raw field names like "ImbalanceRatio" or "FinalScore" — translate them into plain meaning.

## Tone guidance per confidence level
- Confidence 4–5: warm and encouraging, grounded
- Confidence 3: balanced, mention both sides briefly
- Confidence 1–2: honest and cautious, no false encouragement`

const interpretSystemPrompt = `You are a trading signal interpreter for Vietnamese retail investors with limited financial knowledge.

You will receive structured trading signal data for a stock listed on HOSE or HNX. Your job is to explain what the data means in plain, simple language — like explaining to a friend who knows nothing about charts or order books.

## Your role
- You interpret data. You do NOT generate trading advice beyond what the data supports.
- Every claim in your reasoning MUST be directly traceable to a field in ` + "`data_used`" + `.
- Do not introduce information, context, or opinions not present in the input.

## Output format
You must respond with a single valid JSON object matching this exact schema. No preamble, no markdown, no explanation outside the JSON.

## Confidence scoring rules
Assign confidence on a 1–5 scale using this rubric:
- 5: Bull regime + FinalScore well above threshold + ImbalanceRatio >= 3.0 + SnapshotCount >= 10 + Above20MA true + no gap
- 4: Most conditions met, one moderate weakness
- 3: Mixed signals — some bullish, some neutral or contradictory
- 2: Bear or Choppy regime, or large OpenGap, or PositionSizeFlag is Reduced
- 1: Bear regime + multiple conflicting signals + PositionSizeFlag is Skip or Reduced

## Signal strength rules
- Strong: confidence 4–5
- Moderate: confidence 3
- Weak: confidence 1–2

## Reasoning rules
- Each reasoning field must be exactly one sentence.
- Each sentence must reference at least one named field from data_used by name and value.
- If two fields contradict each other (e.g. Above20MA: true but Regime: Bear), you must acknowledge the contradiction explicitly in regime_fit.
- Do not use jargon. Write as if explaining to someone who has never read a financial chart.
- Avoid words like: "bullish", "bearish", "resistance", "support", "consolidation", "breakout". Use plain equivalents instead.

## T+2 note rules
Vietnam stock market uses T+2 settlement. The t2_note must address one of:
- Whether the stock is likely liquid enough to sell before T+2 if needed
- Whether holding to T+2 changes the risk profile given current momentum
- Whether an OpenGap makes the entry price stale by the time settlement clears

## Special cases
- If PositionSizeFlag is "Skip": recommendation must be "Skip" regardless of other signals.
- If PositionSizeFlag is "Reduced": confidence must be capped at 3, and key_risk must mention position sizing.
- If OpenGap > +1.5%: avoid_if must reference the gap explicitly.
- If Regime is "Bear": regime_fit must explain why buying in a down market needs extra caution in plain terms.
- If ImbalanceRatio < 1.5 or SnapshotCount < 5: order_book must flag low conviction.`

var interpretSchema = map[string]any{
	"type": "object",
	"properties": map[string]any{
		"recommendation":  map[string]any{"type": "string", "enum": []string{"Buy", "Skip"}},
		"confidence":      map[string]any{"type": "integer"},
		"signal_strength": map[string]any{"type": "string", "enum": []string{"Strong", "Moderate", "Weak"}},
		"entry_price":     map[string]any{"type": "number"},
		"tp_price":        map[string]any{"type": "number"},
		"sl_price":        map[string]any{"type": "number"},
		"data_used":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
		"reasoning": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"order_book": map[string]any{"type": "string"},
				"technicals": map[string]any{"type": "string"},
				"regime_fit": map[string]any{"type": "string"},
				"key_risk":   map[string]any{"type": "string"},
			},
			"required":             []string{"order_book", "technicals", "regime_fit", "key_risk"},
			"additionalProperties": false,
		},
		"t2_note":        map[string]any{"type": "string"},
		"action_clarity": map[string]any{"type": "string"},
		"avoid_if":       map[string]any{"type": "string"},
	},
	"required":             []string{"recommendation", "confidence", "signal_strength", "entry_price", "tp_price", "sl_price", "data_used", "reasoning", "t2_note", "action_clarity", "avoid_if"},
	"additionalProperties": false,
}

type OpenAIClient struct {
	client *openai.Client
	model  openai.ChatModel
}

func NewOpenAIClient(cfg config.OpenAIConfig) (output.AI, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("openai: api key is required")
	}
	client := openai.NewClient(option.WithAPIKey(cfg.APIKey))
	return &OpenAIClient{client: &client, model: openai.ChatModel(cfg.Model)}, nil
}

func (o *OpenAIClient) Response(ctx context.Context, text string) (string, error) {
	msg, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(text),
		},
	})
	if err != nil {
		return "", fmt.Errorf("openai: completion: %w", err)
	}

	return msg.Choices[0].Message.Content, nil
}

func (o *OpenAIClient) Interpret(ctx context.Context, rec output.SignalRecord, scoreThreshold int) (output.SignalInterpretation, error) {
	dataUsed := buildDataUsed(rec, scoreThreshold)
	userMsg := buildInterpretPrompt(dataUsed)

	msg, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(interpretSystemPrompt),
			openai.UserMessage(userMsg),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &shared.ResponseFormatJSONSchemaParam{
				JSONSchema: shared.ResponseFormatJSONSchemaJSONSchemaParam{
					Name:   "signal_interpretation",
					Schema: interpretSchema,
					Strict: openai.Bool(true),
				},
			},
		},
	})
	if err != nil {
		return output.SignalInterpretation{}, fmt.Errorf("openai: interpret: %w", err)
	}

	var result output.SignalInterpretation
	if err := json.Unmarshal([]byte(msg.Choices[0].Message.Content), &result); err != nil {
		return output.SignalInterpretation{}, fmt.Errorf("openai: interpret: unmarshal: %w", err)
	}
	return result, nil
}

func (o *OpenAIClient) XInterpret(ctx context.Context, interp output.SignalInterpretation) (string, error) {
	return o.interpretWith(ctx, xInterpretSystemPrompt, interp)
}

func (o *OpenAIClient) TelegramInterpret(ctx context.Context, interp output.SignalInterpretation) (string, error) {
	return o.interpretWith(ctx, telegramInterpretSystemPrompt, interp)
}

func (o *OpenAIClient) interpretWith(ctx context.Context, systemPrompt string, interp output.SignalInterpretation) (string, error) {
	payload, err := json.Marshal(interp)
	if err != nil {
		return "", fmt.Errorf("openai: interpret: marshal: %w", err)
	}
	msg, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(systemPrompt),
			openai.UserMessage(string(payload)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("openai: interpret: %w", err)
	}
	return msg.Choices[0].Message.Content, nil
}

func buildDataUsed(rec output.SignalRecord, scoreThreshold int) []string {
	above20MA := "false"
	if rec.Above20MA {
		above20MA = "true"
	}
	return []string{
		fmt.Sprintf("Symbol: %s", rec.Symbol),
		fmt.Sprintf("ImbalanceRatio: %.2f", rec.ImbalanceRatio),
		fmt.Sprintf("SnapshotCount: %d", rec.SnapshotCount),
		fmt.Sprintf("FinalScore: %d", rec.FinalScore),
		fmt.Sprintf("RegimeThreshold: %d", scoreThreshold),
		fmt.Sprintf("Regime: %s", rec.Regime),
		fmt.Sprintf("CandlePattern: %s", rec.CandlePattern),
		fmt.Sprintf("VPR: %s", rec.VPR),
		fmt.Sprintf("VolumeTrend: %s", rec.VolumeTrend),
		fmt.Sprintf("VolumeRatio: %.2fx", rec.VolumeRatio),
		fmt.Sprintf("MomentumScore: %d", rec.MomentumScore),
		fmt.Sprintf("ResistanceDistance: %.2f%%", rec.ResistanceDistance*100),
		fmt.Sprintf("Above20MA: %s", above20MA),
		fmt.Sprintf("PositionSizeFlag: %s", rec.PositionSizeFlag),
		fmt.Sprintf("OpenGap: %+.2f%%", rec.OpenGap*100),
		fmt.Sprintf("IndicatedPrice: %.0f", rec.IndicatedPrice),
		fmt.Sprintf("TPPrice: %.0f", rec.TPPrice),
	}
}

func buildInterpretPrompt(dataUsed []string) string {
	dataUsedJSON, _ := json.Marshal(dataUsed)
	return fmt.Sprintf("SIGNAL DATA:\n%s\n\nDATA_USED (echo this array exactly in your response):\n%s",
		strings.Join(dataUsed, "\n"),
		string(dataUsedJSON),
	)
}

const summarizeSessionSystemPrompt = `Bạn là trợ lý thị trường chứng khoán Việt Nam. Bạn sẽ nhận được dữ liệu tóm tắt phiên ATO (trước giờ mở cửa) trên sàn chứng khoán Việt Nam.

Hãy viết một tin nhắn Telegram duy nhất (tiếng Việt) bao gồm các phần sau:

1. Một dòng tổng quan: số cổ phiếu theo dõi, regime thị trường, ngưỡng điểm áp dụng.

2. Với mỗi cổ phiếu — nêu rõ:
   - Tên + FinalScore + PositionSize
   - Nếu có tín hiệu mua: giá khớp dự kiến (IndicatedPrice), tỷ lệ cầu/cung (PeakRatio)
   - Nếu không có tín hiệu: lý do cụ thể từ dữ liệu — ví dụ "không hình thành giá (EstMatchedPrice = 0)", "tỷ lệ cầu/cung chỉ đạt X× (cần ≥ 3×)", "bị chặn bởi gap X%", v.v.
   - Nếu bị drop: nêu DropReason

3. Kết luận một câu: phiên hôm nay có thể giao dịch hay nên chờ?

Quy tắc:
- Bắt buộc dùng số liệu cụ thể từ input cho mỗi cổ phiếu. Không được viết chung chung.
- Chỉ dùng dữ liệu từ input. Không thêm bình luận thị trường bên ngoài.
- Không đưa ra lời khuyên mua/bán ngoài những gì dữ liệu tín hiệu hỗ trợ.
- Giọng văn: bình tĩnh, thực tế, tối đa 2 emoji.
- Nếu PeakRatio = 0 hoặc IndicatedPrice = 0, nói rõ là không có dữ liệu sổ lệnh — không suy diễn thêm.`

const sellWarnSystemPrompt = `Bạn là trợ lý thị trường chứng khoán Việt Nam đang theo dõi phiên ATO (trước giờ mở cửa).

Bạn sẽ nhận được dữ liệu sổ lệnh cho một cổ phiếu đang có áp lực bán mạnh. Cổ phiếu này ĐÃ được hệ thống chọn vào danh sách theo dõi đêm qua dựa trên các chỉ số kỹ thuật tích cực — nhưng hiện tại sổ lệnh ATO đang đi ngược lại kỳ vọng đó.

Hãy viết một cảnh báo ngắn trên Telegram (2–4 câu, tiếng Việt) bao gồm:
- Tên cổ phiếu, điểm số đêm qua (FinalScore) và giá khớp lệnh dự kiến hiện tại
- Tỷ lệ mất cân bằng — bên bán đang áp đảo bên mua bao nhiêu lần, trái ngược với tín hiệu kỹ thuật đêm qua
- Các mức giá có khối lượng bán lớn nhất (tường cản cung)
- Kết luận một câu: sự mâu thuẫn giữa tín hiệu đêm qua và áp lực bán hiện tại có nghĩa gì cho phiên mở cửa ATO

Quy tắc:
- Chỉ dùng dữ liệu được cung cấp. Không bình luận bên ngoài.
- Không khuyên bán khống hoặc mua vào. Chỉ quan sát thực tế.
- Làm nổi bật sự mâu thuẫn — đây chính là giá trị của cảnh báo này.
- Giọng văn: bình tĩnh, thông tin. Tối đa 1 emoji.`

func (o *OpenAIClient) SummarizeSession(ctx context.Context, rec output.SessionSummaryRecord) (string, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("openai: summarize session: marshal: %w", err)
	}
	msg, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(summarizeSessionSystemPrompt),
			openai.UserMessage(string(payload)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("openai: summarize session: %w", err)
	}
	return msg.Choices[0].Message.Content, nil
}

func (o *OpenAIClient) WarnSellPressure(ctx context.Context, rec output.SellWarnRecord) (string, error) {
	payload, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("openai: sell warn: marshal: %w", err)
	}
	msg, err := o.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: o.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sellWarnSystemPrompt),
			openai.UserMessage(string(payload)),
		},
	})
	if err != nil {
		return "", fmt.Errorf("openai: sell warn: %w", err)
	}
	return msg.Choices[0].Message.Content, nil
}
