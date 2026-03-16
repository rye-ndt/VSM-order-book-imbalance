package modules

import (
	"context"
	"fmt"

	"github.com/michimani/gotwi"
	"github.com/michimani/gotwi/tweet/managetweet"
	"github.com/michimani/gotwi/tweet/managetweet/types"

	"github.com/example/order-book-imbalance/internal/config"
	"github.com/example/order-book-imbalance/internal/interface/output"
)

// XPoster implements output.SocialPoster by posting tweets via Twitter API v2
// using OAuth 1.0a User Context authentication.
type XPoster struct {
	client *gotwi.Client
}

// NewXPoster constructs an XPoster. Returns an error if any credential field
// is missing or if the underlying gotwi client fails to initialise.
func NewXPoster(cfg config.TwitterConfig) (output.SocialPoster, error) {
	if cfg.APIKey == "" || cfg.APIKeySecret == "" || cfg.AccessToken == "" || cfg.AccessTokenSecret == "" {
		return nil, fmt.Errorf("x: credentials incomplete — api_key, api_key_secret, access_token, access_token_secret are all required")
	}

	in := &gotwi.NewClientInput{
		AuthenticationMethod: gotwi.AuthenMethodOAuth1UserContext,
		APIKey:               cfg.APIKey,
		APIKeySecret:         cfg.APIKeySecret,
		OAuthToken:           cfg.AccessToken,
		OAuthTokenSecret:     cfg.AccessTokenSecret,
	}

	client, err := gotwi.NewClient(in)
	if err != nil {
		return nil, fmt.Errorf("x: init client: %w", err)
	}

	return &XPoster{client: client}, nil
}

// Post publishes text as a new tweet. Twitter API v2 enforces a 280-character
// limit; callers are responsible for truncating if needed.
func (x *XPoster) Post(ctx context.Context, text string) error {
	_, err := managetweet.Create(ctx, x.client, &types.CreateInput{
		Text: gotwi.String(text),
	})
	if err != nil {
		return fmt.Errorf("x: post tweet: %w", err)
	}
	return nil
}
