package output

import "context"

// SocialPoster is the output port for publishing status updates to a social
// media platform. Implementations are responsible for authentication and
// platform-specific formatting constraints (e.g. character limits).
type SocialPoster interface {
	Post(ctx context.Context, text string) error
}
