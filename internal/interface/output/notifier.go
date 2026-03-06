package output

import "context"

// Notifier is the output port for sending alert messages to the operator.
type Notifier interface {
	Notify(ctx context.Context, message string) error
}
