package gateway

import (
	"context"
	"errors"
	"io"
)

// redactProtocolError keeps upstream protocol adapters from serializing
// repository/provider error details into their response envelopes. The native
// Agent and A2A servers receive an error from Runner and may call Error() on
// it; only Gateway-owned stable sentinels are allowed across that boundary.
func safeCloseProtocolBody(body io.Closer) (err error) {
	if body == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrInvalid
		}
	}()
	return body.Close()
}

func redactProtocolError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	case errors.Is(err, ErrInvalid):
		return ErrInvalid
	case errors.Is(err, ErrUnauthenticated):
		return ErrUnauthenticated
	case errors.Is(err, ErrNotReady):
		return ErrNotReady
	case errors.Is(err, ErrRateLimited):
		return ErrRateLimited
	case errors.Is(err, ErrDuplicateMessage):
		return ErrDuplicateMessage
	case errors.Is(err, ErrAuditWriteFailed):
		return ErrAuditWriteFailed
	default:
		return ErrExecution
	}
}
