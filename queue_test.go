package gatequeue

import "testing"

// Tests expect a Queue API; implementation is intentionally absent for 0-1.
type Clock interface{ Now() int64 }

func TestNotReadyCannotRelease(t *testing.T) {
	t.Skip("implement Queue first")
}
