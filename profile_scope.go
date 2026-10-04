package firebirdotel

import (
	"database/sql/driver"

	"go.opentelemetry.io/otel/trace"
)

// Profiler sampling follows the incoming trace flags. No additional random
// decision is made, so every eligible SQL call in a sampled trace is profiled.
func (c *connState) profileStart(op *operation) {
	if !op.enabled || !c.t.c.Profiler.Enabled || op.ctx.Err() != nil {
		return
	}
	parent := trace.SpanContextFromContext(op.ctx)
	if !parent.IsValid() || !parent.IsSampled() {
		return
	}
	session, err := c.t.c.Profiler.Starter.Start(op.ctx, c.raw, parent, op.d.Summary)
	if err == nil {
		op.profile = session
		op.profileFailure = func() { c.profileDirty.Store(true) }
	} else {
		c.profileDirty.Store(true)
	}
}

func (c *connState) profileFinish(op *operation, client trace.SpanContext, result error) {
	if op.profile == nil {
		return
	}
	if result == driver.ErrSkip {
		if err := op.profile.Cancel(); err != nil {
			c.profileDirty.Store(true)
		}
		return
	}
	if err := op.profile.Finish(client); err != nil {
		c.profileDirty.Store(true)
	}
}
