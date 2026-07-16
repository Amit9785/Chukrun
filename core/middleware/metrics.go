package middleware

import (
	"context"

	"chukrun/core/execution"
	"chukrun/core/telemetry"
)

type TelemetryMiddleware struct {
	telemetry telemetry.Telemetry
}

func NewTelemetryMiddleware(t telemetry.Telemetry) *TelemetryMiddleware {
	return &TelemetryMiddleware{telemetry: t}
}

func (m *TelemetryMiddleware) Name() string               { return "Telemetry" }
func (m *TelemetryMiddleware) Dependencies() []Capability { return nil }
func (m *TelemetryMiddleware) Provides() []Capability     { return nil }
func (m *TelemetryMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *TelemetryMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	if m.telemetry != nil {
		m.telemetry.Counter("middleware_executions_total").Inc(ctx, telemetry.Label{Key: "name", Value: m.Name()})
	}
	return next(ctx, req)
}
