package middleware

import (
	"context"
	"fmt"
	"time"

	"chukrun/core/execution"
	"chukrun/core/telemetry"
)

type LoggingMiddleware struct {
	logger telemetry.Logger
}

func NewLoggingMiddleware(logger telemetry.Logger) *LoggingMiddleware {
	return &LoggingMiddleware{logger: logger}
}

func (m *LoggingMiddleware) Name() string               { return "Logging" }
func (m *LoggingMiddleware) Dependencies() []Capability { return nil }
func (m *LoggingMiddleware) Provides() []Capability     { return nil }
func (m *LoggingMiddleware) FailureMode() FailureMode   { return FailOpen }

func (m *LoggingMiddleware) Handle(ctx context.Context, req *execution.ExecutionRequest, next execution.Handler) (*execution.ExecutionResult, error) {
	start := time.Now()
	if m.logger != nil {
		m.logger.Info(ctx, fmt.Sprintf("middleware: starting execution request %s", req.ID))
	}

	res, err := next(ctx, req)

	if m.logger != nil {
		duration := time.Since(start)
		status := "Unknown"
		if res != nil {
			status = string(res.Status)
		}
		m.logger.Info(ctx, fmt.Sprintf("middleware: finished execution request %s in %v with status %s", req.ID, duration, status))
	}

	return res, err
}
