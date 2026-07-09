package observability

import (
	"context"
	"testing"
)

func TestJSONLoggerLeveledLogging(t *testing.T) {
	log := NewJSONLogger("debug")

	if LogLevel(LevelDebug).String() != "DEBUG" {
		t.Errorf("expected level string DEBUG, got: %s", LogLevel(LevelDebug).String())
	}
	if LogLevel(LevelInfo).String() != "INFO" {
		t.Errorf("expected level string INFO, got: %s", LogLevel(LevelInfo).String())
	}
	if LogLevel(LevelWarn).String() != "WARN" {
		t.Errorf("expected level string WARN, got: %s", LogLevel(LevelWarn).String())
	}
	if LogLevel(LevelError).String() != "ERROR" {
		t.Errorf("expected level string ERROR, got: %s", LogLevel(LevelError).String())
	}
	if LogLevel(LevelFatal).String() != "FATAL" {
		t.Errorf("expected level string FATAL, got: %s", LogLevel(LevelFatal).String())
	}
	if LogLevel(-1).String() != "INFO" {
		t.Errorf("expected default level string INFO, got: %s", LogLevel(-1).String())
	}

	if ParseLevel("debug") != LevelDebug {
		t.Error("expected debug level")
	}
	if ParseLevel("WARN") != LevelWarn {
		t.Error("expected warn level")
	}
	if ParseLevel("ERROR") != LevelError {
		t.Error("expected error level")
	}
	if ParseLevel("FATAL") != LevelFatal {
		t.Error("expected fatal level")
	}
	if ParseLevel("unknown") != LevelInfo {
		t.Error("expected default level to be info")
	}

	// Test output logging methods (Verify no panics)
	log.Debug("test debug", Field{Key: "k1", Value: "v1"})
	log.Info("test info", Field{Key: "api_key", Value: "secret_value"}) // test redaction
	log.Warn("test warn")
	log.Error("test error")

	// Test clone and field appending
	logWithFields := log.WithFields(Field{Key: "global_field", Value: 42})
	logWithFields.Info("test with fields")

	// Test context lookup
	var (
		compatTraceID   any = "trace_id"
		compatSessionID any = "session_id"
		compatUserID    any = "user_id"
	)
	ctx := context.Background()
	ctx = context.WithValue(ctx, compatTraceID, "tr-xyz")
	ctx = context.WithValue(ctx, compatSessionID, "sess-xyz")
	ctx = context.WithValue(ctx, compatUserID, "user-xyz")

	logWithCtx := log.WithContext(ctx)
	logWithCtx.Info("test with context correlation")

	// Test nil context safety
	var nilCtx context.Context
	logWithNilCtx := log.WithContext(nilCtx)
	logWithNilCtx.Info("test with nil context")
}
