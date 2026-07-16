package observability

import (
	stdcontext "context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	rtcontext "chukrun/runtime/context"
	"chukrun/runtime/kernel"
)

type MockSink struct {
	mu      sync.Mutex
	records []LogEntry
}

func (s *MockSink) Write(ctx stdcontext.Context, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, entry)
	return nil
}

func (s *MockSink) Close() error { return nil }

func (s *MockSink) GetRecords() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]LogEntry, len(s.records))
	copy(copied, s.records)
	return copied
}

func setupMockSink() *MockSink {
	sink := &MockSink{records: make([]LogEntry, 0)}
	ClearLogSinks()
	RegisterLogSink(sink)
	return sink
}

func TestPlatformLoggerBasicLeveled(t *testing.T) {
	sink := setupMockSink()
	SetGlobalLogLevel(LevelDebug)

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	logger.Debug(ctx, "debug msg")
	logger.Info(ctx, "info msg")
	logger.Warn(ctx, "warn msg")
	logger.Error(ctx, "error msg")

	time.Sleep(100 * time.Millisecond)

	records := sink.GetRecords()
	if len(records) < 4 {
		t.Fatalf("expected at least 4 log records, got: %d", len(records))
	}

	if records[0].Message != "debug msg" || records[0].Level != LevelDebug {
		t.Errorf("unexpected record 0: %+v", records[0])
	}
	if records[1].Message != "info msg" || records[1].Level != LevelInfo {
		t.Errorf("unexpected record 1: %+v", records[1])
	}
}

func TestPlatformLoggerHotReloadLevel(t *testing.T) {
	sink := setupMockSink()
	SetGlobalLogLevel(LevelWarn)

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	logger.Debug(ctx, "should drop debug")
	logger.Info(ctx, "should drop info")
	logger.Warn(ctx, "should keep warn")

	time.Sleep(100 * time.Millisecond)
	records := sink.GetRecords()
	if len(records) != 1 {
		t.Errorf("expected exactly 1 warn log, got: %d", len(records))
	}
	if records[0].Message != "should keep warn" || records[0].Level != LevelWarn {
		t.Errorf("unexpected logged record: %+v", records[0])
	}
}

func TestPlatformLoggerErrorExpansion(t *testing.T) {
	sink := setupMockSink()
	SetGlobalLogLevel(LevelDebug)

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	platErr := kernel.NewError(kernel.ErrCategoryValidation, "validation error text", true, errors.New("underlying failure"))
	logger.Error(ctx, "failed request", ErrorField(platErr))

	time.Sleep(100 * time.Millisecond)
	records := sink.GetRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 error log, got: %d", len(records))
	}
	lastRecord := records[0]

	fields := make(map[string]any)
	for _, f := range lastRecord.Fields {
		fields[f.Key] = f.Value
	}

	if fields["error.category"] != "validation" {
		t.Errorf("expected error.category to be validation, got %v", fields["error.category"])
	}
	if fields["error.retryable"] != true {
		t.Errorf("expected error.retryable to be true, got %v", fields["error.retryable"])
	}
	if fields["error.message"] != "validation error text" {
		t.Errorf("expected error.message validation error text, got %v", fields["error.message"])
	}
	if fields["error.cause"] != "underlying failure" {
		t.Errorf("expected error.cause underlying failure, got %v", fields["error.cause"])
	}
}

func TestPlatformLoggerEnrichment(t *testing.T) {
	sink := setupMockSink()
	SetGlobalLogLevel(LevelDebug)

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	enrichCtx := rtcontext.WithSession(ctx, "sess-123", "user-456")
	enrichCtx = rtcontext.WithExecution(enrichCtx, "exec-789", 5*time.Second, kernel.PriorityClassHigh)

	logger.Info(enrichCtx, "enriched message")
	time.Sleep(100 * time.Millisecond)
	records := sink.GetRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 log, got: %d", len(records))
	}
	enrichedRecord := records[0]

	fields := make(map[string]any)
	for _, f := range enrichedRecord.Fields {
		fields[f.Key] = f.Value
	}

	if fields["session_id"] != "sess-123" {
		t.Errorf("expected session_id sess-123, got %v", fields["session_id"])
	}
	if fields["user_id"] != "user-456" {
		t.Errorf("expected user_id user-456, got %v", fields["user_id"])
	}
	if fields["execution_id"] != "exec-789" {
		t.Errorf("expected execution_id exec-789, got %v", fields["execution_id"])
	}
	if fields["attempt_number"] != 1 {
		t.Errorf("expected attempt_number 1, got %v", fields["attempt_number"])
	}
}

func TestPlatformLoggerRedaction(t *testing.T) {
	sink := setupMockSink()
	SetGlobalLogLevel(LevelDebug)

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	redactCtx := rtcontext.WithSensitiveVariable(ctx, "password", "my-secret-password")
	logger.Info(redactCtx, "confidential info", Field{Key: "password", Value: "my-secret-password"}, SensitiveField("token", "super-secret-token"))
	time.Sleep(100 * time.Millisecond)
	records := sink.GetRecords()
	if len(records) != 1 {
		t.Fatalf("expected 1 log, got: %d", len(records))
	}
	redactedRecord := records[0]

	fields := make(map[string]any)
	for _, f := range redactedRecord.Fields {
		fields[f.Key] = f.Value
	}

	if fields["token"] != "[REDACTED]" {
		t.Error("expected token field to be redacted")
	}
	if fields["password"] != "[REDACTED]" {
		t.Error("expected password Context field to be redacted")
	}
}

type BlockingSink struct {
	blocked chan struct{}
}

func (s *BlockingSink) Write(ctx stdcontext.Context, entry LogEntry) error {
	<-s.blocked
	return nil
}
func (s *BlockingSink) Close() error { return nil }

func TestPlatformLoggerBackpressure(t *testing.T) {
	sink := &BlockingSink{blocked: make(chan struct{})}
	ClearLogSinks()
	RegisterLogSink(sink)
	defer func() {
		close(sink.blocked)
		ClearLogSinks()
	}()

	logger := NewPlatformLogger()
	ctx := stdcontext.Background()

	for i := 0; i < 5005; i++ {
		logger.Debug(ctx, "pressure log")
	}
}

func TestStdoutSinkFormatting(t *testing.T) {
	jsonSink := NewStdoutSink(true)
	_ = jsonSink.Write(stdcontext.Background(), LogEntry{
		Timestamp: time.Now(),
		Level:     LevelInfo,
		Message:   "test message",
		Fields:    []Field{{Key: "key1", Value: "val1"}},
	})

	humanSink := NewStdoutSink(false)
	_ = humanSink.Write(stdcontext.Background(), LogEntry{
		Timestamp: time.Now(),
		Level:     LevelInfo,
		Message:   "test message",
		Fields:    []Field{{Key: "key1", Value: "val1"}},
	})
}

func TestFileAndOTLPSinks(t *testing.T) {
	tempFile, err := os.CreateTemp("", "logging_test_*.log")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tempFile.Name())
	tempFile.Close()

	fileSink, err := NewFileSink(tempFile.Name(), true)
	if err != nil {
		t.Fatalf("failed to create file sink: %v", err)
	}
	defer fileSink.Close()

	_ = fileSink.Write(stdcontext.Background(), LogEntry{
		Timestamp: time.Now(),
		Level:     LevelInfo,
		Message:   "file log",
	})

	otlpSink := NewOTLPSink("http://localhost:4317")
	defer otlpSink.Close()
	_ = otlpSink.Write(stdcontext.Background(), LogEntry{
		Timestamp: time.Now(),
		Level:     LevelInfo,
		Message:   "otlp log",
	})
	if len(otlpSink.GetRecords()) != 1 {
		t.Errorf("expected 1 record in OTLPSink")
	}
}

func TestParseLogLevel(t *testing.T) {
	if ParseLevel("debug") != LevelDebug {
		t.Error("expected LevelDebug")
	}
	if ParseLevel("info") != LevelInfo {
		t.Error("expected LevelInfo")
	}
	if ParseLevel("warn") != LevelWarn {
		t.Error("expected LevelWarn")
	}
	if ParseLevel("error") != LevelError {
		t.Error("expected LevelError")
	}
	if ParseLevel("fatal") != LevelFatal {
		t.Error("expected LevelFatal")
	}
	if ParseLevel("invalid") != LevelInfo {
		t.Error("expected LevelInfo for invalid")
	}
}
