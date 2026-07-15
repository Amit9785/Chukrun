package observability

import (
	stdcontext "context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rtcontext "chukrun/runtime/context"
	"chukrun/runtime/kernel"
)

type Field struct {
	Key         string
	Value       any
	IsSensitive bool
	IsError     bool
}

func SensitiveField(key string, value any) Field {
	return Field{Key: key, Value: value, IsSensitive: true}
}

func ErrorField(err error) Field {
	return Field{Key: "error", Value: err, IsError: true}
}

// LogLevel represents severity of logs
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func (l LogLevel) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "INFO"
	}
}

func ParseLevel(s string) LogLevel {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "warn":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		return LevelInfo
	}
}

// Logger defines structured, leveled logging interface matching PRD-1.9 §22
type Logger interface {
	Debug(ctx stdcontext.Context, msg string, fields ...Field)
	Info(ctx stdcontext.Context, msg string, fields ...Field)
	Warn(ctx stdcontext.Context, msg string, fields ...Field)
	Error(ctx stdcontext.Context, msg string, fields ...Field)
	Fatal(ctx stdcontext.Context, msg string, fields ...Field)
	With(fields ...Field) Logger
}

// LogEntry represents structured log line data
type LogEntry struct {
	Timestamp time.Time
	Level     LogLevel
	Message   string
	Fields    []Field
}

// LogSink is the interface for log destinations
type LogSink interface {
	Write(ctx stdcontext.Context, entry LogEntry) error
	Close() error
}

// LogSinkRegistry acts as a thread-safe registry and asynchronous logging channel
type LogSinkRegistry struct {
	mu         sync.RWMutex
	sinks      []LogSink
	queue      chan LogEntry
	queueSize  int
	dropped    uint64
	closeCh    chan struct{}
	wg         sync.WaitGroup
	formatJSON bool
}

var globalRegistry = &LogSinkRegistry{
	queue:      make(chan LogEntry, 5000),
	queueSize:  5000,
	closeCh:    make(chan struct{}),
	formatJSON: true,
}

func init() {
	globalRegistry.wg.Add(1)
	go globalRegistry.processQueue()
}

func RegisterLogSink(sink LogSink) error {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.sinks = append(globalRegistry.sinks, sink)
	return nil
}

func ClearLogSinks() {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	for _, s := range globalRegistry.sinks {
		s.Close()
	}
	globalRegistry.sinks = nil
}

func (r *LogSinkRegistry) processQueue() {
	defer r.wg.Done()
	for {
		select {
		case entry, ok := <-r.queue:
			if !ok {
				return
			}
			r.mu.RLock()
			for _, sink := range r.sinks {
				_ = sink.Write(stdcontext.Background(), entry)
			}
			r.mu.RUnlock()
		case <-r.closeCh:
			close(r.queue)
			for entry := range r.queue {
				r.mu.RLock()
				for _, sink := range r.sinks {
					_ = sink.Write(stdcontext.Background(), entry)
				}
				r.mu.RUnlock()
			}
			return
		}
	}
}

func ShutdownLogging() {
	close(globalRegistry.closeCh)
	globalRegistry.wg.Wait()
}

// Global active log level (hot-reloadable)
var currentLogLevel int32 = int32(LevelInfo)

func SetGlobalLogLevel(level LogLevel) {
	atomic.StoreInt32(&currentLogLevel, int32(level))
}

func GetGlobalLogLevel() LogLevel {
	return LogLevel(atomic.LoadInt32(&currentLogLevel))
}

// PlatformLogger implementation
type PlatformLogger struct {
	fields []Field
}

func NewPlatformLogger() *PlatformLogger {
	return &PlatformLogger{
		fields: make([]Field, 0),
	}
}

func (l *PlatformLogger) clone() *PlatformLogger {
	newFields := make([]Field, len(l.fields))
	copy(newFields, l.fields)
	return &PlatformLogger{
		fields: newFields,
	}
}

func (l *PlatformLogger) With(fields ...Field) Logger {
	cloned := l.clone()
	cloned.fields = append(cloned.fields, fields...)
	return cloned
}

func (l *PlatformLogger) log(ctx stdcontext.Context, level LogLevel, msg string, fields ...Field) {
	if level < GetGlobalLogLevel() {
		return
	}

	// 1. Context Enrichment
	allFields := enrich(ctx, l.fields)
	allFields = append(allFields, fields...)

	// 2. Error expansion
	allFields = expandErrors(allFields)

	// 3. Redaction
	allFields = redact(ctx, allFields)

	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   msg,
		Fields:    allFields,
	}

	// 4. Submit to async queue with backpressure
	select {
	case globalRegistry.queue <- entry:
		// successfully enqueued
	default:
		// saturated: apply dropping policy
		if level == LevelDebug || level == LevelInfo {
			atomic.AddUint64(&globalRegistry.dropped, 1)
		} else {
			select {
			case <-globalRegistry.queue:
				atomic.AddUint64(&globalRegistry.dropped, 1)
			default:
			}
			select {
			case globalRegistry.queue <- entry:
			default:
				fmt.Fprintf(os.Stderr, `{"timestamp":"%s","level":"ERROR","message":"logging queue saturated, dropping log"}`+"\n",
					time.Now().Format(time.RFC3339))
			}
		}
	}

	if level == LevelFatal {
		ShutdownLogging()
		os.Exit(1)
	}
}

func (l *PlatformLogger) Debug(ctx stdcontext.Context, msg string, fields ...Field) {
	l.log(ctx, LevelDebug, msg, fields...)
}

func (l *PlatformLogger) Info(ctx stdcontext.Context, msg string, fields ...Field) {
	l.log(ctx, LevelInfo, msg, fields...)
}

func (l *PlatformLogger) Warn(ctx stdcontext.Context, msg string, fields ...Field) {
	l.log(ctx, LevelWarn, msg, fields...)
}

func (l *PlatformLogger) Error(ctx stdcontext.Context, msg string, fields ...Field) {
	l.log(ctx, LevelError, msg, fields...)
}

func (l *PlatformLogger) Fatal(ctx stdcontext.Context, msg string, fields ...Field) {
	l.log(ctx, LevelFatal, msg, fields...)
}

// Helpers
func enrich(ctx stdcontext.Context, entryFields []Field) []Field {
	if ctx == nil {
		return entryFields
	}

	enriched := make([]Field, 0, len(entryFields)+6)

	enriched = addEnrichedField(enriched, entryFields, "trace_id", rtcontext.GetTraceID(ctx))
	enriched = addEnrichedField(enriched, entryFields, "session_id", rtcontext.GetSessionID(ctx))
	enriched = addEnrichedField(enriched, entryFields, "execution_id", rtcontext.GetExecutionID(ctx))

	if !hasField(entryFields, "attempt_number") {
		attempt := rtcontext.GetAttemptNumber(ctx)
		enriched = append(enriched, Field{Key: "attempt_number", Value: attempt})
	}

	enriched = addEnrichedField(enriched, entryFields, "user_id", rtcontext.GetUserID(ctx))

	if !hasField(entryFields, "tenant_id") {
		if ul := rtcontext.GetUserLayer(ctx); ul != nil && ul.OrgID != "" {
			enriched = append(enriched, Field{Key: "tenant_id", Value: ul.OrgID})
		}
	}

	enriched = append(enriched, entryFields...)
	return enriched
}

func hasField(fields []Field, key string) bool {
	for _, f := range fields {
		if f.Key == key {
			return true
		}
	}
	return false
}

func addEnrichedField(enriched []Field, entryFields []Field, key string, val string) []Field {
	if hasField(entryFields, key) {
		return enriched
	}
	if val != "" {
		return append(enriched, Field{Key: key, Value: val})
	}
	return enriched
}

func expandErrors(fields []Field) []Field {
	expanded := make([]Field, 0, len(fields))
	for _, f := range fields {
		if (f.IsError || f.Key == "error") && f.Value != nil {
			if err, ok := f.Value.(error); ok {
				expanded = append(expanded, expandSingleError(err)...)
				continue
			}
		}
		expanded = append(expanded, f)
	}
	return expanded
}

func expandSingleError(err error) []Field {
	if platErr, ok := err.(*kernel.PlatformError); ok && platErr != nil {
		res := []Field{
			{Key: "error.category", Value: string(platErr.Category)},
			{Key: "error.retryable", Value: platErr.Retryable},
			{Key: "error.message", Value: platErr.Message},
		}
		if platErr.Cause != nil {
			res = append(res, Field{Key: "error.cause", Value: platErr.Cause.Error()})
		}
		return res
	}
	return []Field{
		{Key: "error.category", Value: "internal"},
		{Key: "error.retryable", Value: false},
		{Key: "error.message", Value: err.Error()},
	}
}

func redact(ctx stdcontext.Context, fields []Field) []Field {
	redacted := make([]Field, len(fields))
	for i, f := range fields {
		keyLower := strings.ToLower(f.Key)
		isSens := f.IsSensitive ||
			keyLower == "api_key" || keyLower == "secret" || keyLower == "password" || keyLower == "authorization" ||
			strings.Contains(keyLower, "key") || strings.Contains(keyLower, "secret") ||
			rtcontext.IsSensitiveKey(ctx, f.Key)

		if isSens {
			redacted[i] = Field{Key: f.Key, Value: "[REDACTED]", IsSensitive: true}
		} else {
			redacted[i] = f
		}
	}
	return redacted
}

// Built-in standard sinks
type StdoutSink struct {
	mu     sync.Mutex
	writer io.Writer
	isJSON bool
}

func NewStdoutSink(isJSON bool) *StdoutSink {
	return &StdoutSink{
		writer: os.Stdout,
		isJSON: isJSON,
	}
}

func (s *StdoutSink) Write(ctx stdcontext.Context, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isJSON {
		m := make(map[string]any)
		m["timestamp"] = entry.Timestamp.Format(time.RFC3339Nano)
		m["level"] = entry.Level.String()
		m["message"] = entry.Message
		for _, f := range entry.Fields {
			m[f.Key] = f.Value
		}
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		fmt.Fprintln(s.writer, string(data))
	} else {
		var color, reset string
		if os.Getenv("TERM") != "dumb" {
			reset = "\033[0m"
			switch entry.Level {
			case LevelDebug:
				color = "\033[36m"
			case LevelInfo:
				color = "\033[32m"
			case LevelWarn:
				color = "\033[33m"
			case LevelError:
				color = "\033[31m"
			case LevelFatal:
				color = "\033[35m"
			}
		}
		var fieldsStr []string
		for _, f := range entry.Fields {
			fieldsStr = append(fieldsStr, fmt.Sprintf("%s=%v", f.Key, f.Value))
		}
		fieldsPart := ""
		if len(fieldsStr) > 0 {
			fieldsPart = " {" + strings.Join(fieldsStr, ", ") + "}"
		}
		fmt.Fprintf(s.writer, "%s %s[%s]%s %s%s\n",
			entry.Timestamp.Format("2006-01-02 15:04:05.000"),
			color, entry.Level.String(), reset,
			entry.Message, fieldsPart)
	}
	return nil
}

func (s *StdoutSink) Close() error { return nil }

type FileSink struct {
	mu     sync.Mutex
	file   *os.File
	isJSON bool
}

func NewFileSink(path string, isJSON bool) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &FileSink{
		file:   f,
		isJSON: isJSON,
	}, nil
}

func (s *FileSink) Write(ctx stdcontext.Context, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isJSON {
		m := make(map[string]any)
		m["timestamp"] = entry.Timestamp.Format(time.RFC3339Nano)
		m["level"] = entry.Level.String()
		m["message"] = entry.Message
		for _, f := range entry.Fields {
			m[f.Key] = f.Value
		}
		data, err := json.Marshal(m)
		if err != nil {
			return err
		}
		fmt.Fprintln(s.file, string(data))
	} else {
		var fieldsStr []string
		for _, f := range entry.Fields {
			fieldsStr = append(fieldsStr, fmt.Sprintf("%s=%v", f.Key, f.Value))
		}
		fieldsPart := ""
		if len(fieldsStr) > 0 {
			fieldsPart = " {" + strings.Join(fieldsStr, ", ") + "}"
		}
		fmt.Fprintf(s.file, "%s [%s] %s%s\n",
			entry.Timestamp.Format("2006-01-02 15:04:05.000"),
			entry.Level.String(),
			entry.Message, fieldsPart)
	}
	return nil
}

func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

type OTLPSink struct {
	mu       sync.Mutex
	endpoint string
	records  []LogEntry
}

func NewOTLPSink(endpoint string) *OTLPSink {
	return &OTLPSink{
		endpoint: endpoint,
		records:  make([]LogEntry, 0),
	}
}

func (s *OTLPSink) Write(ctx stdcontext.Context, entry LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, entry)
	return nil
}

func (s *OTLPSink) Close() error { return nil }

func (s *OTLPSink) GetRecords() []LogEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := make([]LogEntry, len(s.records))
	copy(copied, s.records)
	return copied
}

func NewJSONLogger(levelName string) Logger {
	SetGlobalLogLevel(ParseLevel(levelName))
	return NewPlatformLogger()
}
