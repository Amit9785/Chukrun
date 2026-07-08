package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type Field struct {
	Key   string
	Value any
}

// LogLevel represents the severity of logs
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

// Logger defines structured, leveled logging interface
type Logger interface {
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)
	WithContext(ctx context.Context) Logger
	WithFields(fields ...Field) Logger
}

type JSONLogger struct {
	mu            sync.Mutex
	level         LogLevel
	fields        []Field
	redactFields  map[string]bool
	systemContext context.Context
}

func NewJSONLogger(levelName string) *JSONLogger {
	level := ParseLevel(levelName)
	return &JSONLogger{
		level: level,
		redactFields: map[string]bool{
			"api_key":       true,
			"secret":        true,
			"password":      true,
			"authorization": true,
		},
	}
}

func (jl *JSONLogger) clone() *JSONLogger {
	newFields := make([]Field, len(jl.fields))
	copy(newFields, jl.fields)
	return &JSONLogger{
		level:         jl.level,
		fields:        newFields,
		redactFields:  jl.redactFields,
		systemContext: jl.systemContext,
	}
}

func (jl *JSONLogger) WithContext(ctx context.Context) Logger {
	if ctx == nil {
		return jl
	}
	cloned := jl.clone()
	cloned.systemContext = ctx

	// Extract standard context correlation fields if present
	if traceID, ok := ctx.Value("trace_id").(string); ok && traceID != "" {
		cloned.fields = append(cloned.fields, Field{Key: "trace_id", Value: traceID})
	}
	if sessionID, ok := ctx.Value("session_id").(string); ok && sessionID != "" {
		cloned.fields = append(cloned.fields, Field{Key: "session_id", Value: sessionID})
	}
	if userID, ok := ctx.Value("user_id").(string); ok && userID != "" {
		cloned.fields = append(cloned.fields, Field{Key: "user_id", Value: userID})
	}
	return cloned
}

func (jl *JSONLogger) WithFields(fields ...Field) Logger {
	cloned := jl.clone()
	cloned.fields = append(cloned.fields, fields...)
	return cloned
}

func (jl *JSONLogger) log(level LogLevel, msg string, fields ...Field) {
	if level < jl.level {
		return
	}

	entry := make(map[string]any)
	entry["timestamp"] = time.Now().Format(time.RFC3339Nano)
	entry["level"] = level.String()
	entry["message"] = msg

	// Add contextual and direct fields, redacting sensitive ones
	allFields := append(jl.fields, fields...)
	for _, f := range allFields {
		keyLower := strings.ToLower(f.Key)
		if jl.redactFields[keyLower] || strings.Contains(keyLower, "key") || strings.Contains(keyLower, "secret") {
			entry[f.Key] = "[REDACTED]"
		} else {
			entry[f.Key] = f.Value
		}
	}

	jl.mu.Lock()
	defer jl.mu.Unlock()

	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, `{"timestamp":"%s","level":"ERROR","message":"failed to marshal log: %v"}`+"\n",
			time.Now().Format(time.RFC3339), err)
		return
	}

	fmt.Fprintln(os.Stdout, string(data))
	if level == LevelFatal {
		os.Exit(1)
	}
}

func (jl *JSONLogger) Debug(msg string, fields ...Field) {
	jl.log(LevelDebug, msg, fields...)
}

func (jl *JSONLogger) Info(msg string, fields ...Field) {
	jl.log(LevelInfo, msg, fields...)
}

func (jl *JSONLogger) Warn(msg string, fields ...Field) {
	jl.log(LevelWarn, msg, fields...)
}

func (jl *JSONLogger) Error(msg string, fields ...Field) {
	jl.log(LevelError, msg, fields...)
}

func (jl *JSONLogger) Fatal(msg string, fields ...Field) {
	jl.log(LevelFatal, msg, fields...)
}
