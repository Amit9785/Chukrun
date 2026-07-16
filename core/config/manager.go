package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"chukrun/core/errors"

	"gopkg.in/yaml.v3"
)

type ConcurrencyConfig struct {
	GlobalLimit int `json:"global_limit" yaml:"global_limit"`
	QueueSize   int `json:"queue_size" yaml:"queue_size"`
}

type RuntimeConfig struct {
	Environment       string            `json:"environment" yaml:"environment"`
	LogLevel          string            `json:"log_level" yaml:"log_level"`
	StartupTimeoutMS  int               `json:"startup_timeout_ms" yaml:"startup_timeout_ms"`
	ShutdownTimeoutMS int               `json:"shutdown_timeout_ms" yaml:"shutdown_timeout_ms"`
	Concurrency       ConcurrencyConfig `json:"concurrency" yaml:"concurrency"`
}

type ProviderConfig struct {
	Name      string `json:"name" yaml:"name"`
	Type      string `json:"type" yaml:"type"`
	APIKey    string `json:"api_key" yaml:"api_key"`
	TimeoutMS int    `json:"timeout_ms" yaml:"timeout_ms"`
}

type MiddlewareConfig struct {
	Order []string `json:"order" yaml:"order"`
}

type LifecycleConfig struct {
	RestartCooldownMS    int  `json:"restart_cooldown_ms" yaml:"restart_cooldown_ms"`
	AcceptWhenDegraded   bool `json:"accept_when_degraded" yaml:"accept_when_degraded"`
	ExposeInternalErrors bool `json:"expose_internal_errors" yaml:"expose_internal_errors"`
}

type LoggingRedactionConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

type LoggingConfig struct {
	Level        string                 `json:"level" yaml:"level"`
	Format       string                 `json:"format" yaml:"format"` // json | human
	Sinks        []string               `json:"sinks" yaml:"sinks"`   // stdout | file | otlp
	FilePath     string                 `json:"file_path" yaml:"file_path"`
	OTLPEndpoint string                 `json:"otlp_endpoint" yaml:"otlp_endpoint"`
	Redaction    LoggingRedactionConfig `json:"redaction" yaml:"redaction"`
}

type TracingSamplingConfig struct {
	DefaultRate       float64            `json:"default_rate" yaml:"default_rate"`
	PriorityOverrides map[string]float64 `json:"priority_overrides" yaml:"priority_overrides"`
}

type TracingConfig struct {
	Exporter string                `json:"exporter" yaml:"exporter"`
	Endpoint string                `json:"endpoint" yaml:"endpoint"`
	Sampling TracingSamplingConfig `json:"sampling" yaml:"sampling"`
}

type MetricsConfig struct {
	Exporter string `json:"exporter" yaml:"exporter"`
	Endpoint string `json:"endpoint" yaml:"endpoint"`
}

type TelemetryConfig struct {
	Metrics MetricsConfig `json:"metrics" yaml:"metrics"`
	Tracing TracingConfig `json:"tracing" yaml:"tracing"`
}

type Config struct {
	Runtime                            RuntimeConfig    `json:"runtime" yaml:"runtime"`
	Lifecycle                          LifecycleConfig  `json:"lifecycle" yaml:"lifecycle"`
	Providers                          []ProviderConfig `json:"providers" yaml:"providers"`
	Middleware                         MiddlewareConfig `json:"middleware" yaml:"middleware"`
	Logging                            LoggingConfig    `json:"logging" yaml:"logging"`
	Telemetry                          TelemetryConfig  `json:"telemetry" yaml:"telemetry"`
	DebugMode                          bool             `json:"debug_mode" yaml:"debug_mode"`
	DebugModeAcknowledgeProductionRisk bool             `json:"debug_mode_acknowledge_production_risk" yaml:"debug_mode_acknowledge_production_risk"`
}

// GetDefaultConfig returns the baseline default configuration
func GetDefaultConfig() *Config {
	return &Config{
		Runtime: RuntimeConfig{
			Environment:       "development",
			LogLevel:          "info",
			StartupTimeoutMS:  10000,
			ShutdownTimeoutMS: 15000,
			Concurrency: ConcurrencyConfig{
				GlobalLimit: 10000,
				QueueSize:   5000,
			},
		},
		Lifecycle: LifecycleConfig{
			RestartCooldownMS:    1000,
			AcceptWhenDegraded:   true,
			ExposeInternalErrors: false,
		},
		Middleware: MiddlewareConfig{
			Order: []string{"logging", "metrics"},
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "json",
			Sinks:  []string{"stdout"},
			Redaction: LoggingRedactionConfig{
				Enabled: true,
			},
		},
		Telemetry: TelemetryConfig{
			Metrics: MetricsConfig{
				Exporter: "prometheus",
				Endpoint: ":9090",
			},
			Tracing: TracingConfig{
				Exporter: "otlp",
				Endpoint: "https://collector.internal:4317",
				Sampling: TracingSamplingConfig{
					DefaultRate: 0.10,
					PriorityOverrides: map[string]float64{
						"critical": 1.0,
						"high":     0.5,
					},
				},
			},
		},
		DebugMode:                          false,
		DebugModeAcknowledgeProductionRisk: false,
	}
}

var secretRegex = regexp.MustCompile(`\$\{secret:([^}]+)\}`)

// InterpolateSecrets replaces ${secret:KEY} placeholders with the corresponding env var value.
func InterpolateSecrets(s string) string {
	return secretRegex.ReplaceAllStringFunc(s, func(match string) string {
		submatches := secretRegex.FindStringSubmatch(match)
		if len(submatches) > 1 {
			envVal := os.Getenv(submatches[1])
			if envVal != "" {
				return envVal
			}
		}
		return match
	})
}

// LoadConfig loads, overrides, and validates the configuration
func LoadConfig(filePath string, codeOverrides *Config) (*Config, error) {
	cfg := GetDefaultConfig()

	// 1. Load from file if specified
	if err := loadFromFile(filePath, cfg); err != nil {
		return nil, err
	}

	// 2. Override from environment variables
	overrideFromEnv(cfg)

	// 3. Apply explicit code overrides
	applyCodeOverrides(cfg, codeOverrides)

	// 4. Interpolate secrets in provider configurations
	for i := range cfg.Providers {
		cfg.Providers[i].APIKey = InterpolateSecrets(cfg.Providers[i].APIKey)
	}

	// 5. Validate config
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func loadFromFile(filePath string, cfg *Config) error {
	if filePath == "" {
		return nil
	}
	file, err := os.Open(filePath)
	if err != nil {
		return errors.NewError(errors.ErrCategoryConfig, fmt.Sprintf("failed to open config file: %s", filePath), false, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return errors.NewError(errors.ErrCategoryConfig, "failed to read config file", false, err)
	}

	if strings.HasSuffix(filePath, ".yaml") || strings.HasSuffix(filePath, ".yml") {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return errors.NewError(errors.ErrCategoryConfig, "failed to parse YAML config file", false, err)
		}
	} else {
		if err := json.Unmarshal(data, cfg); err != nil {
			return errors.NewError(errors.ErrCategoryConfig, "failed to parse JSON config file", false, err)
		}
	}
	return nil
}

func getEnvInt(key string) (int, bool) {
	val := os.Getenv(key)
	if val == "" {
		return 0, false
	}
	if i, err := strconv.Atoi(val); err == nil {
		return i, true
	}
	return 0, false
}

func overrideFromEnv(cfg *Config) {
	if val := os.Getenv("RUNTIME_ENVIRONMENT"); val != "" {
		cfg.Runtime.Environment = val
	}
	if val := os.Getenv("RUNTIME_LOG_LEVEL"); val != "" {
		cfg.Runtime.LogLevel = val
	}
	if val, ok := getEnvInt("RUNTIME_STARTUP_TIMEOUT"); ok {
		cfg.Runtime.StartupTimeoutMS = val
	}
	if val, ok := getEnvInt("RUNTIME_SHUTDOWN_TIMEOUT"); ok {
		cfg.Runtime.ShutdownTimeoutMS = val
	}
	if val, ok := getEnvInt("RUNTIME_GLOBAL_LIMIT"); ok {
		cfg.Runtime.Concurrency.GlobalLimit = val
	}
	if val, ok := getEnvInt("RUNTIME_QUEUE_SIZE"); ok {
		cfg.Runtime.Concurrency.QueueSize = val
	}
	if val, ok := getEnvInt("LIFECYCLE_RESTART_COOLDOWN"); ok {
		cfg.Lifecycle.RestartCooldownMS = val
	}
	if val := os.Getenv("LIFECYCLE_ACCEPT_WHEN_DEGRADED"); val != "" {
		cfg.Lifecycle.AcceptWhenDegraded = (strings.ToLower(val) == "true")
	}
	if val := os.Getenv("LIFECYCLE_EXPOSE_INTERNAL_ERRORS"); val != "" {
		cfg.Lifecycle.ExposeInternalErrors = (strings.ToLower(val) == "true")
	}
	if val := os.Getenv("RUNTIME_DEBUG_MODE"); val != "" {
		cfg.DebugMode = (strings.ToLower(val) == "true")
	}
	if val := os.Getenv("RUNTIME_DEBUG_MODE_ACKNOWLEDGE_PRODUCTION_RISK"); val != "" {
		cfg.DebugModeAcknowledgeProductionRisk = (strings.ToLower(val) == "true")
	}
}

func applyCodeOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides == nil {
		return
	}
	applyRuntimeOverrides(cfg, codeOverrides)
	applyLifecycleOverrides(cfg, codeOverrides)
	applyLoggingOverrides(cfg, codeOverrides)
	applyTelemetryOverrides(cfg, codeOverrides)
	cfg.DebugMode = codeOverrides.DebugMode
	cfg.DebugModeAcknowledgeProductionRisk = codeOverrides.DebugModeAcknowledgeProductionRisk
}

func applyRuntimeOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides.Runtime.Environment != "" {
		cfg.Runtime.Environment = codeOverrides.Runtime.Environment
	}
	if codeOverrides.Runtime.LogLevel != "" {
		cfg.Runtime.LogLevel = codeOverrides.Runtime.LogLevel
	}
	if codeOverrides.Runtime.StartupTimeoutMS != 0 {
		cfg.Runtime.StartupTimeoutMS = codeOverrides.Runtime.StartupTimeoutMS
	}
	if codeOverrides.Runtime.ShutdownTimeoutMS != 0 {
		cfg.Runtime.ShutdownTimeoutMS = codeOverrides.Runtime.ShutdownTimeoutMS
	}
	if codeOverrides.Runtime.Concurrency.GlobalLimit != 0 {
		cfg.Runtime.Concurrency.GlobalLimit = codeOverrides.Runtime.Concurrency.GlobalLimit
	}
	if codeOverrides.Runtime.Concurrency.QueueSize != 0 {
		cfg.Runtime.Concurrency.QueueSize = codeOverrides.Runtime.Concurrency.QueueSize
	}
}

func applyLifecycleOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides.Lifecycle != (LifecycleConfig{}) {
		if codeOverrides.Lifecycle.RestartCooldownMS != 0 {
			cfg.Lifecycle.RestartCooldownMS = codeOverrides.Lifecycle.RestartCooldownMS
		}
		cfg.Lifecycle.AcceptWhenDegraded = codeOverrides.Lifecycle.AcceptWhenDegraded
		cfg.Lifecycle.ExposeInternalErrors = codeOverrides.Lifecycle.ExposeInternalErrors
	}
	if len(codeOverrides.Providers) > 0 {
		cfg.Providers = codeOverrides.Providers
	}
	if len(codeOverrides.Middleware.Order) > 0 {
		cfg.Middleware.Order = codeOverrides.Middleware.Order
	}
}

func applyLoggingOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides.Logging.Level != "" {
		cfg.Logging.Level = codeOverrides.Logging.Level
	}
	if codeOverrides.Logging.Format != "" {
		cfg.Logging.Format = codeOverrides.Logging.Format
	}
	if len(codeOverrides.Logging.Sinks) > 0 {
		cfg.Logging.Sinks = codeOverrides.Logging.Sinks
	}
	if codeOverrides.Logging.FilePath != "" {
		cfg.Logging.FilePath = codeOverrides.Logging.FilePath
	}
	if codeOverrides.Logging.OTLPEndpoint != "" {
		cfg.Logging.OTLPEndpoint = codeOverrides.Logging.OTLPEndpoint
	}
	cfg.Logging.Redaction.Enabled = codeOverrides.Logging.Redaction.Enabled
}

func applyTelemetryOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides.Telemetry.Metrics.Exporter != "" {
		cfg.Telemetry.Metrics.Exporter = codeOverrides.Telemetry.Metrics.Exporter
	}
	if codeOverrides.Telemetry.Metrics.Endpoint != "" {
		cfg.Telemetry.Metrics.Endpoint = codeOverrides.Telemetry.Metrics.Endpoint
	}
	if codeOverrides.Telemetry.Tracing.Exporter != "" {
		cfg.Telemetry.Tracing.Exporter = codeOverrides.Telemetry.Tracing.Exporter
	}
	if codeOverrides.Telemetry.Tracing.Endpoint != "" {
		cfg.Telemetry.Tracing.Endpoint = codeOverrides.Telemetry.Tracing.Endpoint
	}
	if codeOverrides.Telemetry.Tracing.Sampling.DefaultRate != 0 {
		cfg.Telemetry.Tracing.Sampling.DefaultRate = codeOverrides.Telemetry.Tracing.Sampling.DefaultRate
	}
	if len(codeOverrides.Telemetry.Tracing.Sampling.PriorityOverrides) > 0 {
		cfg.Telemetry.Tracing.Sampling.PriorityOverrides = codeOverrides.Telemetry.Tracing.Sampling.PriorityOverrides
	}
}

// Validate validates configuration and returns aggregated errors if any
func (c *Config) Validate() error {
	var errs []string

	errs = c.validateRuntime(errs)
	errs = c.validateLifecycle(errs)
	errs = c.validateProviders(errs)
	errs = c.validateDebugMode(errs)

	if len(errs) > 0 {
		return errors.NewError(
			errors.ErrCategoryConfig,
			fmt.Sprintf("configuration validation failed: %s", strings.Join(errs, "; ")),
			false,
			nil,
		)
	}

	return nil
}

func (c *Config) validateRuntime(errs []string) []string {
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true, "fatal": true}
	if !validLevels[strings.ToLower(c.Runtime.LogLevel)] {
		errs = append(errs, fmt.Sprintf("invalid log_level: %q", c.Runtime.LogLevel))
	}
	if c.Runtime.StartupTimeoutMS <= 0 {
		errs = append(errs, fmt.Sprintf("startup_timeout_ms must be positive, got: %d", c.Runtime.StartupTimeoutMS))
	}
	if c.Runtime.ShutdownTimeoutMS <= 0 {
		errs = append(errs, fmt.Sprintf("shutdown_timeout_ms must be positive, got: %d", c.Runtime.ShutdownTimeoutMS))
	}
	if c.Runtime.Concurrency.GlobalLimit <= 0 {
		errs = append(errs, fmt.Sprintf("global_limit must be positive, got: %d", c.Runtime.Concurrency.GlobalLimit))
	}
	if c.Runtime.Concurrency.QueueSize < 0 {
		errs = append(errs, fmt.Sprintf("queue_size must be non-negative, got: %d", c.Runtime.Concurrency.QueueSize))
	}
	return errs
}

func (c *Config) validateLifecycle(errs []string) []string {
	if c.Lifecycle.RestartCooldownMS <= 0 {
		errs = append(errs, fmt.Sprintf("restart_cooldown_ms must be positive, got: %d", c.Lifecycle.RestartCooldownMS))
	}
	return errs
}

func (c *Config) validateProviders(errs []string) []string {
	for i, p := range c.Providers {
		if p.Name == "" {
			errs = append(errs, fmt.Sprintf("provider[%d].name cannot be empty", i))
		}
		if p.Type == "" {
			errs = append(errs, fmt.Sprintf("provider[%d].type cannot be empty", i))
		}
		if secretRegex.MatchString(p.APIKey) {
			errs = append(errs, fmt.Sprintf("provider[%d] API key secret placeholder %q could not be resolved", i, p.APIKey))
		}
	}
	return errs
}

func (c *Config) validateDebugMode(errs []string) []string {
	if c.DebugMode && strings.ToLower(c.Runtime.Environment) == "production" && !c.DebugModeAcknowledgeProductionRisk {
		errs = append(errs, "debug mode is enabled in production environment without acknowledgment of risk (debug_mode_acknowledge_production_risk: true)")
	}
	return errs
}
