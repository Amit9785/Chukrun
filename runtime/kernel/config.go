package kernel

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type ConcurrencyConfig struct {
	GlobalLimit int `json:"global_limit" yaml:"global_limit"`
	QueueSize   int `json:"queue_size" yaml:"queue_size"`
}

type RuntimeConfig struct {
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

type TelemetryConfig struct {
	Exporter string `json:"exporter" yaml:"exporter"`
	Endpoint string `json:"endpoint" yaml:"endpoint"`
}

type Config struct {
	Runtime    RuntimeConfig    `json:"runtime" yaml:"runtime"`
	Lifecycle  LifecycleConfig  `json:"lifecycle" yaml:"lifecycle"`
	Providers  []ProviderConfig `json:"providers" yaml:"providers"`
	Middleware MiddlewareConfig `json:"middleware" yaml:"middleware"`
	Telemetry  TelemetryConfig  `json:"telemetry" yaml:"telemetry"`
}

// GetDefaultConfig returns the baseline default configuration
func GetDefaultConfig() *Config {
	return &Config{
		Runtime: RuntimeConfig{
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
		Telemetry: TelemetryConfig{
			Exporter: "stdout",
		},
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
		return NewError(ErrCategoryConfig, fmt.Sprintf("failed to open config file: %s", filePath), false, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return NewError(ErrCategoryConfig, "failed to read config file", false, err)
	}

	if strings.HasSuffix(filePath, ".yaml") || strings.HasSuffix(filePath, ".yml") {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return NewError(ErrCategoryConfig, "failed to parse YAML config file", false, err)
		}
	} else {
		if err := json.Unmarshal(data, cfg); err != nil {
			return NewError(ErrCategoryConfig, "failed to parse JSON config file", false, err)
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
}

func applyCodeOverrides(cfg *Config, codeOverrides *Config) {
	if codeOverrides == nil {
		return
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
	if codeOverrides.Telemetry.Exporter != "" {
		cfg.Telemetry.Exporter = codeOverrides.Telemetry.Exporter
	}
	if codeOverrides.Telemetry.Endpoint != "" {
		cfg.Telemetry.Endpoint = codeOverrides.Telemetry.Endpoint
	}
}

// Validate validates configuration and returns aggregated errors if any
func (c *Config) Validate() error {
	var errors []string

	// Validate LogLevel
	validLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true, "fatal": true}
	if !validLevels[strings.ToLower(c.Runtime.LogLevel)] {
		errors = append(errors, fmt.Sprintf("invalid log_level: %q", c.Runtime.LogLevel))
	}

	// Validate timeouts
	if c.Runtime.StartupTimeoutMS <= 0 {
		errors = append(errors, fmt.Sprintf("startup_timeout_ms must be positive, got: %d", c.Runtime.StartupTimeoutMS))
	}
	if c.Runtime.ShutdownTimeoutMS <= 0 {
		errors = append(errors, fmt.Sprintf("shutdown_timeout_ms must be positive, got: %d", c.Runtime.ShutdownTimeoutMS))
	}

	// Validate lifecycle
	if c.Lifecycle.RestartCooldownMS <= 0 {
		errors = append(errors, fmt.Sprintf("restart_cooldown_ms must be positive, got: %d", c.Lifecycle.RestartCooldownMS))
	}

	// Validate concurrency
	if c.Runtime.Concurrency.GlobalLimit <= 0 {
		errors = append(errors, fmt.Sprintf("global_limit must be positive, got: %d", c.Runtime.Concurrency.GlobalLimit))
	}
	if c.Runtime.Concurrency.QueueSize < 0 {
		errors = append(errors, fmt.Sprintf("queue_size must be non-negative, got: %d", c.Runtime.Concurrency.QueueSize))
	}

	// Validate providers
	for i, p := range c.Providers {
		if p.Name == "" {
			errors = append(errors, fmt.Sprintf("provider[%d].name cannot be empty", i))
		}
		if p.Type == "" {
			errors = append(errors, fmt.Sprintf("provider[%d].type cannot be empty", i))
		}
		// If API key is still structured as ${secret:...}, it means it wasn't resolved
		if secretRegex.MatchString(p.APIKey) {
			errors = append(errors, fmt.Sprintf("provider[%d] API key secret placeholder %q could not be resolved", i, p.APIKey))
		}
	}

	if len(errors) > 0 {
		return NewError(
			ErrCategoryConfig,
			fmt.Sprintf("configuration validation failed: %s", strings.Join(errors, "; ")),
			false,
			nil,
		)
	}

	return nil
}
