package config

import (
	"os"
	"testing"

	"chukrun/core/errors"
)

func TestDefaultConfig(t *testing.T) {
	cfg := GetDefaultConfig()
	if cfg.Runtime.LogLevel != "info" {
		t.Errorf("expected default log level to be info, got: %s", cfg.Runtime.LogLevel)
	}
	if cfg.Runtime.StartupTimeoutMS != 10000 {
		t.Errorf("expected default startup timeout to be 10000, got: %d", cfg.Runtime.StartupTimeoutMS)
	}
	if cfg.Runtime.Concurrency.GlobalLimit != 10000 {
		t.Errorf("expected default global limit to be 10000, got: %d", cfg.Runtime.Concurrency.GlobalLimit)
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("expected default config validation to succeed, got: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.Runtime.LogLevel = "invalid-level"
	cfg.Runtime.StartupTimeoutMS = -10

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation to fail for invalid log level and negative timeout")
	}

	platErr, ok := err.(*errors.PlatformError)
	if !ok {
		t.Fatalf("expected error to be of type *errors.PlatformError, got: %T", err)
	}
	if platErr.Category != errors.ErrCategoryConfig {
		t.Errorf("expected error category to be config, got: %s", platErr.Category)
	}
}

func TestConfigEnvOverride(t *testing.T) {
	os.Setenv("RUNTIME_LOG_LEVEL", "debug")
	os.Setenv("RUNTIME_STARTUP_TIMEOUT", "20000")
	defer func() {
		os.Unsetenv("RUNTIME_LOG_LEVEL")
		os.Unsetenv("RUNTIME_STARTUP_TIMEOUT")
	}()

	cfg, err := LoadConfig("", nil)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Runtime.LogLevel != "debug" {
		t.Errorf("expected log level debug, got: %s", cfg.Runtime.LogLevel)
	}
	if cfg.Runtime.StartupTimeoutMS != 20000 {
		t.Errorf("expected startup timeout 20000, got: %d", cfg.Runtime.StartupTimeoutMS)
	}
}

func TestSecretInterpolation(t *testing.T) {
	os.Setenv("TEST_OPENAI_KEY", "sk-1234567890")
	defer os.Unsetenv("TEST_OPENAI_KEY")

	cfg := &Config{
		Providers: []ProviderConfig{
			{
				Name:   "openai-primary",
				Type:   "openai",
				APIKey: "${secret:TEST_OPENAI_KEY}",
			},
		},
	}

	cfgOverrides, err := LoadConfig("", cfg)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfgOverrides.Providers[0].APIKey != "sk-1234567890" {
		t.Errorf("expected key to be sk-1234567890, got: %s", cfgOverrides.Providers[0].APIKey)
	}
}

func TestConfigProductionDebugSafety(t *testing.T) {
	cfg := GetDefaultConfig()
	cfg.Runtime.Environment = "production"
	cfg.DebugMode = true
	cfg.DebugModeAcknowledgeProductionRisk = false

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation to fail when debug mode is enabled in production without acknowledgment")
	}

	cfg.DebugModeAcknowledgeProductionRisk = true
	err = cfg.Validate()
	if err != nil {
		t.Errorf("expected validation to pass with acknowledgment, got: %v", err)
	}
}

func TestConfigOverridesAndDefaults(t *testing.T) {
	cfg := GetDefaultConfig()

	overrides := &Config{}
	overrides.Logging.Level = "warn"
	overrides.Logging.Format = "json"
	overrides.Logging.Sinks = []string{"stdout", "file"}
	overrides.Logging.FilePath = "test.log"
	overrides.Logging.OTLPEndpoint = "http://localhost:4317"
	overrides.Logging.Redaction.Enabled = true

	overrides.Telemetry.Metrics.Exporter = "otlp"
	overrides.Telemetry.Metrics.Endpoint = "localhost:4317"
	overrides.Telemetry.Tracing.Exporter = "otlp"
	overrides.Telemetry.Tracing.Endpoint = "localhost:4318"
	overrides.Telemetry.Tracing.Sampling.DefaultRate = 0.5
	overrides.Telemetry.Tracing.Sampling.PriorityOverrides = map[string]float64{"critical": 1.0}

	applyCodeOverrides(cfg, overrides)

	if cfg.Logging.Level != "warn" || cfg.Logging.Format != "json" || len(cfg.Logging.Sinks) != 2 {
		t.Errorf("failed to apply logging overrides: %+v", cfg.Logging)
	}
	if cfg.Telemetry.Tracing.Sampling.DefaultRate != 0.5 {
		t.Errorf("failed to apply telemetry overrides: %+v", cfg.Telemetry.Tracing.Sampling)
	}
}
