package runtime

import (
	"chukrun/runtime/execution"
	"chukrun/runtime/kernel"
	"chukrun/runtime/observability"
	"chukrun/runtime/provider"
)

// Option defines a functional configuration option for the runtime constructor
type Option func(*CoreRuntime)

// WithConfigFile sets a configuration file to load at startup
func WithConfigFile(path string) Option {
	return func(r *CoreRuntime) {
		r.configFile = path
	}
}

// WithConfigOverrides sets runtime settings programmatically overriding loaded ones
func WithConfigOverrides(overrides *kernel.Config) Option {
	return func(r *CoreRuntime) {
		r.overrides = overrides
	}
}

// WithLogger configures a custom Logger implementation
func WithLogger(logger observability.Logger) Option {
	return func(r *CoreRuntime) {
		r.logger = logger
	}
}

// WithTelemetry configures a custom Telemetry implementation
func WithTelemetry(telemetry observability.Telemetry) Option {
	return func(r *CoreRuntime) {
		r.telemetry = telemetry
	}
}

// WithProvider registers a provider adapter programmatically at creation time
func WithProvider(p provider.Provider) Option {
	return func(r *CoreRuntime) {
		r.providers = append(r.providers, p)
	}
}

// WithMiddleware registers a pipeline execution middleware programmatically
func WithMiddleware(m execution.Middleware) Option {
	return func(r *CoreRuntime) {
		r.middlewares = append(r.middlewares, m)
	}
}

// NewRuntime is the bootstrap constructor for creating a new Runtime instance
func NewRuntime(opts ...Option) kernel.Runtime {
	r := &CoreRuntime{
		lifecycle:   kernel.NewLifecycleManager(),
		providers:   make([]provider.Provider, 0),
		middlewares: make([]execution.Middleware, 0),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}
