package core

import (
	"chukrun/core/config"
	"chukrun/core/execution"
	"chukrun/core/lifecycle"
	"chukrun/core/middleware"
	"chukrun/core/telemetry"
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
func WithConfigOverrides(overrides *config.Config) Option {
	return func(r *CoreRuntime) {
		r.overrides = overrides
	}
}

// WithLogger configures a custom Logger implementation
func WithLogger(logger telemetry.Logger) Option {
	return func(r *CoreRuntime) {
		r.logger = logger
	}
}

// WithTelemetry configures a custom Telemetry implementation
func WithTelemetry(tel telemetry.Telemetry) Option {
	return func(r *CoreRuntime) {
		r.telemetry = tel
	}
}

// WithProvider registers a provider adapter programmatically at creation time
func WithProvider(p execution.Provider) Option {
	return func(r *CoreRuntime) {
		r.providers = append(r.providers, p)
	}
}

// WithMiddleware registers a pipeline execution middleware programmatically
func WithMiddleware(m middleware.Middleware) Option {
	return func(r *CoreRuntime) {
		r.middlewares = append(r.middlewares, m)
	}
}

// NewRuntime is the bootstrap constructor for creating a new Runtime instance
func NewRuntime(opts ...Option) Runtime {
	r := &CoreRuntime{
		lifecycle:   lifecycle.NewLifecycleManager(),
		providers:   make([]execution.Provider, 0),
		middlewares: make([]middleware.Middleware, 0),
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}
