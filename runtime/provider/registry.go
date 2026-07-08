package provider

import (
	"fmt"
	"sync"

	"chukrun/runtime/kernel"
)

// Registry manages thread-safe provider registrations and lookup.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

func NewRegistry() *Registry {
	return &Registry{
		providers: make(map[string]Provider),
	}
}

// Register registers a provider. Fails if name is duplicate.
func (r *Registry) Register(p Provider) error {
	if p == nil {
		return kernel.NewError(kernel.ErrCategoryValidation, "cannot register nil provider", false, nil)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	name := p.Name()
	if name == "" {
		return kernel.NewError(kernel.ErrCategoryValidation, "cannot register provider with empty name", false, nil)
	}

	if _, exists := r.providers[name]; exists {
		return kernel.NewError(kernel.ErrCategoryValidation, fmt.Sprintf("provider already registered: %s", name), false, nil)
	}

	r.providers[name] = p
	return nil
}

// Resolve returns the provider for the given reference.
// If provider name is empty and there are providers registered, returns the first one as default/auto-route.
func (r *Registry) Resolve(name string) (Provider, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.providers) == 0 {
		return nil, kernel.NewError(kernel.ErrCategoryProvider, "no providers registered in system", false, nil)
	}

	if name == "" {
		// Auto-routing default fallback: return first provider
		for _, p := range r.providers {
			return p, nil
		}
	}

	p, exists := r.providers[name]
	if !exists {
		return nil, kernel.NewError(kernel.ErrCategoryProvider, fmt.Sprintf("provider not found: %s", name), false, nil)
	}

	return p, nil
}

// List returns names of all registered providers
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	list := make([]string, 0, len(r.providers))
	for name := range r.providers {
		list = append(list, name)
	}
	return list
}
