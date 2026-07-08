package context

import (
	"sync"
	"time"
)

type SessionState interface {
	Get(key string) (any, bool)
	Set(key string, value any)
	Delete(key string)
}

type sessionState struct {
	mu   sync.RWMutex
	vars map[string]any
}

func (s *sessionState) Get(key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.vars[key]
	return val, ok
}

func (s *sessionState) Set(key string, value any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vars[key] = value
}

func (s *sessionState) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vars, key)
}

type sessionStateEntry struct {
	state      *sessionState
	lastAccess time.Time
}

type SessionStore struct {
	mu       sync.Mutex
	sessions map[string]*sessionStateEntry
	expiry   time.Duration
}

var (
	sessionStoreInstance *SessionStore
	onceStore            sync.Once
)

func GetSessionStore() *SessionStore {
	onceStore.Do(func() {
		sessionStoreInstance = &SessionStore{
			sessions: make(map[string]*sessionStateEntry),
			expiry:   30 * time.Minute,
		}
		go sessionStoreInstance.startGC()
	})
	return sessionStoreInstance
}

func (ss *SessionStore) GetOrCreate(sessionID string) SessionState {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	entry, exists := ss.sessions[sessionID]
	if !exists {
		entry = &sessionStateEntry{
			state: &sessionState{
				vars: make(map[string]any),
			},
		}
		ss.sessions[sessionID] = entry
	}
	entry.lastAccess = time.Now()
	return entry.state
}

func (ss *SessionStore) SetExpiry(d time.Duration) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.expiry = d
}

func (ss *SessionStore) startGC() {
	ticker := time.NewTicker(5 * time.Minute)
	for range ticker.C {
		ss.mu.Lock()
		now := time.Now()
		for id, entry := range ss.sessions {
			if now.Sub(entry.lastAccess) > ss.expiry {
				delete(ss.sessions, id)
			}
		}
		ss.mu.Unlock()
	}
}
