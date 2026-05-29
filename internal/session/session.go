// Package session holds per-connection state for the gateway access layer:
// a stable connection id and the authenticated player binding established on
// the first AuthRequest. It is safe for concurrent use because multiple
// per-request goroutines on one connection read the player binding.
package session

import (
	"sync"
	"time"
)

// Session is the state bound to a single client connection.
type Session struct {
	ID         string
	RemoteAddr string
	CreatedAt  time.Time

	mu       sync.RWMutex
	playerID string
	authed   bool
}

// New creates a session for a freshly accepted connection.
func New(id, remoteAddr string) *Session {
	return &Session{
		ID:         id,
		RemoteAddr: remoteAddr,
		CreatedAt:  time.Now(),
	}
}

// Bind records the authenticated player and marks the session authenticated.
func (s *Session) Bind(playerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.playerID = playerID
	s.authed = true
}

// PlayerID returns the bound player id (empty until authenticated).
func (s *Session) PlayerID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.playerID
}

// Authenticated reports whether the session has completed authentication.
func (s *Session) Authenticated() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.authed
}
