package session

import (
	"sync"
	"testing"
)

func TestSessionStartsUnauthenticated(t *testing.T) {
	s := New("conn-1", "127.0.0.1:5000")
	if s.Authenticated() {
		t.Fatal("new session authenticated = true, want false")
	}
	if s.PlayerID() != "" {
		t.Fatalf("PlayerID = %q, want empty", s.PlayerID())
	}
}

func TestSessionBind(t *testing.T) {
	s := New("conn-1", "127.0.0.1:5000")
	s.Bind("player-42")
	if !s.Authenticated() {
		t.Fatal("Authenticated = false after Bind")
	}
	if s.PlayerID() != "player-42" {
		t.Fatalf("PlayerID = %q, want player-42", s.PlayerID())
	}
}

// TestSessionConcurrentReadDuringBind exercises the RWMutex under the race
// detector: many per-request goroutines read the binding while it is set.
func TestSessionConcurrentReadDuringBind(t *testing.T) {
	s := New("conn-1", "127.0.0.1:5000")
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.PlayerID()
			_ = s.Authenticated()
		}()
	}
	s.Bind("player-1")
	wg.Wait()
	if s.PlayerID() != "player-1" {
		t.Fatalf("PlayerID = %q", s.PlayerID())
	}
}
