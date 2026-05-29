package contextasm

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func staticProvider(name string, vals map[string]string) Provider {
	return ProviderFunc{
		ProviderName: name,
		FetchFn: func(context.Context, string, string) (map[string]string, error) {
			return vals, nil
		},
	}
}

func failingProvider(name string) Provider {
	return ProviderFunc{
		ProviderName: name,
		FetchFn: func(context.Context, string, string) (map[string]string, error) {
			return nil, errors.New("source unavailable")
		},
	}
}

func TestBuildAssemblesContractKeys(t *testing.T) {
	a := New(DefaultContract(), quietLogger(),
		staticProvider("player-state", map[string]string{VarPlayerLevel: "12"}),
		staticProvider("quest", map[string]string{VarCurrentQuest: "find_sword"}),
		staticProvider("affinity", map[string]string{VarAffinity: "friendly"}),
	)

	inputs, err := a.Build(context.Background(), "player-1", "npc-blacksmith")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	want := map[string]string{
		VarPlayerLevel:  "12",
		VarCurrentQuest: "find_sword",
		VarAffinity:     "friendly",
	}
	if len(inputs) != len(want) {
		t.Fatalf("inputs = %#v, want %#v", inputs, want)
	}
	for k, v := range want {
		if inputs[k] != v {
			t.Fatalf("inputs[%q] = %q, want %q", k, inputs[k], v)
		}
	}
}

// TestBuildDropsOutOfContractKeys is the core acceptance: only contract keys are
// admitted, so control variables cannot be polluted by an untrusted/injected
// key (and player input never reaches Build at all).
func TestBuildDropsOutOfContractKeys(t *testing.T) {
	a := New(DefaultContract(), quietLogger(),
		staticProvider("rogue", map[string]string{
			VarPlayerLevel:           "12",   // allowed
			"is_admin":               "true", // not in contract -> dropped
			"system_prompt_override": "x",    // not in contract -> dropped
		}),
	)

	inputs, err := a.Build(context.Background(), "player-1", "npc-1")
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if inputs[VarPlayerLevel] != "12" {
		t.Fatalf("player_level = %q, want 12", inputs[VarPlayerLevel])
	}
	if _, ok := inputs["is_admin"]; ok {
		t.Fatal("out-of-contract key is_admin leaked into inputs")
	}
	if _, ok := inputs["system_prompt_override"]; ok {
		t.Fatal("out-of-contract key system_prompt_override leaked into inputs")
	}
	if len(inputs) != 1 {
		t.Fatalf("inputs = %#v, want only player_level", inputs)
	}
}

func TestBuildDegradesOnProviderError(t *testing.T) {
	a := New(DefaultContract(), quietLogger(),
		staticProvider("player-state", map[string]string{VarPlayerLevel: "5"}),
		failingProvider("quest"), // unavailable -> skipped
		staticProvider("scene", map[string]string{VarScene: "town"}),
	)

	inputs, err := a.Build(context.Background(), "player-1", "npc-1")
	if err != nil {
		t.Fatalf("Build() error = %v, want nil (partial context)", err)
	}
	if inputs[VarPlayerLevel] != "5" || inputs[VarScene] != "town" {
		t.Fatalf("partial inputs = %#v, want player_level+scene", inputs)
	}
	if _, ok := inputs[VarCurrentQuest]; ok {
		t.Fatal("failing provider contributed a value")
	}
}

func TestBuildLaterProviderOverrides(t *testing.T) {
	a := New(DefaultContract(), quietLogger(),
		staticProvider("base", map[string]string{VarAffinity: "neutral"}),
		staticProvider("override", map[string]string{VarAffinity: "friendly"}),
	)
	inputs, _ := a.Build(context.Background(), "p", "n")
	if inputs[VarAffinity] != "friendly" {
		t.Fatalf("affinity = %q, want friendly (later provider wins)", inputs[VarAffinity])
	}
}

func TestBuildReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := New(DefaultContract(), quietLogger(),
		staticProvider("player-state", map[string]string{VarPlayerLevel: "1"}),
	)
	_, err := a.Build(ctx, "p", "n")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Build() error = %v, want context.Canceled", err)
	}
}

func TestBuildNoProvidersYieldsEmpty(t *testing.T) {
	a := New(DefaultContract(), quietLogger())
	inputs, err := a.Build(context.Background(), "p", "n")
	if err != nil || len(inputs) != 0 {
		t.Fatalf("Build() = (%#v, %v), want (empty, nil)", inputs, err)
	}
}

func TestDefaultContractKeys(t *testing.T) {
	got := DefaultContract()
	want := []string{"player_level", "current_quest", "affinity", "scene", "recent_events"}
	if len(got) != len(want) {
		t.Fatalf("DefaultContract() = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("DefaultContract()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
