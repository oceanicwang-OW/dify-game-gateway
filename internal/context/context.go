// Package contextasm assembles the Dify `inputs` map from trusted game-internal
// sources, constrained to the variable contract (PDR §7.1). It lives in
// internal/context but is named contextasm to avoid shadowing the standard
// library `context` package.
//
// Player input never reaches this package: Build takes only the (player, npc)
// identity and admits only contract keys, so control variables cannot be
// polluted by player-supplied content (PDR §6.5).
package contextasm

import (
	"context"
	"log/slog"
)

// Variable-contract keys (PDR §7.1). Only these are admitted into inputs.
const (
	VarPlayerLevel  = "player_level"
	VarCurrentQuest = "current_quest"
	VarAffinity     = "affinity"
	VarScene        = "scene"
	VarRecentEvents = "recent_events"
)

// DefaultContract returns the §7.1 example variable contract.
func DefaultContract() []string {
	return []string{VarPlayerLevel, VarCurrentQuest, VarAffinity, VarScene, VarRecentEvents}
}

// ContextAssembler is the §9.1 contract.
type ContextAssembler interface {
	Build(ctx context.Context, playerID, npcID string) (map[string]string, error)
}

// Provider supplies one or more contract variables for a (player, npc) from a
// trusted game-internal source (player state, quest, affinity, scene, events).
// A failing provider degrades to partial context rather than failing the build.
type Provider interface {
	Name() string
	Fetch(ctx context.Context, playerID, npcID string) (map[string]string, error)
}

// ProviderFunc adapts a named function to Provider, convenient for wiring mock
// or inline sources during early milestones.
type ProviderFunc struct {
	ProviderName string
	FetchFn      func(ctx context.Context, playerID, npcID string) (map[string]string, error)
}

func (f ProviderFunc) Name() string { return f.ProviderName }

func (f ProviderFunc) Fetch(ctx context.Context, playerID, npcID string) (map[string]string, error) {
	return f.FetchFn(ctx, playerID, npcID)
}

// Assembler builds inputs from an ordered list of providers, admitting only
// keys present in the variable contract.
type Assembler struct {
	providers []Provider
	allowed   map[string]struct{}
	logger    *slog.Logger
}

var _ ContextAssembler = (*Assembler)(nil)

// New creates an Assembler for the given contract and providers. A nil logger
// uses slog.Default().
func New(contract []string, logger *slog.Logger, providers ...Provider) *Assembler {
	if logger == nil {
		logger = slog.Default()
	}
	allowed := make(map[string]struct{}, len(contract))
	for _, k := range contract {
		allowed[k] = struct{}{}
	}
	return &Assembler{providers: providers, allowed: allowed, logger: logger}
}

// Build assembles the inputs map for (playerID, npcID). Providers are queried in
// order; later providers override earlier values for the same key. A provider
// that returns an error is logged and skipped (partial context, not a failure).
// Any key a provider returns that is not in the contract is dropped, so neither
// a buggy source nor — by construction — player input can introduce unexpected
// or control variables. Build returns ctx.Err() if the context is cancelled.
func (a *Assembler) Build(ctx context.Context, playerID, npcID string) (map[string]string, error) {
	inputs := make(map[string]string)
	for _, p := range a.providers {
		if err := ctx.Err(); err != nil {
			return inputs, err
		}
		vals, err := p.Fetch(ctx, playerID, npcID)
		if err != nil {
			a.logger.Warn("context provider unavailable; degrading to partial context",
				"provider", p.Name(), "err", err.Error())
			continue
		}
		for k, v := range vals {
			if _, ok := a.allowed[k]; ok {
				inputs[k] = v
			} else {
				a.logger.Warn("dropping out-of-contract context variable",
					"provider", p.Name(), "key", k)
			}
		}
	}
	return inputs, nil
}
