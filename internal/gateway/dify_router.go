package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"dify_gateway/internal/dify"
)

// DifyRouter selects a Dify App API key per npc_id while preserving the
// pipeline.DifyClient interface. The npc_id is encoded in the gateway-scoped
// Dify user as "player_id:npc_id".
type DifyRouter struct {
	clients       map[string]*dify.Client
	defaultClient *dify.Client
}

func NewDifyRouter(baseURL string, appKeys map[string]string, httpClient *http.Client) (*DifyRouter, error) {
	if strings.TrimSpace(baseURL) == "" {
		return nil, fmt.Errorf("gateway: DIFY_BASE_URL is required")
	}
	if len(appKeys) == 0 {
		return nil, fmt.Errorf("gateway: at least one Dify app key is required")
	}

	clients := make(map[string]*dify.Client, len(appKeys))
	for npcID, key := range appKeys {
		npcID = strings.TrimSpace(npcID)
		key = strings.TrimSpace(key)
		if npcID == "" || key == "" {
			return nil, fmt.Errorf("gateway: Dify app key mapping contains empty name or key")
		}
		clients[npcID] = dify.NewClient(baseURL, key, httpClient)
	}

	return &DifyRouter{
		clients:       clients,
		defaultClient: clients["default"],
	}, nil
}

func (r *DifyRouter) ChatStream(ctx context.Context, req dify.ChatReq, onEvent func(taskID, convID string), onDelta func(delta string)) (dify.ChatResult, error) {
	client, err := r.clientForUser(req.User)
	if err != nil {
		return dify.ChatResult{}, err
	}
	return client.ChatStream(ctx, req, onEvent, onDelta)
}

func (r *DifyRouter) Stop(ctx context.Context, taskID, user string) error {
	client, err := r.clientForUser(user)
	if err != nil {
		return err
	}
	return client.Stop(ctx, taskID, user)
}

func (r *DifyRouter) clientForUser(user string) (*dify.Client, error) {
	npcID := npcIDFromUser(user)
	if npcID != "" {
		if client := r.clients[npcID]; client != nil {
			return client, nil
		}
	}
	if r.defaultClient != nil {
		return r.defaultClient, nil
	}
	return nil, fmt.Errorf("gateway: no Dify app key configured for npc_id %q and no default key", npcID)
}

func npcIDFromUser(user string) string {
	user = strings.TrimSpace(user)
	if user == "" {
		return ""
	}
	if idx := strings.LastIndex(user, ":"); idx >= 0 && idx < len(user)-1 {
		return strings.TrimSpace(user[idx+1:])
	}
	return ""
}
