package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const blockingResponseMode = "blocking"

type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

func NewClient(baseURL, apiKey string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: httpClient,
	}
}

func (c *Client) Chat(ctx context.Context, req ChatReq) (ChatResult, error) {
	if err := validateChatReq(req); err != nil {
		return ChatResult{}, err
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return ChatResult{}, fmt.Errorf("dify base URL is required")
	}
	if strings.TrimSpace(c.apiKey) == "" {
		return ChatResult{}, fmt.Errorf("dify API key is required")
	}

	payload := chatRequestPayload{
		Query:          req.Query,
		Inputs:         req.Inputs,
		User:           req.User,
		ResponseMode:   blockingResponseMode,
		ConversationID: req.ConversationID,
		Files:          req.Files,
		AutoGenerate:   req.AutoGenerateName,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ChatResult{}, fmt.Errorf("marshal Dify chat request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat-messages", bytes.NewReader(body))
	if err != nil {
		return ChatResult{}, fmt.Errorf("create Dify chat request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return ChatResult{}, fmt.Errorf("call Dify chat: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return ChatResult{}, fmt.Errorf("Dify chat returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}

	var decoded chatResponsePayload
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return ChatResult{}, fmt.Errorf("decode Dify chat response: %w", err)
	}

	return ChatResult{
		Answer:         decoded.Answer,
		ConversationID: decoded.ConversationID,
		MessageID:      decoded.MessageID,
		TotalTokens:    decoded.Metadata.Usage.TotalTokens,
	}, nil
}

func validateChatReq(req ChatReq) error {
	if strings.TrimSpace(req.Query) == "" {
		return fmt.Errorf("query is required")
	}
	if req.Inputs == nil {
		return fmt.Errorf("inputs is required")
	}
	if strings.TrimSpace(req.User) == "" {
		return fmt.Errorf("user is required")
	}
	return nil
}

type chatRequestPayload struct {
	Query          string            `json:"query"`
	Inputs         map[string]string `json:"inputs"`
	User           string            `json:"user"`
	ResponseMode   string            `json:"response_mode"`
	ConversationID string            `json:"conversation_id,omitempty"`
	Files          []FileRef         `json:"files,omitempty"`
	AutoGenerate   *bool             `json:"auto_generate_name,omitempty"`
}

type chatResponsePayload struct {
	Answer         string `json:"answer"`
	ConversationID string `json:"conversation_id"`
	MessageID      string `json:"message_id"`
	Metadata       struct {
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	} `json:"metadata"`
}
