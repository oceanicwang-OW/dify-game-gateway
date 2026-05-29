package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const blockingResponseMode = "blocking"
const maxUpstreamAttempts = 3

var upstreamRetryBackoff = 10 * time.Millisecond

type UpstreamError struct {
	Operation  string
	StatusCode int
	Body       string
}

func (e *UpstreamError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("Dify %s returned HTTP %d", e.Operation, e.StatusCode)
	}
	return fmt.Sprintf("Dify %s returned HTTP %d: %s", e.Operation, e.StatusCode, e.Body)
}

func (e *UpstreamError) Retryable() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

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

	resp, err := c.doRequestWithRetry(ctx, "chat", http.MethodPost, c.baseURL+"/chat-messages", body, "application/json")
	if err != nil {
		return ChatResult{}, err
	}
	defer resp.Body.Close()

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

func (c *Client) doRequestWithRetry(ctx context.Context, operation, method, url string, body []byte, accept string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= maxUpstreamAttempts; attempt++ {
		resp, err := c.doRequest(ctx, operation, method, url, body, accept)
		if err == nil {
			return resp, nil
		}
		lastErr = err

		var upstreamErr *UpstreamError
		if !errors.As(err, &upstreamErr) || !upstreamErr.Retryable() || attempt == maxUpstreamAttempts {
			return nil, err
		}
		if err := sleepBeforeRetry(ctx); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *Client) doRequest(ctx context.Context, operation, method, url string, body []byte, accept string) (*http.Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create Dify %s request: %w", operation, err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", accept)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("call Dify %s: %w", operation, err)
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp, nil
	}
	defer resp.Body.Close()

	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return nil, &UpstreamError{
		Operation:  operation,
		StatusCode: resp.StatusCode,
		Body:       strings.TrimSpace(string(data)),
	}
}

func sleepBeforeRetry(ctx context.Context) error {
	timer := time.NewTimer(upstreamRetryBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
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
