package dify

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const streamingResponseMode = "streaming"

// ErrStreamIncomplete is returned when the SSE stream ends without a terminal
// event (message_end/error/message_replace). The orchestration layer uses it to
// tell an intentional Stop (it called Stop, so truncation is expected) apart
// from a genuine upstream truncation that should surface as UPSTREAM_ERROR.
var ErrStreamIncomplete = errors.New("Dify SSE stream ended before terminal event")

type StreamError struct {
	Code    string
	Message string
}

func (e *StreamError) Error() string {
	if e.Code == "" {
		return e.Message
	}
	return e.Code + ": " + e.Message
}

type MessageReplaceError struct {
	Fallback string
}

func (e *MessageReplaceError) Error() string {
	return "message replaced by upstream moderation"
}

func (c *Client) ChatStream(ctx context.Context, req ChatReq, onEvent func(taskID, convID string), onDelta func(delta string)) (ChatResult, error) {
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
		ResponseMode:   streamingResponseMode,
		ConversationID: req.ConversationID,
		Files:          req.Files,
		AutoGenerate:   req.AutoGenerateName,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return ChatResult{}, fmt.Errorf("marshal Dify stream request: %w", err)
	}

	resp, err := c.doRequestWithRetry(ctx, "stream", http.MethodPost, c.baseURL+"/chat-messages", body, "text/event-stream")
	if err != nil {
		return ChatResult{}, err
	}
	defer resp.Body.Close()

	return parseSSE(resp.Body, onEvent, onDelta)
}

func parseSSE(r io.Reader, onEvent func(taskID, convID string), onDelta func(delta string)) (ChatResult, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	var result ChatResult
	eventNotified := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}

		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "" || data == "[DONE]" {
			continue
		}

		var event sseEvent
		if err := json.Unmarshal([]byte(data), &event); err != nil {
			return result, fmt.Errorf("decode Dify SSE event: %w", err)
		}

		switch event.Event {
		case "message", "agent_message":
			if result.ConversationID == "" {
				result.ConversationID = event.ConversationID
			}
			if result.MessageID == "" {
				result.MessageID = event.MessageID
			}
			if !eventNotified && onEvent != nil {
				onEvent(event.TaskID, event.ConversationID)
				eventNotified = true
			}
			if event.Answer != "" && onDelta != nil {
				onDelta(event.Answer)
			}
		case "message_end":
			if event.ConversationID != "" {
				result.ConversationID = event.ConversationID
			}
			if event.MessageID != "" {
				result.MessageID = event.MessageID
			}
			result.TotalTokens = event.Metadata.Usage.TotalTokens
			return result, nil
		case "message_replace":
			return result, &MessageReplaceError{Fallback: event.Answer}
		case "error":
			return result, &StreamError{Code: event.Code, Message: event.Message}
		case "ping", "message_file", "tts_message", "tts_message_end", "workflow_started", "node_started", "node_finished", "workflow_finished":
			continue
		default:
			continue
		}
	}
	if err := scanner.Err(); err != nil {
		return result, fmt.Errorf("read Dify SSE stream: %w", err)
	}
	return result, ErrStreamIncomplete
}

type sseEvent struct {
	Event          string `json:"event"`
	TaskID         string `json:"task_id"`
	MessageID      string `json:"message_id"`
	ConversationID string `json:"conversation_id"`
	Answer         string `json:"answer"`
	Code           string `json:"code"`
	Message        string `json:"message"`
	Metadata       struct {
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	} `json:"metadata"`
}
