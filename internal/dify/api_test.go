package dify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestClientStopPostsTaskStopRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/chat-messages/task-123/stop" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}

		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["user"] != "player-10086:npc-blacksmith" {
			t.Fatalf("user = %q", body["user"])
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	if err := client.Stop(context.Background(), "task-123", "player-10086:npc-blacksmith"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}

func TestClientStopInterruptsMockStream(t *testing.T) {
	stopped := make(chan struct{})
	var stopOnce sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/chat-messages":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"event\":\"message\",\"task_id\":\"task-123\",\"message_id\":\"msg-1\",\"conversation_id\":\"conv-1\",\"answer\":\"first\"}\n\n"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			<-stopped
			return
		case "/chat-messages/task-123/stop":
			stopOnce.Do(func() { close(stopped) })
			w.WriteHeader(http.StatusOK)
			return
		default:
			t.Fatalf("unexpected path = %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	var deltas []string
	_, err := client.ChatStream(
		context.Background(),
		ChatReq{Query: "hello", Inputs: map[string]string{}, User: "player-1"},
		func(taskID, convID string) {
			if taskID != "task-123" || convID != "conv-1" {
				t.Fatalf("onEvent(%q, %q), want task-123, conv-1", taskID, convID)
			}
			if err := client.Stop(context.Background(), taskID, "player-1"); err != nil {
				t.Fatalf("Stop() error = %v", err)
			}
		},
		func(delta string) { deltas = append(deltas, delta) },
	)
	if err == nil {
		t.Fatal("ChatStream() error = nil, want incomplete stream error after stop")
	}
	if !strings.Contains(err.Error(), "ended before terminal event") {
		t.Fatalf("ChatStream() error = %v, want incomplete stream error", err)
	}
	if len(deltas) != 1 || deltas[0] != "first" {
		t.Fatalf("deltas = %#v, want only first delta before stop", deltas)
	}
}

func TestClientUploadFileSendsMultipartAndParsesID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/files/upload" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data; boundary=") {
			t.Fatalf("Content-Type = %q", r.Header.Get("Content-Type"))
		}

		reader, err := r.MultipartReader()
		if err != nil {
			t.Fatalf("MultipartReader: %v", err)
		}
		fields := map[string]string{}
		var fileName string
		var fileContent string
		var fileContentType string
		for {
			part, err := reader.NextPart()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("NextPart: %v", err)
			}
			data, err := io.ReadAll(part)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if part.FormName() == "file" {
				fileName = part.FileName()
				fileContent = string(data)
				fileContentType = part.Header.Get("Content-Type")
			} else {
				fields[part.FormName()] = string(data)
			}
		}
		if fields["user"] != "player-1" {
			t.Fatalf("user field = %q", fields["user"])
		}
		if fileName != "avatar.png" || fileContent != "png-bytes" {
			t.Fatalf("fileName=%q fileContent=%q", fileName, fileContent)
		}
		if fileContentType != "image/png" {
			t.Fatalf("file part Content-Type = %q, want image/png", fileContentType)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"file-123","name":"avatar.png","size":9}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	result, err := client.UploadFile(context.Background(), UploadFileReq{
		User:        "player-1",
		Filename:    "avatar.png",
		Reader:      strings.NewReader("png-bytes"),
		ContentType: "image/png",
	})
	if err != nil {
		t.Fatalf("UploadFile() error = %v", err)
	}
	if result.ID != "file-123" || result.Name != "avatar.png" || result.Size != 9 {
		t.Fatalf("result = %#v", result)
	}
}

func TestClientGetParametersParsesVariableDefinitions(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/parameters" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer app-test-key" {
			t.Fatalf("Authorization = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"user_input_form":[
				{"text-input":{"label":"Player Level","variable":"player_level","required":true,"default":"1"}},
				{"paragraph":{"label":"Recent Events","variable":"recent_events","required":false}}
			],
			"file_upload":{"image":{"enabled":true,"number_limits":3}},
			"opening_statement":"Welcome"
		}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	params, err := client.GetParameters(context.Background())
	if err != nil {
		t.Fatalf("GetParameters() error = %v", err)
	}
	if params.OpeningStatement != "Welcome" {
		t.Fatalf("OpeningStatement = %q", params.OpeningStatement)
	}
	if len(params.UserInputForm) != 2 {
		t.Fatalf("UserInputForm len = %d", len(params.UserInputForm))
	}
	if params.UserInputForm[0].Type != "text-input" || params.UserInputForm[0].Variable != "player_level" || !params.UserInputForm[0].Required {
		t.Fatalf("first form item = %#v", params.UserInputForm[0])
	}
	if params.UserInputForm[1].Type != "paragraph" || params.UserInputForm[1].Variable != "recent_events" {
		t.Fatalf("second form item = %#v", params.UserInputForm[1])
	}
	if !params.FileUpload.Image.Enabled || params.FileUpload.Image.NumberLimits != 3 {
		t.Fatalf("FileUpload = %#v", params.FileUpload)
	}
}

func TestClientCheckParameterContractWarnsAboutMismatches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/parameters" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"user_input_form":[
				{"text-input":{"label":"Player Level","variable":"player_level","required":true}},
				{"text-input":{"label":"Affinity","variable":"affinity","required":false}}
			]
		}`))
	}))
	defer server.Close()

	client := NewClient(server.URL, "app-test-key", server.Client())
	var warnings []string
	err := client.CheckParameterContract(
		context.Background(),
		[]string{"player_level", "current_quest"},
		func(message string) { warnings = append(warnings, message) },
	)
	if err != nil {
		t.Fatalf("CheckParameterContract() error = %v", err)
	}
	if len(warnings) != 2 {
		t.Fatalf("warnings = %#v, want missing and unexpected warnings", warnings)
	}
	if !strings.Contains(warnings[0], "current_quest") || !strings.Contains(warnings[0], "missing") {
		t.Fatalf("first warning = %q", warnings[0])
	}
	if !strings.Contains(warnings[1], "affinity") || !strings.Contains(warnings[1], "unexpected") {
		t.Fatalf("second warning = %q", warnings[1])
	}
}
