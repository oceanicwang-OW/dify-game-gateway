package dify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strings"
)

func (c *Client) Stop(ctx context.Context, taskID, user string) error {
	if strings.TrimSpace(taskID) == "" {
		return fmt.Errorf("task ID is required")
	}
	if strings.TrimSpace(user) == "" {
		return fmt.Errorf("user is required")
	}

	body, err := json.Marshal(map[string]string{"user": user})
	if err != nil {
		return fmt.Errorf("marshal Dify stop request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat-messages/"+url.PathEscape(taskID)+"/stop", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create Dify stop request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call Dify stop: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Dify stop returned HTTP %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}
	return nil
}

func (c *Client) UploadFile(ctx context.Context, req UploadFileReq) (UploadFileResult, error) {
	if strings.TrimSpace(req.User) == "" {
		return UploadFileResult{}, fmt.Errorf("user is required")
	}
	if strings.TrimSpace(req.Filename) == "" {
		return UploadFileResult{}, fmt.Errorf("filename is required")
	}
	if req.Reader == nil {
		return UploadFileResult{}, fmt.Errorf("file reader is required")
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("user", req.User); err != nil {
		return UploadFileResult{}, fmt.Errorf("write upload user field: %w", err)
	}
	partHeader := make(textproto.MIMEHeader)
	partHeader.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, req.Filename))
	partHeader.Set("Content-Type", fileContentType(req))
	part, err := writer.CreatePart(partHeader)
	if err != nil {
		return UploadFileResult{}, fmt.Errorf("create upload file part: %w", err)
	}
	if _, err := io.Copy(part, req.Reader); err != nil {
		return UploadFileResult{}, fmt.Errorf("write upload file content: %w", err)
	}
	if err := writer.Close(); err != nil {
		return UploadFileResult{}, fmt.Errorf("finalize upload multipart body: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/files/upload", &body)
	if err != nil {
		return UploadFileResult{}, fmt.Errorf("create Dify upload request: %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", writer.FormDataContentType())
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return UploadFileResult{}, fmt.Errorf("call Dify upload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return UploadFileResult{}, fmt.Errorf("Dify upload returned HTTP %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}

	var result UploadFileResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return UploadFileResult{}, fmt.Errorf("decode Dify upload response: %w", err)
	}
	return result, nil
}

func (c *Client) GetParameters(ctx context.Context) (Parameters, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/parameters", nil)
	if err != nil {
		return Parameters{}, fmt.Errorf("create Dify parameters request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Parameters{}, fmt.Errorf("call Dify parameters: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Parameters{}, fmt.Errorf("Dify parameters returned HTTP %d: %s", resp.StatusCode, readErrorBody(resp.Body))
	}

	var raw parametersPayload
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return Parameters{}, fmt.Errorf("decode Dify parameters response: %w", err)
	}
	return raw.toParameters(), nil
}

func (c *Client) CheckParameterContract(ctx context.Context, expectedVariables []string, warn func(message string)) error {
	params, err := c.GetParameters(ctx)
	if err != nil {
		return err
	}
	if warn == nil {
		return nil
	}

	defined := make(map[string]struct{}, len(params.UserInputForm))
	for _, item := range params.UserInputForm {
		if item.Variable != "" {
			defined[item.Variable] = struct{}{}
		}
	}
	expected := make(map[string]struct{}, len(expectedVariables))
	for _, variable := range expectedVariables {
		variable = strings.TrimSpace(variable)
		if variable == "" {
			continue
		}
		expected[variable] = struct{}{}
		if _, ok := defined[variable]; !ok {
			warn(fmt.Sprintf("Dify parameter contract missing variable %q", variable))
		}
	}
	for variable := range defined {
		if _, ok := expected[variable]; !ok {
			warn(fmt.Sprintf("Dify parameter contract has unexpected variable %q", variable))
		}
	}
	return nil
}

func readErrorBody(r io.Reader) string {
	data, _ := io.ReadAll(io.LimitReader(r, 4096))
	return strings.TrimSpace(string(data))
}

// fileContentType resolves the MIME type for an uploaded file part. An explicit
// req.ContentType wins; otherwise it is inferred from the filename extension,
// falling back to application/octet-stream. Dify's multimodal upload keys off
// this, so a missing/wrong type can cause images to be rejected.
func fileContentType(req UploadFileReq) string {
	if ct := strings.TrimSpace(req.ContentType); ct != "" {
		return ct
	}
	if ct := mime.TypeByExtension(filepath.Ext(req.Filename)); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

type parametersPayload struct {
	UserInputForm    []map[string]parameterFormItem `json:"user_input_form"`
	FileUpload       FileUploadConfig               `json:"file_upload"`
	OpeningStatement string                         `json:"opening_statement"`
}

func (p parametersPayload) toParameters() Parameters {
	items := make([]ParameterFormItem, 0, len(p.UserInputForm))
	for _, wrapped := range p.UserInputForm {
		for typ, item := range wrapped {
			items = append(items, ParameterFormItem{
				Type:     typ,
				Label:    item.Label,
				Variable: item.Variable,
				Required: item.Required,
				Default:  item.Default,
			})
		}
	}
	return Parameters{
		UserInputForm:    items,
		FileUpload:       p.FileUpload,
		OpeningStatement: p.OpeningStatement,
	}
}

type parameterFormItem struct {
	Label    string `json:"label"`
	Variable string `json:"variable"`
	Required bool   `json:"required"`
	Default  string `json:"default"`
}
