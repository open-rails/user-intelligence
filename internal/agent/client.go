package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const defaultOpenAIBaseURL = "https://api.openai.com/v1"

type ResponsesRequest struct {
	Model              string            `json:"model"`
	Input              []map[string]any  `json:"input"`
	Tools              []map[string]any  `json:"tools,omitempty"`
	PreviousResponseID string            `json:"previous_response_id,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	MaxOutputTokens    int               `json:"max_output_tokens,omitempty"`
}

type ResponsesResponse struct {
	ID         string             `json:"id"`
	OutputText string             `json:"output_text,omitempty"`
	Output     []ResponsesOutput  `json:"output,omitempty"`
	Error      *ResponsesAPIError `json:"error,omitempty"`
}

type ResponsesOutput struct {
	ID        string             `json:"id,omitempty"`
	Type      string             `json:"type,omitempty"`
	Role      string             `json:"role,omitempty"`
	Name      string             `json:"name,omitempty"`
	CallID    string             `json:"call_id,omitempty"`
	Arguments string             `json:"arguments,omitempty"`
	Content   []ResponsesContent `json:"content,omitempty"`
}

type ResponsesContent struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type ResponsesAPIError struct {
	Message string `json:"message,omitempty"`
	Type    string `json:"type,omitempty"`
}

type ResponsesClient interface {
	Create(ctx context.Context, req ResponsesRequest) (ResponsesResponse, error)
}

type OpenAIResponsesClient struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

func NewOpenAIResponsesClient(apiKey string) (*OpenAIResponsesClient, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, fmt.Errorf("openai api key is required")
	}
	return &OpenAIResponsesClient{
		apiKey:  apiKey,
		baseURL: defaultOpenAIBaseURL,
		httpClient: &http.Client{
			Timeout: 45 * time.Second,
		},
	}, nil
}

func (c *OpenAIResponsesClient) Create(ctx context.Context, req ResponsesRequest) (ResponsesResponse, error) {
	if strings.TrimSpace(req.Model) == "" {
		return ResponsesResponse{}, fmt.Errorf("model is required")
	}
	if len(req.Input) == 0 {
		return ResponsesResponse{}, fmt.Errorf("input is required")
	}

	body, err := json.Marshal(req)
	if err != nil {
		return ResponsesResponse{}, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/responses", bytes.NewReader(body))
	if err != nil {
		return ResponsesResponse{}, err
	}
	httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return ResponsesResponse{}, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var e struct {
			Error ResponsesAPIError `json:"error"`
		}
		_ = json.Unmarshal(respBody, &e)
		if strings.TrimSpace(e.Error.Message) != "" {
			return ResponsesResponse{}, fmt.Errorf("responses api failed: status=%d message=%s", resp.StatusCode, e.Error.Message)
		}
		return ResponsesResponse{}, fmt.Errorf("responses api failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var out ResponsesResponse
	if err := json.Unmarshal(respBody, &out); err != nil {
		return ResponsesResponse{}, err
	}
	if out.Error != nil && strings.TrimSpace(out.Error.Message) != "" {
		return ResponsesResponse{}, fmt.Errorf("responses api error: %s", out.Error.Message)
	}
	return out, nil
}
