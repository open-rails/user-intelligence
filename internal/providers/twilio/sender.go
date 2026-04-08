package twilio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Sender struct {
	AccountSID  string
	AuthToken   string
	SMSFrom     string
	EmailFrom   string
	EmailAPIKey string
	HTTPClient  *http.Client
}

func (s Sender) SendSMS(ctx context.Context, to, body string) (string, error) {
	if strings.TrimSpace(s.AccountSID) == "" || strings.TrimSpace(s.AuthToken) == "" {
		return "", fmt.Errorf("twilio credentials are required")
	}
	if strings.TrimSpace(s.SMSFrom) == "" {
		return "", fmt.Errorf("twilio sms from is required")
	}
	form := url.Values{}
	form.Set("To", to)
	form.Set("From", s.SMSFrom)
	form.Set("Body", body)

	endpoint := fmt.Sprintf("https://api.twilio.com/2010-04-01/Accounts/%s/Messages.json", s.AccountSID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(s.AccountSID, s.AuthToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("twilio sms send failed: status=%d body=%s", resp.StatusCode, string(b))
	}

	var out struct {
		SID string `json:"sid"`
	}
	if err := json.Unmarshal(b, &out); err != nil {
		return "", err
	}
	return out.SID, nil
}

func (s Sender) SendEmail(ctx context.Context, to, subject, body string) error {
	if strings.TrimSpace(s.EmailAPIKey) == "" {
		return fmt.Errorf("twilio email api key is required")
	}
	if strings.TrimSpace(s.EmailFrom) == "" {
		return fmt.Errorf("twilio email from is required")
	}
	if strings.TrimSpace(to) == "" {
		return fmt.Errorf("recipient email is required")
	}
	if strings.TrimSpace(subject) == "" {
		return fmt.Errorf("email subject is required")
	}
	if strings.TrimSpace(body) == "" {
		return fmt.Errorf("email body is required")
	}

	payload := map[string]any{
		"personalizations": []map[string]any{
			{
				"to": []map[string]string{
					{"email": strings.TrimSpace(to)},
				},
			},
		},
		"from": map[string]string{
			"email": strings.TrimSpace(s.EmailFrom),
		},
		"subject": strings.TrimSpace(subject),
		"content": []map[string]string{
			{
				"type":  "text/plain",
				"value": strings.TrimSpace(body),
			},
		},
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.sendgrid.com/v3/mail/send", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(s.EmailAPIKey))
	req.Header.Set("Content-Type", "application/json")

	client := s.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("twilio email send failed: status=%d body=%s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}
