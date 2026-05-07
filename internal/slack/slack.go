// Package slack posts to a Slack incoming-webhook URL.
//
// On any non-2xx response, Post returns an error carrying the response body.
// The caller (a pitr-akeyless Job binary's main) surfaces non-zero exit, the
// K8s Job fails, the wrapping Crossplane Object goes NotReady, the composite
// goes Ready=False, the drill phase=Failed.
//
// This is the 1:1 reconciliation property Decision 15 commits to (PITR
// Conventions Confluence page 4062117901 §"Chart-level master switches" +
// PITR as a Packaged Function page 4060807173 Decision 11): "drill green"
// implies "every Slack notification reached its target."
//
// The webhook URL is sensitive (a Slack incoming webhook is a bearer
// credential) — this package does not log or echo it.
package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DefaultTimeout caps a single Post attempt. Slack's webhook endpoint is
// usually <1s; 10s is generous for transient slowness.
const DefaultTimeout = 10 * time.Second

// Message is the minimum Slack incoming-webhook payload shape. Slack supports
// blocks/attachments — this struct will grow as drill iteration surfaces a
// genuine need.
type Message struct {
	Text string `json:"text"`
}

// Client posts JSON payloads to a Slack webhook URL.
type Client struct {
	URL  string
	HTTP *http.Client
}

// New returns a Client targeting the given webhook URL. The URL is typically
// mounted as an env var by the K8s Job spec, sourced from a Secret synced via
// ESO from akeyless (Decision 11).
func New(webhookURL string) *Client {
	return &Client{
		URL:  webhookURL,
		HTTP: &http.Client{Timeout: DefaultTimeout},
	}
}

// Post sends the message. Returns an error on non-2xx; the response body is
// included to aid drill-iteration debugging. The webhook URL is never echoed.
func (c *Client) Post(msg Message) error {
	if c.URL == "" {
		return errors.New("slack: webhook URL is empty")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("slack: marshal payload: %w", err)
	}
	req, err := http.NewRequest(http.MethodPost, c.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("slack: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("slack: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("slack: non-2xx response: status=%d body=%q", resp.StatusCode, string(respBody))
	}
	return nil
}
