package channels

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/notify"
)

// Webhook delivers incident transitions as a signed JSON POST, following
// the common "HMAC over raw body" pattern (à la Stripe/GitHub webhooks) so
// receivers can verify authenticity without a shared secret over the wire.
type Webhook struct {
	client *http.Client
}

func NewWebhook() *Webhook {
	return &Webhook{client: &http.Client{Timeout: 10 * time.Second}}
}

func (w *Webhook) Type() models.NotificationChannelType { return models.ChannelWebhook }

type webhookPayload struct {
	MonitorID   string `json:"monitor_id"`
	MonitorName string `json:"monitor_name"`
	URL         string `json:"url"`
	Region      string `json:"region"`
	Transition  string `json:"transition"`
	Cause       string `json:"cause,omitempty"`
	StartedAt   string `json:"started_at"`
	ResolvedAt  string `json:"resolved_at,omitempty"`
}

func (w *Webhook) Send(ctx context.Context, cfg models.NotificationChannel, event notify.Event) error {
	payload := webhookPayload{
		MonitorID:   event.Monitor.ID.String(),
		MonitorName: event.Monitor.Name,
		URL:         event.Monitor.URL,
		Region:      event.Incident.Region,
		Transition:  event.Transition,
		Cause:       event.Incident.Cause,
		StartedAt:   event.Incident.StartedAt.Format(time.RFC3339),
	}
	if event.Incident.ResolvedAt != nil {
		payload.ResolvedAt = event.Incident.ResolvedAt.Format(time.RFC3339)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.Target, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.Secret != "" {
		req.Header.Set("X-Uptime-Signature", sign(cfg.Secret, body))
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook receiver returned status %d", resp.StatusCode)
	}
	return nil
}

func sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
