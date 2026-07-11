package channels

import (
	"context"
	"fmt"
	"net/smtp"
	"time"

	"github.com/tailus/uptime-monitor/internal/models"
	"github.com/tailus/uptime-monitor/internal/notify"
)

// EmailConfig holds the shared SMTP relay settings (typically a transactional
// provider — SES, Postmark, Mailgun — reached over authenticated SMTP).
type EmailConfig struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
}

type Email struct {
	cfg  EmailConfig
	auth smtp.Auth
}

func NewEmail(cfg EmailConfig) *Email {
	return &Email{
		cfg:  cfg,
		auth: smtp.PlainAuth("", cfg.Username, cfg.Password, cfg.Host),
	}
}

func (e *Email) Type() models.NotificationChannelType { return models.ChannelEmail }

func (e *Email) Send(ctx context.Context, cfg models.NotificationChannel, event notify.Event) error {
	subject := fmt.Sprintf("[%s] %s is %s", event.Incident.Region, event.Monitor.Name, statusWord(event.Transition))
	body := fmt.Sprintf(
		"Monitor: %s\nURL: %s\nRegion: %s\nStatus: %s\nCause: %s\nSince: %s\n",
		event.Monitor.Name, event.Monitor.URL, event.Incident.Region,
		statusWord(event.Transition), event.Incident.Cause,
		event.Incident.StartedAt.Format(time.RFC1123),
	)
	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n%s", e.cfg.From, cfg.Target, subject, body)

	addr := fmt.Sprintf("%s:%d", e.cfg.Host, e.cfg.Port)
	// net/smtp has no context support; callers bound overall attempt time via
	// the retry policy's per-attempt nature rather than a context deadline here.
	return smtp.SendMail(addr, e.auth, e.cfg.From, []string{cfg.Target}, []byte(msg))
}

func statusWord(transition string) string {
	if transition == "resolved" {
		return "UP"
	}
	return "DOWN"
}
