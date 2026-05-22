package mailer

import (
	"bytes"
	"fmt"
	"html/template"
	"time"

	"github.com/go-mail/mail/v2"
)

type Mailer struct {
	dialer    *mail.Dialer
	templates map[string]*template.Template
}

// --------------------------------------------------------------------
// Dynamic Template Data
// --------------------------------------------------------------------

type TemplateData struct {
	Email string
	JobID string
}

// --------------------------------------------------------------------
// Template Registry
// --------------------------------------------------------------------

// NEVER trust template IDs directly from external input.
// Whitelist them explicitly.
var allowedTemplates = map[string]string{
	"welcome":      "templates/welcome.html",
	"early_access": "templates/early_access.html",
	"newsletter":   "templates/newsletter.html",
}

// --------------------------------------------------------------------
// Constructor
// --------------------------------------------------------------------

func New(
	host string,
	port int,
	username string,
	password string,
) (*Mailer, error) {

	// --------------------------------------------------------
	// SMTP Dialer
	// --------------------------------------------------------

	dialer := mail.NewDialer(
		host,
		port,
		username,
		password,
	)

	// Prevent hanging SMTP connections
	dialer.Timeout = 10 * time.Second

	// --------------------------------------------------------
	// Preload Templates
	// --------------------------------------------------------

	templateCache := make(map[string]*template.Template)

	for templateID, path := range allowedTemplates {

		// absPath, err := filepath.Abs(path)
		// if err != nil {
		// 	return nil, fmt.Errorf(
		// 		"failed to resolve template path %s: %w",
		// 		path,
		// 		err,
		// 	)
		// }

		tmpl, err := template.ParseFiles(path)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to parse template %s: %w",
				templateID,
				err,
			)
		}

		templateCache[templateID] = tmpl
	}

	return &Mailer{
		dialer:    dialer,
		templates: templateCache,
	}, nil
}

// --------------------------------------------------------------------
// Send Email
// --------------------------------------------------------------------

func (m *Mailer) SendEmail(
	to string,
	subject string,
	templateID string,
	data TemplateData,
) error {

	// --------------------------------------------------------
	// Lookup Cached Template
	// --------------------------------------------------------

	tmpl, exists := m.templates[templateID]

	if !exists {
		return fmt.Errorf(
			"unknown template ID: %s",
			templateID,
		)
	}

	// --------------------------------------------------------
	// Render HTML Template
	// --------------------------------------------------------

	var body bytes.Buffer

	if err := tmpl.Execute(&body, data); err != nil {
		return fmt.Errorf(
			"failed to execute template %s: %w",
			templateID,
			err,
		)
	}

	// --------------------------------------------------------
	// Build Email Message
	// --------------------------------------------------------

	msg := mail.NewMessage()

	msg.SetHeader(
		"From",
		"noreply@yourcompany.com",
	)

	msg.SetHeader(
		"To",
		to,
	)

	msg.SetHeader(
		"Subject",
		subject,
	)

	// Plain-text fallback improves deliverability
	msg.SetBody(
		"text/plain",
		fmt.Sprintf(
			"Hello %s,\n\nPlease view this email in an HTML-compatible client.",
			data.Email,
		),
	)

	// HTML body
	msg.AddAlternative(
		"text/html",
		body.String(),
	)

	// --------------------------------------------------------
	// SMTP Dispatch
	// --------------------------------------------------------

	if err := m.dialer.DialAndSend(msg); err != nil {
		return fmt.Errorf(
			"failed to send email via SMTP: %w",
			err,
		)
	}

	return nil
}
