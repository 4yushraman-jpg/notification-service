package mailer

import (
	"errors"
	"net"
	"strings"
	"syscall"
)

// IsTransientSMTPError reports whether err indicates a retryable SMTP failure.
func IsTransientSMTPError(err error) bool {
	if err == nil {
		return false
	}

	if IsPermanentSMTPError(err) {
		return false
	}

	msg := strings.ToLower(err.Error())

	for _, code := range []string{"421", "450", "451", "452"} {
		if containsSMTPCode(msg, code) {
			return true
		}
	}

	transientPhrases := []string{
		"throttl",
		"too many emails per second",
		"too many emails",
		"connection reset",
		"connection refused",
		"timeout",
		"timed out",
		"temporary unavailable",
		"temporarily unavailable",
		"temporarily",
		"try again later",
		"service not available",
		"server busy",
		"i/o timeout",
		"eof",
	}

	for _, phrase := range transientPhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscall.ECONNRESET) ||
			errors.Is(opErr.Err, syscall.ECONNREFUSED) {
			return true
		}
	}

	return false
}

// IsPermanentSMTPError reports whether err indicates a non-retryable SMTP failure.
func IsPermanentSMTPError(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())

	if strings.Contains(msg, "unknown template id") {
		return true
	}

	permanentPhrases := []string{
		"mailbox not found",
		"mailbox unavailable",
		"user unknown",
		"recipient address rejected",
		"invalid recipient",
		"invalid domain",
		"no such user",
		"does not exist",
		"address rejected",
		"not found",
	}

	for _, phrase := range permanentPhrases {
		if strings.Contains(msg, phrase) {
			return true
		}
	}

	for _, code := range []string{"551", "553", "554"} {
		if containsSMTPCode(msg, code) {
			return true
		}
	}

	if containsSMTPCode(msg, "550") && isMailboxFailure550(msg) {
		return true
	}

	return false
}

func containsSMTPCode(msg, code string) bool {
	patterns := []string{
		code + " ",
		code + "-",
		"smtp " + code,
		"response " + code,
		"status " + code,
		":" + code,
	}
	for _, p := range patterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return strings.Contains(msg, code)
}

func isMailboxFailure550(msg string) bool {
	mailboxIndicators := []string{
		"mailbox",
		"user unknown",
		"recipient",
		"no such",
		"does not exist",
		"invalid address",
		"address rejected",
		"undeliverable",
		"not found",
	}
	for _, indicator := range mailboxIndicators {
		if strings.Contains(msg, indicator) {
			return true
		}
	}
	return false
}
