package sub2api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"flashcat.cloud/categraf/inputs/internal/llmupstream"
)

// wrapClientError converts a llmupstream client error into an error whose
// message includes the HTTP status and the sanitized upstream error payload,
// with the raw API key redacted. Non-HTTP errors are returned as-is.
func (ins *Instance) wrapClientError(err error) error {
	var statusErr *llmupstream.HTTPStatusError
	if !errors.As(err, &statusErr) {
		return err
	}
	fields := fmt.Sprintf("status=%d", statusErr.StatusCode)
	var payload struct {
		Error *struct {
			Type    string `json:"type"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(statusErr.Body), &payload) == nil {
		if payload.Error != nil {
			if payload.Error.Type != "" {
				fields += fmt.Sprintf(" type=%q", ins.sanitizeErrorField(payload.Error.Type))
			}
			if payload.Error.Code != "" {
				fields += fmt.Sprintf(" code=%q", ins.sanitizeErrorField(payload.Error.Code))
			}
			if payload.Error.Message != "" {
				fields += fmt.Sprintf(" message=%q", ins.sanitizeErrorField(payload.Error.Message))
			}
		} else {
			if payload.Code != 0 {
				fields += fmt.Sprintf(" code=%d", payload.Code)
			}
			if payload.Message != "" {
				fields += fmt.Sprintf(" message=%q", ins.sanitizeErrorField(payload.Message))
			}
		}
	}
	return fmt.Errorf("%s", fields)
}

func (ins *Instance) authHeaders() map[string]string {
	return map[string]string{"Authorization": "Bearer " + ins.apiKey}
}

func (ins *Instance) updateRedactionSecrets() {
	secrets := make([]string, 0, 1)
	if ins.apiKey != "" {
		secrets = append(secrets, ins.apiKey)
	}
	ins.redactionMu.Lock()
	ins.redactionSecrets = secrets
	ins.redactionMu.Unlock()
}

func (ins *Instance) sanitizeErrorField(value string) string {
	ins.redactionMu.RLock()
	secrets := append([]string(nil), ins.redactionSecrets...)
	ins.redactionMu.RUnlock()
	for _, secret := range secrets {
		if secret != "" && strings.Contains(value, secret) {
			value = strings.ReplaceAll(value, secret, "[redacted]")
		}
	}
	return sanitizeErrorField(value)
}

func sanitizeErrorField(value string) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	lower := strings.ToLower(value)
	for _, marker := range []string{"bearer ", "sk-", "rt_"} {
		if strings.Contains(lower, marker) {
			return "[redacted]"
		}
	}
	for _, field := range strings.Fields(value) {
		if isLikelyJWT(strings.Trim(field, `"'(),;`)) {
			return "[redacted]"
		}
	}
	const maxRunes = 256
	if utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes]) + "..."
}

func isLikelyJWT(value string) bool {
	if len(value) < 32 {
		return false
	}
	parts := strings.Split(value, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		for _, r := range part {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_') {
				return false
			}
		}
	}
	return true
}
