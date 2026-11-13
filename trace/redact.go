package trace

import (
	"net/url"
	"regexp"
	"strings"
)

const redactedValue = "[redacted]"

// DefaultRedactKeys are case-insensitive substrings; any JSON field, form
// field or query parameter whose name contains one of them has its value
// masked in the stored record.
var DefaultRedactKeys = []string{
	"password", "passwd", "pwd", "secret", "token", "apikey", "api_key",
	"api-key", "authorization", "credential", "private_key", "cookie", "csrf",
	"xsrf",
}

// defaultBinaryContentTypes are redacted when RedactUploads (requests) or
// RedactDownloads (responses) is on. A trailing "/" matches a whole family.
var defaultBinaryContentTypes = []string{
	"application/octet-stream",
	"application/zip",
	"application/pdf",
	"image/",
	"video/",
	"audio/",
}

// keyMasker masks sensitive values in queries, form bodies and JSON bodies.
type keyMasker struct {
	keys []string
	json *regexp.Regexp
}

func newKeyMasker(keys []string) *keyMasker {
	m := &keyMasker{}
	alt := make([]string, 0, len(keys))
	for _, k := range keys {
		k = strings.ToLower(strings.TrimSpace(k))
		if k == "" {
			continue
		}
		m.keys = append(m.keys, k)
		alt = append(alt, regexp.QuoteMeta(k))
	}
	if len(alt) == 0 {
		return nil
	}
	// The closing quote is optional so a string cut off by body truncation is
	// still masked.
	m.json = regexp.MustCompile(`(?i)("[^"\\]*(?:` + strings.Join(alt, "|") +
		`)[^"\\]*"\s*:\s*)"(?:[^"\\]|\\.)*(?:"|$)`)
	return m
}

func (m *keyMasker) sensitive(name string) bool {
	name = strings.ToLower(name)
	for _, k := range m.keys {
		if strings.Contains(name, k) {
			return true
		}
	}
	return false
}

// maskQuery masks values of sensitive keys in a query string or a
// form-urlencoded body, leaving every other pair byte-for-byte intact.
func (m *keyMasker) maskQuery(raw string) string {
	if m == nil || raw == "" {
		return raw
	}
	parts := strings.Split(raw, "&")
	for i, p := range parts {
		k, _, found := strings.Cut(p, "=")
		if !found {
			continue
		}
		name, err := url.QueryUnescape(k)
		if err != nil {
			name = k
		}
		if m.sensitive(name) {
			parts[i] = k + "=" + url.QueryEscape(redactedValue)
		}
	}
	return strings.Join(parts, "&")
}

// maskJSON masks string values of sensitive keys. It works on the text so it
// also handles truncated documents.
func (m *keyMasker) maskJSON(body []byte) []byte {
	if m == nil || len(body) == 0 {
		return body
	}
	return m.json.ReplaceAll(body, []byte(`${1}"`+redactedValue+`"`))
}

// maskBody picks the masking strategy from the content type.
func (m *keyMasker) maskBody(contentType string, body []byte) []byte {
	if m == nil || len(body) == 0 {
		return body
	}
	ct := strings.ToLower(contentType)
	switch {
	case strings.Contains(ct, "x-www-form-urlencoded"):
		return []byte(m.maskQuery(string(body)))
	case ct == "" || strings.Contains(ct, "json") || strings.HasPrefix(ct, "text/"):
		return m.maskJSON(body)
	}
	return body
}

func matchContentType(ct string, patterns []string) bool {
	for _, p := range patterns {
		p = strings.ToLower(p)
		if strings.HasSuffix(p, "/") {
			if strings.HasPrefix(ct, p) {
				return true
			}
		} else if ct == p || strings.HasPrefix(ct, p+";") {
			return true
		}
	}
	return false
}

// shouldRedact decides whether a body is replaced with a placeholder.
// Uploads (requests) and downloads (responses) are only checked against the
// built-in binary types when their respective flag is on; an explicit
// RedactContentTypes list and RedactThresholdBytes always apply.
func (r *Recorder) shouldRedact(contentType string, size int64, cfg Config, upload bool) bool {
	ct := strings.ToLower(contentType)
	if upload && cfg.RedactUploads {
		if strings.HasPrefix(ct, "multipart/") || matchContentType(ct, defaultBinaryContentTypes) {
			return true
		}
	}
	if !upload && cfg.RedactDownloads && matchContentType(ct, defaultBinaryContentTypes) {
		return true
	}
	if matchContentType(ct, cfg.RedactContentTypes) {
		return true
	}
	return cfg.RedactThresholdBytes > 0 && size > cfg.RedactThresholdBytes
}
