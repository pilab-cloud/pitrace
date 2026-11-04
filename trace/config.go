package trace

import (
	"os"
	"path/filepath"
	"time"
)

// Config controls the request recorder. Every field has a safe zero-value
// default, so an operator only has to set Enabled and Dir.
type Config struct {
	// Enabled turns recording on. Off by default: traces contain mail bodies.
	Enabled bool `mapstructure:"enabled"`

	// Dir is the root of the trace segments. Defaults to
	// /var/lib/pimail/trace.
	Dir string `mapstructure:"dir"`

	// GRPCEndpoint is the loopback address of the read API. The daemon starts
	// it when Enabled is true; empty means 127.0.0.1:9460.
	GRPCEndpoint string `mapstructure:"grpc_endpoint"`

	// MaxRequestBytes/MaxResponseBytes cap what is kept per record. Larger
	// payloads are truncated and marked as such; the byte counts stay exact.
	MaxRequestBytes  int64 `mapstructure:"max_request_bytes"`
	MaxResponseBytes int64 `mapstructure:"max_response_bytes"`

	// MaxAge and MaxBytes bound retention. Whole segments are deleted, oldest
	// first, when either limit is crossed.
	MaxAge   time.Duration `mapstructure:"max_age"`
	MaxBytes int64         `mapstructure:"max_bytes"`

	// QueueSize is the number of pending records before the recorder starts
	// dropping them. It never blocks a request. Defaults to 4096.
	QueueSize int `mapstructure:"queue_size"`

	// SampleRatio keeps a fraction of exchanges (1.0 = all). Operations in
	// AlwaysOperations ignore the ratio, so mutations are never lost.
	SampleRatio      float64  `mapstructure:"sample_ratio"`
	AlwaysOperations []string `mapstructure:"always_operations"`

	// Rotation bounds one segment file.
	RotateInterval time.Duration `mapstructure:"rotate_interval"`
	RotateBytes    int64         `mapstructure:"rotate_bytes"`

	// HeaderAllowlist names the request/response headers copied into a record.
	// Authorization, Cookie and Set-Cookie must never be added here.
	HeaderAllowlist []string `mapstructure:"header_allowlist"`

	// HeaderNameMode controls how header names are stored: "enum" (default,
	// compact well-known enum) or "original" (preserve original casing/format).
	HeaderNameMode string `mapstructure:"header_name_mode"`

	// MaxRequestBodyBytes caps how many request body bytes are read (hard cap
	// via MaxBytesReader). 0 disables the hard cap.
	MaxRequestBodyBytes int64 `mapstructure:"max_request_body_bytes"`
	// MaxResponseBodyBytes caps response body capture buffer.
	MaxResponseBodyBytes int64 `mapstructure:"max_response_body_bytes"`

	// StorageType selects the backend: "file" (default) or "s3".
	StorageType string `mapstructure:"storage_type"`
	// S3 settings (used when StorageType == "s3").
	S3Endpoint  string `mapstructure:"s3_endpoint"`
	S3AccessKey string `mapstructure:"s3_access_key"`
	S3SecretKey string `mapstructure:"s3_secret_key"`
	S3Bucket    string `mapstructure:"s3_bucket"`
	S3Prefix    string `mapstructure:"s3_prefix"`
	S3UseSSL    bool   `mapstructure:"s3_use_ssl"`

	// RedactUploads replaces request bodies with a placeholder when the request
	// looks like a file upload (multipart or large binary payload).
	RedactUploads bool `mapstructure:"redact_uploads"`
	// RedactDownloads replaces response bodies with a placeholder for binary
	// downloads (application/octet-stream, etc.).
	RedactDownloads bool `mapstructure:"redact_downloads"`
	// RedactContentTypes lists content types whose bodies are always redacted.
	RedactContentTypes []string `mapstructure:"redact_content_types"`
	// RedactThresholdBytes redacts bodies larger than this (0 = disabled).
	RedactThresholdBytes int64 `mapstructure:"redact_threshold_bytes"`
}

// DefaultHeaderAllowlist is deliberately small and credential-free.
var DefaultHeaderAllowlist = []string{
	"Content-Type",
	"Content-Encoding",
	"Content-Length",
	"Accept",
	"User-Agent",
	"SOAPAction",
	"MS-ASProtocolVersion",
	"X-Forwarded-For",
	"X-Real-IP",
	"X-Request-Id",
	"X-PiMail-Trace-Id",
}

const (
	defaultDir            = "/var/lib/pimail/trace"
	defaultGRPCEndpoint   = "127.0.0.1:9460"
	defaultMaxRequest     = 1 << 20 // 1 MiB
	defaultMaxResponse    = 2 << 20 // 2 MiB
	defaultMaxAge         = 7 * 24 * time.Hour
	defaultMaxBytes       = 1 << 30 // 1 GiB
	defaultQueueSize      = 4096
	defaultRotateInterval = time.Hour
	defaultRotateBytes    = 64 << 20 // 64 MiB
	defaultRingSize       = 500
)

func (c Config) withDefaults() Config {
	if c.Dir == "" {
		c.Dir = defaultDir
	}
	if c.GRPCEndpoint == "" {
		c.GRPCEndpoint = defaultGRPCEndpoint
	}
	if c.MaxRequestBytes <= 0 {
		c.MaxRequestBytes = defaultMaxRequest
	}
	if c.MaxResponseBytes <= 0 {
		c.MaxResponseBytes = defaultMaxResponse
	}
	if c.MaxAge <= 0 {
		c.MaxAge = defaultMaxAge
	}
	if c.MaxBytes <= 0 {
		c.MaxBytes = defaultMaxBytes
	}
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.SampleRatio <= 0 || c.SampleRatio > 1 {
		c.SampleRatio = 1
	}
	if c.RotateInterval <= 0 {
		c.RotateInterval = defaultRotateInterval
	}
	if c.RotateBytes <= 0 {
		c.RotateBytes = defaultRotateBytes
	}
	if len(c.HeaderAllowlist) == 0 {
		c.HeaderAllowlist = append([]string(nil), DefaultHeaderAllowlist...)
	}
	if c.HeaderNameMode == "" {
		c.HeaderNameMode = "enum"
	}
	if c.MaxRequestBodyBytes <= 0 {
		c.MaxRequestBodyBytes = 64 << 20
	}
	if c.MaxResponseBodyBytes <= 0 {
		c.MaxResponseBodyBytes = c.MaxResponseBytes
		if c.MaxResponseBodyBytes <= 0 {
			c.MaxResponseBodyBytes = 2 << 20
		}
	}
	if len(c.RedactContentTypes) == 0 {
		c.RedactContentTypes = []string{
			"application/octet-stream",
			"application/zip",
			"application/pdf",
			"image/",
			"video/",
			"audio/",
		}
	}
	return c
}

// ensureDir creates the trace root with owner-only permissions.
func ensureDir(dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	return os.Chmod(dir, 0o700)
}

func segmentDir(root string, t time.Time) string {
	return filepath.Join(root, t.UTC().Format("2006-01-02"))
}
