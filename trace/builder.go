package trace

import (
	"time"

	"github.com/rs/zerolog"
)

// Builder constructs a Recorder with a fluent API.
type Builder struct {
	cfg    Config
	logger zerolog.Logger
}

// NewBuilder returns a builder with sensible defaults.
func NewBuilder() *Builder {
	return &Builder{
		cfg:    Config{Enabled: true},
		logger: zerolog.Nop(),
	}
}

func (b *Builder) WithConfig(cfg Config) *Builder { b.cfg = cfg; return b }
func (b *Builder) WithLogger(l zerolog.Logger) *Builder { b.logger = l; return b }
func (b *Builder) WithDir(dir string) *Builder { b.cfg.Dir = dir; return b }
func (b *Builder) WithEnabled(v bool) *Builder { b.cfg.Enabled = v; return b }
func (b *Builder) WithMaxAge(d time.Duration) *Builder { b.cfg.MaxAge = d; return b }
func (b *Builder) WithMaxBytes(n int64) *Builder { b.cfg.MaxBytes = n; return b }
func (b *Builder) WithQueueSize(n int) *Builder { b.cfg.QueueSize = n; return b }
func (b *Builder) WithSampleRatio(r float64) *Builder { b.cfg.SampleRatio = r; return b }
func (b *Builder) WithAlwaysOperations(ops ...string) *Builder { b.cfg.AlwaysOperations = ops; return b }
func (b *Builder) WithHeaderAllowlist(h ...string) *Builder { b.cfg.HeaderAllowlist = h; return b }
func (b *Builder) WithRedactUploads(v bool) *Builder { b.cfg.RedactUploads = v; return b }
func (b *Builder) WithRedactDownloads(v bool) *Builder { b.cfg.RedactDownloads = v; return b }
func (b *Builder) WithRedactContentTypes(ct ...string) *Builder { b.cfg.RedactContentTypes = ct; return b }
func (b *Builder) WithRedactThresholdBytes(n int64) *Builder { b.cfg.RedactThresholdBytes = n; return b }

// File backend (default)
func (b *Builder) WithFileStorage() *Builder {
	b.cfg.StorageType = "file"
	return b
}

// S3 backend (MinIO / silo)
func (b *Builder) WithS3Storage(endpoint, accessKey, secretKey, bucket, prefix string, useSSL bool) *Builder {
	b.cfg.StorageType = "s3"
	b.cfg.S3Endpoint = endpoint
	b.cfg.S3AccessKey = accessKey
	b.cfg.S3SecretKey = secretKey
	b.cfg.S3Bucket = bucket
	b.cfg.S3Prefix = prefix
	b.cfg.S3UseSSL = useSSL
	return b
}

// Build creates the Recorder.
func (b *Builder) Build() (*Recorder, error) {
	return NewRecorder(b.cfg, b.logger)
}

// BuildService creates a Recorder and wraps it in a Service.
func (b *Builder) BuildService() (*Recorder, *Service, error) {
	rec, err := NewRecorder(b.cfg, b.logger)
	if err != nil {
		return nil, nil, err
	}
	return rec, NewService(rec), nil
}
