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

// WithConfig replaces the whole configuration.
func (b *Builder) WithConfig(cfg Config) *Builder { b.cfg = cfg; return b }

// WithLogger sets the logger used by the recorder and storage.
func (b *Builder) WithLogger(l zerolog.Logger) *Builder { b.logger = l; return b }

// WithDir sets the root directory of the trace segments.
func (b *Builder) WithDir(dir string) *Builder { b.cfg.Dir = dir; return b }

// WithTraceIDHeader sets the response header that carries the trace id.
func (b *Builder) WithTraceIDHeader(name string) *Builder { b.cfg.TraceIDHeader = name; return b }

// WithEnabled turns recording on or off.
func (b *Builder) WithEnabled(v bool) *Builder { b.cfg.Enabled = v; return b }

// WithMaxAge sets the maximum age of retained segments.
func (b *Builder) WithMaxAge(d time.Duration) *Builder { b.cfg.MaxAge = d; return b }

// WithMaxBytes sets the maximum total size of retained segments.
func (b *Builder) WithMaxBytes(n int64) *Builder { b.cfg.MaxBytes = n; return b }

// WithQueueSize sets the number of pending records before dropping.
func (b *Builder) WithQueueSize(n int) *Builder { b.cfg.QueueSize = n; return b }

// WithSampleRatio keeps a fraction of exchanges (1.0 = all).
func (b *Builder) WithSampleRatio(r float64) *Builder { b.cfg.SampleRatio = r; return b }

// WithAlwaysOperations lists operations that are always recorded.
func (b *Builder) WithAlwaysOperations(ops ...string) *Builder {
	b.cfg.AlwaysOperations = ops
	return b
}

// WithHeaderAllowlist sets the request/response headers copied into a record.
func (b *Builder) WithHeaderAllowlist(h ...string) *Builder { b.cfg.HeaderAllowlist = h; return b }

// WithRedactUploads enables redaction of request bodies that look like uploads.
func (b *Builder) WithRedactUploads(v bool) *Builder { b.cfg.RedactUploads = v; return b }

// WithRedactDownloads enables redaction of binary download responses.
func (b *Builder) WithRedactDownloads(v bool) *Builder { b.cfg.RedactDownloads = v; return b }

// WithRedactContentTypes lists content types whose bodies are always redacted.
func (b *Builder) WithRedactContentTypes(ct ...string) *Builder {
	b.cfg.RedactContentTypes = ct
	return b
}

// WithRedactKeys sets the field/parameter name substrings whose values are
// masked (default DefaultRedactKeys).
func (b *Builder) WithRedactKeys(keys ...string) *Builder {
	b.cfg.RedactKeys = keys
	return b
}

// WithRedactThresholdBytes redacts bodies larger than n (0 disables).
func (b *Builder) WithRedactThresholdBytes(n int64) *Builder {
	b.cfg.RedactThresholdBytes = n
	return b
}

// File backend (default)
func (b *Builder) WithFileStorage() *Builder {
	b.cfg.StorageType = "file"
	return b
}

// WithRemoteStorage streams exchanges to a hackergate collector.
func (b *Builder) WithRemoteStorage(endpoint string, insecure bool, src Source) *Builder {
	b.cfg.StorageType = "remote"
	b.cfg.CollectorEndpoint = endpoint
	b.cfg.CollectorInsecure = insecure
	b.cfg.Source = src
	return b
}

// WithCollectorCA verifies the collector against an extra CA bundle.
func (b *Builder) WithCollectorCA(path string) *Builder {
	b.cfg.CollectorCAFile = path
	return b
}

// WithMaxRequestBody caps request bodies (413 above it); a negative value
// removes the cap, 0 keeps the default.
func (b *Builder) WithMaxRequestBody(n int64) *Builder {
	b.cfg.MaxRequestBodyBytes = n
	return b
}

// WithStreamThreshold sets the size above which opaque bodies are streamed to
// the collector instead of being captured in memory.
func (b *Builder) WithStreamThreshold(n int64) *Builder {
	b.cfg.StreamThresholdBytes = n
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
