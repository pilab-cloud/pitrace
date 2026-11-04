# pitrace

Reusable request/response tracing module for Go services.

Extracted from pimail's trace subsystem. Provides:
- Async, bounded recorder writing compressed segments + indexes
- Live tail with filtered subscribers
- Retention pruning
- Loopback gRPC read API
- Echo middleware for HTTP capture

## Usage

```go
import "go.pilab.hu/cloud/pitrace/trace"

// Fluent builder
rec, svc, err := trace.NewBuilder().
    WithDir("/var/lib/pitrace").
    WithRedactUploads(true).
    WithRedactDownloads(true).
    WithS3Storage("silo:9000", "key", "secret", "traces", "pitrace", false).
    WithLogger(logger).
    BuildService()
// mount svc on gRPC server
// use rec.Middleware() in echo
```

## Storage backends
- File (shared/NFS): `WithFileStorage()` (default)
- S3 (MinIO/silo): `WithS3Storage(...)`

## Redaction
Uploads (multipart) and downloads (binary content types) can be redacted;
large bodies over threshold are also redacted.

## Metadata & search
Handlers attach labels via `trace.AddLabel(c, key, value)`. Labels are stored
in the index and searchable via `ListTracesRequest.labels`.

## Module

`go.pilab.hu/cloud/pitrace`
EOF
echo done