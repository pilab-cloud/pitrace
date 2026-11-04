# pitrace

<!-- badges:start -->
[![Go Reference](https://pkg.go.dev/badge/go.pilab.hu/cloud/pitrace.svg)](https://pkg.go.dev/go.pilab.hu/cloud/pitrace)
[![Go Report Card](https://goreportcard.com/badge/github.com/pilab-cloud/pitrace)](https://goreportcard.com/report/github.com/pilab-cloud/pitrace)
[![CI](https://github.com/pilab-cloud/pitrace/actions/workflows/ci.yml/badge.svg)](https://github.com/pilab-cloud/pitrace/actions/workflows/ci.yml)
[![CodeQL](https://github.com/pilab-cloud/pitrace/actions/workflows/codeql.yml/badge.svg)](https://github.com/pilab-cloud/pitrace/actions/workflows/codeql.yml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/pilab-cloud/pitrace/badge)](https://scorecard.dev/viewer/?uri=github.com/pilab-cloud/pitrace)
[![Release](https://img.shields.io/github/v/release/pilab-cloud/pitrace)](https://github.com/pilab-cloud/pitrace/releases)
[![Coverage](https://codecov.io/gh/pilab-cloud/pitrace/branch/main/graph/badge.svg)](https://codecov.io/gh/pilab-cloud/pitrace)
[![License](https://img.shields.io/github/license/pilab-cloud/pitrace)](https://github.com/pilab-cloud/pitrace/blob/main/LICENSE)
[![Go Version](https://img.shields.io/badge/Go-1.23+-00ADD8?logo=go)](https://go.dev/dl/)
[![Website](https://img.shields.io/badge/website-pilab.hu-blue)](https://pilab.hu)
<!-- badges:end -->

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

## Documentation

Full API reference: [docs/api.md](docs/api.md). Generated Go documentation is
available via `go doc ./...` and (once published) on pkg.go.dev.