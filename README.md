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
- Echo and net/http middleware for HTTP capture
- Remote storage client + wire protocols for a hackergate collector
- `curl` package: render a recorded exchange as a curl command

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
- S3 (MinIO/silo): `WithS3Storage(...)` (one object per record; single writer)
- Remote (hackergate collector): `WithRemoteStorage(endpoint, insecure, trace.Source{...})`

## net/http middleware

`rec.HTTPMiddleware(opts...)` wraps any `http.Handler`; annotate exchanges via
`trace.InfoFromRequest(r)`. The client IP is the socket peer unless you pass
`trace.WithClientIP(...)` (forwarding headers are client-controlled).

## Hackergate pipeline (sidecar, collector, injector, CLI)

The product components built on this library (the `hgsniffer` recording
proxy, the `hgtrace-collector`, the admission-webhook injector and
`hgctl trace`) live in the hackergate repository, see its `docs/trace.md`.
This module provides what they share: the recorder, redaction, the
`RemoteStorage` client for a collector, and the wire protocols
(`api/proto/trace/v1/trace.proto` and `ingest.proto`: `IngestService.Push` /
`PutBody`, `TraceService`, `BodyService`).

Use `WithRemoteStorage(endpoint, insecure, trace.Source{...})` to stream
records to a collector. Delivery is at-least-once with a bounded in-memory
window; when it is full `Write` fails with `ErrCollectorBusy`.

## Redaction
- Secrets are masked by default: values of JSON/form fields and query
  parameters whose name contains `password`, `token`, `secret`, `api_key`,
  `authorization`, … (`DefaultRedactKeys`) are stored as `[redacted]`, in
  requests and responses, including truncated bodies. Tune with
  `WithRedactKeys(...)` or switch off with `DisableKeyRedaction`.
- `RedactUploads` / `RedactDownloads` redact multipart and binary
  (`image/`, `video/`, `audio/`, zip, pdf, octet-stream) bodies.
- `RedactContentTypes` and `RedactThresholdBytes` always apply.

## Metadata & search
Handlers attach labels via `trace.AddLabel(c, key, value)`. Labels are stored
in the index and searchable via `ListTracesRequest.labels`.

## Module

`go.pilab.hu/cloud/pitrace`

## Documentation

Full API reference: [docs/api.md](docs/api.md). Generated Go documentation is
available via `go doc ./...` and (once published) on pkg.go.dev.