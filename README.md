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
- S3 (MinIO/silo): `WithS3Storage(...)` (one object per record; single writer)
- Remote (hackergate collector): `WithRemoteStorage(endpoint, insecure, trace.Source{...})`

## net/http middleware

`rec.HTTPMiddleware(opts...)` wraps any `http.Handler`; annotate exchanges via
`trace.InfoFromRequest(r)`. The client IP is the socket peer unless you pass
`trace.WithClientIP(...)` (forwarding headers are client-controlled).

## Interceptor (TLS-terminating recording proxy)

`cmd/hackergate-sniffer` terminates TLS, forwards to another host and records
everything. `-mode http` (default) is a reverse proxy (h2, SSE and WebSocket
pass through, trace records identical to the middleware); `-mode tcp` relays
any TLS stream and records one metadata record per connection (payload capture
is opt-in via `-capture-stream-payload`).

```
hackergate-sniffer -listen :8443 -cert tls.crt -key tls.key \
    -upstream https://backend:443 -trace-dir /var/lib/pitrace
```

Library use: `intercept.New(cfg, rec, logger)` then `Run(ctx)`.

## hackergate pipeline (sidecar → collector → MongoDB + MinIO)

```
 pod ──► hackergate-sniffer ──gRPC stream──► hackergate-collector (N replicas) ─┬─► MinIO/S3  (bodies, streamed)
         (RemoteStorage)                       stateless, scale on demand        └─► MongoDB   (metadata, headers, search)
```

- **Wire protocol:** `api/proto/trace/v1/ingest.proto`. One long-lived
  `IngestService.Push` stream per sidecar: `hello` (pod identity), then per
  exchange an `ExchangeBegin` (everything but the bodies) followed by 64 KiB
  body chunks. The collector acks each committed exchange.
- **Streaming, no whole-body buffering:** gRPC's 4 MiB limit is per message,
  not per stream, so bodies travel as 64 KiB chunks. Opaque bodies (not
  JSON/form/XML/text, no `Content-Encoding`) larger than `-stream-threshold`
  (256 KiB) are teed to the collector on their own `IngestService.PutBody`
  stream *while they pass through the sniffer*, and the collector pipes them
  straight into MinIO (multipart, 8 MiB parts, up to `-max-stream-bytes`,
  default 1 GiB). Sniffer memory stays flat (200 MB each way measured at
  ~30 MB RSS). The exchange then references the object (`*_body_uploaded`) and
  the collector verifies it exists with the right size. If the collector
  cannot keep up for ~250 ms, the upload is abandoned: the user's traffic is
  never slowed, and the exchange is stored with the first bytes and
  `*_truncated`. Bodies that need masking, decompression or redaction (JSON,
  forms, text, gzip) are still buffered up to `max_request_bytes` /
  `max_response_bytes`, because secrets must be masked before anything leaves
  the sniffer. Use `-max-request-body` (default unlimited) to cap uploads.
  Small bodies stay inline on `Push`.
- **Delivery:** at-least-once. The sidecar keeps up to 256 unacked exchanges in
  memory and re-sends them after a reconnect; the collector deduplicates by
  trace id (body keys are deterministic, so a resend overwrites identically).
  When the window is full `Write` fails with `ErrCollectorBusy` and the
  recorder counts a write error (a disk spool is the next step).
- **Collector:** bodies are piped chunk by chunk into MinIO as they arrive
  (`bodies/YYYY/MM/DD/<trace>.req|resp`), then the metadata document is
  inserted in MongoDB; a failed insert deletes the uploaded objects. It holds
  no state, so add replicas behind any gRPC load balancer; `grpc.health` flips
  to NOT_SERVING when a backend is down.
- **MongoDB** (`_id` = trace id): flattened fields (`started_at`, `protocol`,
  `account_id`, `status`, `path`, …), `label_set` (`"k=v"`, indexed), `req_headers`
  / `resp_headers` as `{name,value}` arrays, a lowercase `text` field for
  search, a TTL index on `expire_at` (`-retention`), and the proto bytes.
  Text search is an unindexed case-insensitive regex that only runs after the
  indexed time/field filters; move to Atlas Search/`$text` if that becomes hot.
- **MinIO:** a lifecycle rule on `bodies/` expires bodies after `-retention`
  (it replaces the bucket's lifecycle config; use a dedicated bucket).
- **Reads:** the collector serves `TraceService` (`ListTraces`, `GetTrace`) and
  `BodyService.GetBody` (streamed). `GetTrace(include_bodies=true)` only inlines
  up to 3 MiB of bodies and otherwise answers `ResourceExhausted`; larger
  bodies are read with `GetBody` (`RemoteStorage.Read` does this itself).
  `TailTraces` is not implemented there yet.

```
MONGODB_URI=mongodb://mongo:27017 MINIO_ENDPOINT=minio:9000 \
MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... \
hackergate-collector -listen :9470 -bucket hackergate -retention 168h

HACKERGATE_NAMESPACE=shop HACKERGATE_POD=cart-1 \
hackergate-sniffer -listen :8443 -cert tls.crt -key tls.key \
    -upstream http://127.0.0.1:8080 -collector collector.hackergate:9470
```

`hackergate-collector -memory` runs without MongoDB/MinIO for development.
The Mongo/MinIO integration tests run when `MONGO_URI` / `MINIO_ENDPOINT`
(+ `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`) are set.

The product is called hackergate; the Go module path and the proto package
(`pitrace.trace.v1`) keep their names for compatibility with existing importers.

## Where traces live, and the `hackergate` CLI

With many services (each with a sniffer sidecar), do **not** use file or S3
storage per service: traces would be scattered and unsearchable. Run every
sniffer with remote storage pointing at one shared collector deployment. All
exchanges land in one MongoDB (metadata/search) and one MinIO bucket (bodies);
each record is stamped with `k8s.namespace`, `k8s.pod` and workload labels, so
one query spans all services and `-label k8s.namespace=shop` narrows it. File
storage is for single-process dev; S3 is single-writer.

The `hackergate` CLI queries the collector:

```
export HACKERGATE_COLLECTOR=collector.hackergate:9470   # add -ca-file / -insecure
hackergate list -since 1h -status 5xx -label k8s.namespace=shop
hackergate get <trace-id> [-bodies] [-o json]
hackergate body <trace-id> -kind req|resp > body.bin   # gunzips unless -raw
hackergate curl <trace-id>                              # replayable curl
```

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