# pitrace API reference

Module: `go.pilab.hu/cloud/pitrace`

Packages:

- [`trace`](#package-trace) — recorder, storage backends, gRPC service, Echo middleware.
- [`curl`](#package-curl) — conversion between `HTTPExchange` and curl / `*http.Request`.
- [`gen/trace/v1`](#package-gentracev1) — generated protobuf/gRPC types.

---

## Package `trace`

Import: `go.pilab.hu/cloud/pitrace/trace`

### Constants

| Name | Value | Description |
|------|-------|-------------|
| `TraceIDHeader` | `X-Trace-Id` | Default trace-id response header name; override with `Config.TraceIDHeader` / `WithTraceIDHeader`. Quote it in bug reports. |

### Variables

| Name | Description |
|------|-------------|
| `DefaultHeaderAllowlist` | Default credential-free header allowlist. |
| `ErrTraceNotFound` | Returned when no index entry matches a trace id. |

### Functions (context setters)

Handlers call these through `echo.Context` to attach metadata to the in-flight exchange.

| Function | Description |
|----------|-------------|
| `InfoFrom(c) *Info` | Returns (or creates) the exchange info. |
| `SetProtocol(c, protocol)` | Names the protocol (`ews`, `jmap`, `smtp`, ...). |
| `SetOperation(c, operation)` | Names the operation inside the protocol. |
| `SetAccount(c, accountID, principal)` | Records authenticated account and principal. |
| `SetClientVersion(c, version)` | Records a protocol-supplied client version. |
| `SetResponseCodes(c, codes)` | Replaces protocol result codes. |
| `AddResponseCodes(c, codes...)` | Appends result codes, deduplicated. |
| `SetStreamed(c, streamed)` | Marks long-lived responses (SSE, streaming). |
| `SetLabels(c, labels)` | Replaces labels. |
| `AddLabel(c, key, value)` | Adds a single label. |

### `Config`

Controls the recorder. Zero values receive safe defaults via `withDefaults`.

Key fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Enabled` | `bool` | `false` | Turns recording on. |
| `Dir` | `string` | `/var/lib/pitrace` | Root of trace segments. |
| `TraceIDHeader` | `string` | `X-Trace-Id` | Response header carrying the trace id. |
| `GRPCEndpoint` | `string` | `127.0.0.1:9460` | Loopback read API address. |
| `MaxRequestBytes` | `int64` | 1 MiB | Cap kept per request body. |
| `MaxResponseBytes` | `int64` | 2 MiB | Cap kept per response body. |
| `MaxAge` | `time.Duration` | 7 days | Retention age bound. |
| `MaxBytes` | `int64` | 1 GiB | Retention size bound. |
| `QueueSize` | `int` | 4096 | Pending records before dropping. |
| `SampleRatio` | `float64` | 1 | Fraction of exchanges kept. |
| `AlwaysOperations` | `[]string` | — | Operations that ignore sampling. |
| `RotateInterval` | `time.Duration` | 1 hour | Segment rotation time bound. |
| `RotateBytes` | `int64` | 64 MiB | Segment rotation size bound. |
| `HeaderAllowlist` | `[]string` | `DefaultHeaderAllowlist` | Headers copied into a record. |
| `HeaderNameMode` | `string` | `enum` | `enum` or `original`. |
| `MaxRequestBodyBytes` | `int64` | 64 MiB | Hard cap on request body read. |
| `MaxResponseBodyBytes` | `int64` | `MaxResponseBytes` | Response capture buffer cap. |
| `StorageType` | `string` | `file` | `file` or `s3`. |
| `S3Endpoint` / `S3AccessKey` / `S3SecretKey` / `S3Bucket` / `S3Prefix` / `S3UseSSL` | — | S3 settings. |
| `RedactUploads` | `bool` | `false` | Redact multipart and binary request bodies. |
| `RedactDownloads` | `bool` | `false` | Redact binary response bodies. |
| `RedactContentTypes` | `[]string` | empty | Content types that are always redacted (on top of the built-in binary types the two flags enable). |
| `RedactKeys` | `[]string` | `DefaultRedactKeys` | Name substrings of JSON/form fields and query parameters whose values are masked as `[redacted]`. |
| `DisableKeyRedaction` | `bool` | `false` | Turn field/parameter masking off. |
| `RedactThresholdBytes` | `int64` | `0` | Redact bodies larger than this. |

### `Builder`

Fluent constructor. `NewBuilder() *Builder`.

Storage selection:

- `WithFileStorage()` — file backend (default).
- `WithS3Storage(endpoint, accessKey, secretKey, bucket, prefix string, useSSL bool)` — S3 backend.

Build:

- `Build() (*Recorder, error)`
- `BuildService() (*Recorder, *Service, error)`

All `With*` methods mirror the `Config` fields (`WithDir`, `WithEnabled`, `WithMaxAge`, `WithMaxBytes`, `WithQueueSize`, `WithSampleRatio`, `WithAlwaysOperations`, `WithHeaderAllowlist`, `WithRedactUploads`, `WithRedactDownloads`, `WithRedactContentTypes`, `WithRedactKeys`, `WithRedactThresholdBytes`, `WithConfig`, `WithLogger`).

### `Recorder`

| Method | Description |
|--------|-------------|
| `NewRecorder(cfg, logger) (*Recorder, error)` | Creates recorder; disabled config returns `nil`. |
| `Enabled() bool` | Whether recording is active. |
| `Config() Config` | Effective configuration. |
| `Dropped() uint64` | Records dropped due to full queue. |
| `WriteErrors() uint64` | Records that could not be persisted. |
| `Middleware(opts ...Option) echo.MiddlewareFunc` | Echo middleware capturing one exchange per request. |
| `Record(ex *tracev1.HTTPExchange)` | Enqueues one exchange (non-blocking). |
| `List(ctx, req) (*ListTracesResponse, error)` | Index summaries, newest first. |
| `Get(ctx, traceID, includeBodies) (*HTTPExchange, error)` | Loads one exchange. |
| `Tail(ctx, protocol, account, backlog) (<-chan *TraceSummary, func())` | Replays backlog then follows live. |
| `Prune(ctx, now) (int, int64, error)` | Delegates to storage. |
| `Close(ctx) error` | Flushes queue and closes segment. |

Middleware options:

- `WithProtocolResolver(fn func(path string) string)`
- `WithSkipPaths(prefixes ...string)`

### `Service` (gRPC read side)

`NewService(recorder *Recorder) *Service`. Implements `tracev1.TraceServiceServer`.

| Method | Description |
|--------|-------------|
| `ListTraces` | Metadata for matching exchanges. |
| `GetTrace` | One exchange, optional bodies. |
| `TailTraces` | Streaming live summaries. |

### `Storage` interface

Persistence backend contract:

```go
type Storage interface {
    Write(ctx, ex) error
    Read(ctx, traceID, includeBodies) (*HTTPExchange, error)
    List(ctx, req) (*ListTracesResponse, error)
    Prune(ctx, now) (removed int, freed int64, err error)
    Close(ctx) error
}
```

Implementations: `FileStorage`, `S3Storage`.

### `Info`

Protocol-neutral exchange context created by middleware and mutated by handlers.

Fields: `Protocol`, `Operation`, `AccountID`, `Principal`, `ClientVersion`, `ResponseCodes`, `Streamed`, `Labels`.

---

## Package `curl`

Import: `go.pilab.hu/cloud/pitrace/curl`

| Function | Description |
|----------|-------------|
| `ProtoToCurl(ex) string` | Renders an `HTTPExchange` as a replayable curl command. |
| `ProtoToRequest(ex) (*http.Request, error)` | Converts an `HTTPExchange` into `*http.Request`. |
| `RequestToProto(req, resp, reqBody, respBody) *HTTPExchange` | Converts an HTTP pair into an `HTTPExchange`. |
| `ParseCurl(cmd) (*http.Request, error)` | Best-effort parse of a curl command. |

---

## Package `gen/trace/v1`

Generated by buf/protoc. Contains `HTTPExchange`, `TraceSummary`, `ClientInfo`, `Header`, `WellKnownHeader`, and the gRPC `TraceServiceClient` / `TraceServiceServer` interfaces. See `api/proto/trace/v1/trace.proto` for the canonical schema.

### gRPC `TraceService`

- `ListTraces(ListTracesRequest) returns (ListTracesResponse)`
- `GetTrace(GetTraceRequest) returns (GetTraceResponse)`
- `TailTraces(TailTracesRequest) returns (stream TailTracesResponse)`

`ListTracesRequest` filters: `since`, `until`, `protocol`, `account_id`, `operation`, `min_status`, `max_status`, `session_id`, `text`, `limit` (default 100, cap 1000), `offset`, `labels`.

---

## Example

```go
rec, svc, err := trace.NewBuilder().
    WithDir("/var/lib/pitrace").
    WithRedactUploads(true).
    WithRedactDownloads(true).
    WithS3Storage("silo:9000", "key", "secret", "traces", "pitrace", false).
    WithLogger(logger).
    BuildService()
// mount svc on gRPC server
// e.Use(rec.Middleware())
```