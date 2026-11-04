package trace

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// MiddlewareOptions adapts the recorder to one daemon.
type MiddlewareOptions struct {
	// ProtocolResolver maps a request path to a protocol when the handler did
	// not call SetProtocol. Example: /EWS/Exchange.asmx -> "ews".
	ProtocolResolver func(path string) string
	// SkipPaths are prefixes that are never recorded (health checks, metrics).
	SkipPaths []string
	// Now is injectable for tests.
	Now func() time.Time
}

// Option mutates MiddlewareOptions.
type Option func(*MiddlewareOptions)

// WithProtocolResolver supplies the path-to-protocol mapping.
func WithProtocolResolver(fn func(path string) string) Option {
	return func(o *MiddlewareOptions) { o.ProtocolResolver = fn }
}

// WithSkipPaths excludes noisy endpoints from the trace.
func WithSkipPaths(prefixes ...string) Option {
	return func(o *MiddlewareOptions) { o.SkipPaths = append(o.SkipPaths, prefixes...) }
}

// Middleware records one exchange per request. It never blocks the handler:
// captured payloads are bounded and records are handed to an async writer
// which drops them when the queue is full.
func (r *Recorder) Middleware(opts ...Option) echo.MiddlewareFunc {
	if r == nil {
		return func(next echo.HandlerFunc) echo.HandlerFunc { return next }
	}
	o := MiddlewareOptions{Now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	if o.Now == nil {
		o.Now = time.Now
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			if !r.Enabled() || skipped(c.Request().URL.Path, o.SkipPaths) {
				return next(c)
			}

			started := o.Now()
			info := &Info{}
			c.Set(infoContextKey, info)

			var reqCapture *bodyCapture
			if body := c.Request().Body; body != nil && body != http.NoBody {
				// Cap the request body so a huge upload cannot exhaust memory.
				limited := http.MaxBytesReader(c.Response().Writer, body, r.cfg.MaxRequestBodyBytes)
				reqCapture = newBodyCapture(limited, r.cfg.MaxRequestBytes)
				c.Request().Body = reqCapture
			}

			capture := newResponseCapture(c.Response().Writer, r.cfg.MaxResponseBodyBytes)
			c.Response().Writer = capture

			traceID := uuid.NewString()
			c.Response().Header().Set(TraceIDHeader, traceID)

			var handlerErr error
			defer func() {
				if p := recover(); p != nil {
					handlerErr = fmt.Errorf("panic: %v", p)
					r.finish(c, info, traceID, started, o, reqCapture, capture, handlerErr)
					panic(p)
				}
			}()

			handlerErr = next(c)
			r.finish(c, info, traceID, started, o, reqCapture, capture, handlerErr)
			return handlerErr
		}
	}
}

func skipped(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if p != "" && strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// finish builds the record and hands it to the writer.
func (r *Recorder) finish(c echo.Context, info *Info, traceID string, started time.Time, o MiddlewareOptions, reqCapture *bodyCapture, capture *responseCapture, handlerErr error) {
	if !r.shouldRecord(info.Operation) {
		return
	}
	protocol := info.Protocol
	if protocol == "" && o.ProtocolResolver != nil {
		protocol = o.ProtocolResolver(c.Request().URL.Path)
	}
	if protocol == "" {
		protocol = "http"
	}

	req := c.Request()
	clientIP := c.RealIP()
	if clientIP == "" {
		clientIP = req.RemoteAddr
	}
	client := &tracev1.ClientInfo{
		UserAgent:    req.UserAgent(),
		RemoteAddr:   req.RemoteAddr,
		RealIp:       c.RealIP(),
		ForwardedFor: req.Header.Get("X-Forwarded-For"),
		Version:      info.ClientVersion,
	}
	client.Fingerprint = fingerprint(client.UserAgent, client.Version)

	sessionID := sessionKey(info.AccountID, info.Principal, clientIP, protocol, client.Fingerprint)
	seq := r.nextSessionSeq(sessionID)

	status := capture.statusCode()
	if s := c.Response().Status; s != 0 {
		status = s
	}

	summary := &tracev1.TraceSummary{
		TraceId:       traceID,
		StartedAt:     timestamppb.New(started),
		DurationMs:    o.Now().Sub(started).Milliseconds(),
		Protocol:      protocol,
		Operation:     info.Operation,
		AccountId:     info.AccountID,
		Principal:     info.Principal,
		Method:        req.Method,
		Path:          req.URL.Path,
		Status:        int32(status),
		UserAgent:     client.UserAgent,
		ClientIp:      clientIP,
		ResponseCodes: info.ResponseCodes,
		SessionId:     sessionID,
		SessionSeq:    seq,
		Streamed:      info.Streamed,
		Labels:        info.Labels,
	}
	if handlerErr != nil {
		summary.Error = handlerErr.Error()
	}

	ex := &tracev1.HTTPExchange{
		SchemaVersion:   1,
		Summary:         summary,
		Query:           req.URL.RawQuery,
		ContentType:     req.Header.Get("Content-Type"),
		Client:          client,
		RequestHeaders:  selectHeaders(req.Header, r.cfg.HeaderAllowlist, r.cfg.HeaderNameMode),
		ResponseHeaders: selectHeaders(c.Response().Header(), r.cfg.HeaderAllowlist, r.cfg.HeaderNameMode),
	}
	if reqCapture != nil {
		body := reqCapture.bytes()
		enc := req.Header.Get("Content-Encoding")
		ex.RequestContentEncoding = enc
		// Store compressed bodies uncompressed.
		if enc != "" {
			if dec, err := decompressBody(body, enc); err == nil {
				body = dec
			}
		}
		redacted := r.shouldRedact(req.Header.Get("Content-Type"), reqCapture.total, r.cfg, true)
		if redacted {
			body = []byte("[redacted request body]")
			summary.RequestRedacted = true
		}
		// Gzip large bodies to save space.
		if !redacted && len(body) > 4096 {
			if gz, err := gzipBytes(body); err == nil {
				body = gz
				ex.RequestBodyGzipped = true
			}
		}
		ex.RequestBody = body
		summary.RequestTruncated = reqCapture.truncated
		summary.RequestBytes = reqCapture.total
		if req.ContentLength > 0 && reqCapture.total < req.ContentLength {
			summary.RequestTruncated = true
		} else if req.ContentLength < 0 && reqCapture.total == 0 {
			summary.RequestTruncated = true
		}
		if !summary.RequestTruncated && !redacted && !ex.RequestBodyGzipped {
			ex.RequestSha256 = hashHex(ex.RequestBody)
		}
	}
	respBody := capture.bytes()
	respEnc := c.Response().Header().Get("Content-Encoding")
	ex.ResponseContentEncoding = respEnc
	if respEnc != "" {
		if dec, err := decompressBody(respBody, respEnc); err == nil {
			respBody = dec
		}
	}
	respRedacted := r.shouldRedact(c.Response().Header().Get("Content-Type"), capture.total, r.cfg, false)
	if respRedacted {
		respBody = []byte("[redacted response body]")
		summary.ResponseRedacted = true
	}
	if !respRedacted && len(respBody) > 4096 {
		if gz, err := gzipBytes(respBody); err == nil {
			respBody = gz
			ex.ResponseBodyGzipped = true
		}
	}
	ex.ResponseBody = respBody
	summary.ResponseTruncated = capture.truncated
	summary.ResponseBytes = capture.total
	if !capture.truncated && !respRedacted && !ex.ResponseBodyGzipped {
		ex.ResponseSha256 = hashHex(ex.ResponseBody)
	}

	r.Record(ex)
}

// bodyCapture tees a request body into a bounded buffer as the handler reads
// it, so the middleware never consumes the body itself.
type bodyCapture struct {
	rc        io.ReadCloser
	max       int64
	buf       bytes.Buffer
	total     int64
	truncated bool
}

func newBodyCapture(rc io.ReadCloser, max int64) *bodyCapture {
	return &bodyCapture{rc: rc, max: max}
}

func (b *bodyCapture) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.total += int64(n)
		if remaining := b.max - int64(b.buf.Len()); remaining > 0 {
			take := int64(n)
			if take > remaining {
				take = remaining
			}
			_, _ = b.buf.Write(p[:take])
			if take < int64(n) {
				b.truncated = true
			}
		} else {
			b.truncated = true
		}
	}
	return n, err
}

func (b *bodyCapture) Close() error  { return b.rc.Close() }
func (b *bodyCapture) bytes() []byte { return b.buf.Bytes() }

// responseCapture tees the response into a bounded buffer while forwarding
// Write/Flush/Unwrap, so SSE and streaming handlers keep working.
type responseCapture struct {
	w           http.ResponseWriter
	max         int64
	buf         bytes.Buffer
	total       int64
	truncated   bool
	status      int
	wroteHeader bool
}

func newResponseCapture(w http.ResponseWriter, max int64) *responseCapture {
	return &responseCapture{w: w, max: max}
}

func (w *responseCapture) Header() http.Header { return w.w.Header() }

func (w *responseCapture) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
	}
	w.w.WriteHeader(code)
}

func (w *responseCapture) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.w.Write(p)
	if n > 0 {
		w.total += int64(n)
		if remaining := w.max - int64(w.buf.Len()); remaining > 0 {
			take := int64(n)
			if take > remaining {
				take = remaining
			}
			_, _ = w.buf.Write(p[:take])
			if take < int64(n) {
				w.truncated = true
			}
		} else {
			w.truncated = true
		}
	}
	return n, err
}

func (w *responseCapture) Flush() {
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
}

// Unwrap lets http.ResponseController reach the real writer.
func (w *responseCapture) Unwrap() http.ResponseWriter { return w.w }

// Hijack forwards connection upgrades when the underlying writer supports it.
func (w *responseCapture) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.w.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	return hj.Hijack()
}

func (w *responseCapture) statusCode() int {
	if w.status != 0 {
		return w.status
	}
	return http.StatusOK
}

func (w *responseCapture) bytes() []byte { return w.buf.Bytes() }

func wellKnownHeader(name string) tracev1.WellKnownHeader {
	switch strings.ToLower(name) {
	case "content-type":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_TYPE
	case "content-encoding":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_ENCODING
	case "content-length":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_LENGTH
	case "accept":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_ACCEPT
	case "user-agent":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_USER_AGENT
	case "soapaction":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_SOAP_ACTION
	case "ms-asprotocolversion":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_MS_AS_PROTOCOL_VERSION
	case "x-forwarded-for":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_FORWARDED_FOR
	case "x-real-ip":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_REAL_IP
	case "x-request-id":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_REQUEST_ID
	case "x-pimail-trace-id":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_PIMAIL_TRACE_ID
	case "accept-encoding":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_ACCEPT_ENCODING
	case "authorization":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_AUTHORIZATION
	case "cookie":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_COOKIE
	case "set-cookie":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_SET_COOKIE
	case "host":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_HOST
	case "referer":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_REFERER
	case "cache-control":
		return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CACHE_CONTROL
	}
	return tracev1.WellKnownHeader_WELL_KNOWN_HEADER_UNSPECIFIED
}

func selectHeaders(h http.Header, allowlist []string, mode string) []*tracev1.Header {
	if len(h) == 0 {
		return nil
	}
	allowed := make(map[string]bool, len(allowlist))
	for _, name := range allowlist {
		allowed[strings.ToLower(name)] = true
	}
	out := make([]*tracev1.Header, 0, len(h))
	for name, values := range h {
		if !allowed[strings.ToLower(name)] {
			continue
		}
		for _, v := range values {
			hdr := &tracev1.Header{Value: v}
			if mode == "original" {
				hdr.Name = name
			} else {
				if wk := wellKnownHeader(name); wk != tracev1.WellKnownHeader_WELL_KNOWN_HEADER_UNSPECIFIED {
					hdr.WellKnown = wk
				} else {
					hdr.Name = name
				}
			}
			out = append(out, hdr)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		ni, nj := out[i].GetName(), out[j].GetName()
		if out[i].GetWellKnown() != 0 {
			ni = out[i].GetWellKnown().String()
		}
		if out[j].GetWellKnown() != 0 {
			nj = out[j].GetWellKnown().String()
		}
		if ni == nj {
			return out[i].Value < out[j].Value
		}
		return ni < nj
	})
	return out
}

func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decompressBody(b []byte, enc string) ([]byte, error) {
	enc = strings.ToLower(strings.TrimSpace(enc))
	switch enc {
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case "deflate":
		// deflate: try raw deflate
		return b, nil
	default:
		return b, nil
	}
}

func hashHex(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func fingerprint(ua, version string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(ua)) + "|" + strings.TrimSpace(version)))
	return hex.EncodeToString(sum[:8])
}

func sessionKey(accountID, principal, ip, protocol, fingerprint string) string {
	base := accountID
	if base == "" {
		base = principal
	}
	if base == "" {
		base = protocol + "|" + ip
	}
	sum := sha256.Sum256([]byte(base + "|" + fingerprint + "|" + ip))
	return hex.EncodeToString(sum[:8])
}

func (r *Recorder) shouldRecord(operation string) bool {
	if r.cfg.SampleRatio >= 1 {
		return true
	}
	for _, op := range r.cfg.AlwaysOperations {
		if strings.EqualFold(op, operation) {
			return true
		}
	}
	return rand.Float64() < r.cfg.SampleRatio //nolint:gosec // non-cryptographic sampling is acceptable here
}

func (r *Recorder) nextSessionSeq(sessionID string) uint32 {
	r.seqMu.Lock()
	defer r.seqMu.Unlock()
	r.seq[sessionID]++
	if len(r.seq) > 10000 {
		// A long-running gateway sees many clients; resetting keeps the map
		// bounded. Sequence numbers are diagnostic, not a strict log index.
		r.seq = make(map[string]uint32)
	}
	return r.seq[sessionID]
}
