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
	// ClientIP resolves the client address for net/http middleware. Defaults
	// to the host part of RemoteAddr: forwarding headers are client-controlled
	// and are only trusted when the caller opts in by supplying a resolver.
	ClientIP func(*http.Request) string
	// SuppressTraceHeader stops the middleware from adding TraceIDHeader to
	// responses (used by transparent interceptors).
	SuppressTraceHeader bool
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

// WithClientIP supplies the client address resolver used by HTTPMiddleware.
func WithClientIP(fn func(*http.Request) string) Option {
	return func(o *MiddlewareOptions) { o.ClientIP = fn }
}

// WithoutTraceHeader keeps the trace id out of responses.
func WithoutTraceHeader() Option {
	return func(o *MiddlewareOptions) { o.SuppressTraceHeader = true }
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

			traceID := uuid.NewString()
			reqCapture := r.newRequestCapture(c.Response().Writer, c.Request(), traceID, started)
			if reqCapture != nil {
				c.Request().Body = reqCapture
			}
			capture := r.newResponseCapture(c.Response().Writer, traceID, started)
			c.Response().Writer = capture

			if !o.SuppressTraceHeader {
				c.Response().Header().Set(r.cfg.TraceIDHeader, traceID)
			}

			var handlerErr error
			defer func() {
				if p := recover(); p != nil {
					handlerErr = fmt.Errorf("panic: %v", p)
					r.finishEcho(c, info, traceID, started, o, reqCapture, capture, handlerErr)
					panic(p)
				}
			}()

			handlerErr = next(c)
			r.finishEcho(c, info, traceID, started, o, reqCapture, capture, handlerErr)
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

func (r *Recorder) finishEcho(c echo.Context, info *Info, traceID string, started time.Time, o MiddlewareOptions, reqCapture *bodyCapture, capture *responseCapture, handlerErr error) {
	status := capture.statusCode()
	if s := c.Response().Status; s != 0 {
		status = s
	}
	r.finish(c.Request(), c.Response().Header(), status, c.RealIP(), info, traceID, started, o, reqCapture, capture, handlerErr)
}

// finish builds the record and hands it to the writer. It is shared by the
// Echo and net/http middlewares.
func (r *Recorder) finish(req *http.Request, respHeader http.Header, status int, realIP string, info *Info, traceID string, started time.Time, o MiddlewareOptions, reqCapture *bodyCapture, capture *responseCapture, handlerErr error) {
	if !r.shouldRecord(info.Operation) {
		if reqCapture != nil {
			reqCapture.abort()
		}
		capture.abort()
		return
	}
	protocol := info.Protocol
	if protocol == "" && o.ProtocolResolver != nil {
		protocol = o.ProtocolResolver(req.URL.Path)
	}
	if protocol == "" {
		protocol = "http"
	}

	clientIP := realIP
	if clientIP == "" {
		clientIP = req.RemoteAddr
	}
	client := &tracev1.ClientInfo{
		UserAgent:    req.UserAgent(),
		RemoteAddr:   req.RemoteAddr,
		RealIp:       realIP,
		ForwardedFor: req.Header.Get("X-Forwarded-For"),
		Version:      info.ClientVersion,
	}
	client.Fingerprint = fingerprint(client.UserAgent, client.Version)

	sessionID := sessionKey(info.AccountID, info.Principal, clientIP, protocol, client.Fingerprint)
	seq := r.nextSessionSeq(sessionID)

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
	} else if info.Error != "" {
		summary.Error = info.Error
	}

	ex := &tracev1.HTTPExchange{
		SchemaVersion:   1,
		Summary:         summary,
		Query:           r.mask.maskQuery(req.URL.RawQuery),
		ContentType:     req.Header.Get("Content-Type"),
		Client:          client,
		RequestHeaders:  selectHeaders(req.Header, r.cfg.HeaderAllowlist, r.cfg.HeaderNameMode),
		ResponseHeaders: selectHeaders(respHeader, r.cfg.HeaderAllowlist, r.cfg.HeaderNameMode),
	}
	if reqCapture != nil && reqCapture.streaming() {
		// Opaque body streamed to the collector: the first bytes stay as the
		// fallback payload, the rest is already on its way.
		reqCapture.spill.up.Close()
		ex.RequestBody = reqCapture.bytes()
		summary.RequestBytes = reqCapture.total
		summary.RequestTruncated = req.ContentLength > 0 && reqCapture.total < req.ContentLength
	} else if reqCapture != nil {
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
		} else {
			body = r.mask.maskBody(req.Header.Get("Content-Type"), body)
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
	if capture.streaming() {
		capture.spill.up.Close()
		ex.ResponseContentEncoding = respHeader.Get("Content-Encoding")
		ex.ResponseBody = capture.bytes()
		summary.ResponseBytes = capture.total
		r.Record(ex)
		return
	}
	respBody := capture.bytes()
	respEnc := respHeader.Get("Content-Encoding")
	ex.ResponseContentEncoding = respEnc
	if respEnc != "" {
		if dec, err := decompressBody(respBody, respEnc); err == nil {
			respBody = dec
		}
	}
	respRedacted := r.shouldRedact(respHeader.Get("Content-Type"), capture.total, r.cfg, false)
	if respRedacted {
		respBody = []byte("[redacted response body]")
		summary.ResponseRedacted = true
	} else {
		respBody = r.mask.maskBody(respHeader.Get("Content-Type"), respBody)
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

// capBuf is the bounded in-memory capture of one body plus the optional spill
// that streams oversized, opaque bodies to the collector while they pass.
// While streaming, buf keeps only the first bytes: they are the fallback
// payload when the upload fails.
type capBuf struct {
	buf       bytes.Buffer
	max       int64
	total     int64
	truncated bool
	spill     bodySpill
}

func (c *capBuf) add(p []byte) {
	n := int64(len(p))
	c.total += n
	if c.spill.up != nil {
		if !c.spill.up.Write(p) {
			c.spill.up.Abort()
			c.spill.up, c.spill.failed = nil, true
			c.truncated = true
		}
		return
	}
	limit := c.max
	canSpill := c.spill.open != nil && !c.spill.failed
	if canSpill && c.spill.threshold < limit {
		limit = c.spill.threshold
	}
	take := max(0, min(n, limit-int64(c.buf.Len())))
	if take > 0 {
		_, _ = c.buf.Write(p[:take])
	}
	if take == n {
		return
	}
	if canSpill {
		if up := c.spill.open(); up != nil {
			if up.Write(c.buf.Bytes()) && up.Write(p[take:]) {
				c.spill.up = up
				return
			}
			up.Abort()
		}
		c.spill.failed = true
		// Streaming is unavailable: keep buffering up to the normal cap.
		if more := max(0, min(n-take, c.max-int64(c.buf.Len()))); more > 0 {
			_, _ = c.buf.Write(p[take : take+more])
			take += more
		}
	}
	if take < n {
		c.truncated = true
	}
}

func (c *capBuf) bytes() []byte { return c.buf.Bytes() }

// streaming reports whether the body is being stored through an upload.
func (c *capBuf) streaming() bool { return c.spill.up != nil }

// abort drops a running upload (the exchange will not be recorded).
func (c *capBuf) abort() {
	if c.spill.up != nil {
		c.spill.up.Abort()
		c.spill.up = nil
	}
}

// bodyCapture tees a request body into a bounded buffer as the handler reads
// it, so the middleware never consumes the body itself.
type bodyCapture struct {
	capBuf
	rc io.ReadCloser
}

func newBodyCapture(rc io.ReadCloser, max int64, spill bodySpill) *bodyCapture {
	return &bodyCapture{rc: rc, capBuf: capBuf{max: max, spill: spill}}
}

func (b *bodyCapture) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if n > 0 {
		b.add(p[:n])
	}
	return n, err
}

func (b *bodyCapture) Close() error { return b.rc.Close() }

// responseCapture tees the response into a bounded buffer while forwarding
// Write/Flush/Unwrap, so SSE and streaming handlers keep working.
type responseCapture struct {
	capBuf
	w           http.ResponseWriter
	status      int
	wroteHeader bool
	// spillFor is asked once, when the headers are final, whether the
	// response may be streamed.
	spillFor func(h http.Header) bodySpill
}

func newResponseCapture(w http.ResponseWriter, max int64, spillFor func(http.Header) bodySpill) *responseCapture {
	return &responseCapture{w: w, capBuf: capBuf{max: max}, spillFor: spillFor}
}

func (w *responseCapture) Header() http.Header { return w.w.Header() }

func (w *responseCapture) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status = code
		w.wroteHeader = true
		if w.spillFor != nil && code >= 200 && code != http.StatusSwitchingProtocols {
			w.spill = w.spillFor(w.w.Header())
		}
	}
	w.w.WriteHeader(code)
}

func (w *responseCapture) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.w.Write(p)
	if n > 0 {
		w.add(p[:n])
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

// newRequestCapture wraps a request body for capture; reqCapture is nil when
// the request has no body.
func (r *Recorder) newRequestCapture(w http.ResponseWriter, req *http.Request, traceID string, started time.Time) *bodyCapture {
	body := req.Body
	if body == nil || body == http.NoBody {
		return nil
	}
	if r.cfg.MaxRequestBodyBytes > 0 {
		// Cap the request body so a huge upload cannot exhaust memory. A
		// negative value disables the cap (proxies must not reject uploads).
		body = http.MaxBytesReader(w, body, r.cfg.MaxRequestBodyBytes)
	}
	eligible := r.streamable(req.Header.Get("Content-Type"), req.Header.Get("Content-Encoding"), true)
	return newBodyCapture(body, r.cfg.MaxRequestBytes, r.spillFor(eligible, traceID, started, tracev1.BodyKind_BODY_KIND_REQUEST))
}

func (r *Recorder) newResponseCapture(w http.ResponseWriter, traceID string, started time.Time) *responseCapture {
	return newResponseCapture(w, r.cfg.MaxResponseBodyBytes, func(h http.Header) bodySpill {
		eligible := r.streamable(h.Get("Content-Type"), h.Get("Content-Encoding"), false)
		return r.spillFor(eligible, traceID, started, tracev1.BodyKind_BODY_KIND_RESPONSE)
	})
}

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
