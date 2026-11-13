package trace

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

func TestMiddlewareCapturesExchange(t *testing.T) {
	rec := testRecorder(t, nil)
	e := echo.New()
	e.Use(rec.Middleware(WithProtocolResolver(func(path string) string {
		if strings.Contains(path, "/EWS/") {
			return "ews"
		}
		return "http"
	})))
	e.POST("/EWS/Exchange.asmx", func(c echo.Context) error {
		body, _ := io.ReadAll(c.Request().Body)
		SetOperation(c, "CreateItem")
		SetAccount(c, "acc-1", "gyula@pilab.hu")
		SetClientVersion(c, "Exchange2013_SP1")
		SetResponseCodes(c, []string{"NoError", "NoError"})
		c.Response().Header().Set("Content-Type", "text/xml; charset=utf-8")
		return c.XMLBlob(http.StatusOK, []byte(`<response>`+string(body)+`</response>`))
	})

	req := httptest.NewRequest(http.MethodPost, "/EWS/Exchange.asmx?x=1", strings.NewReader("<request/>"))
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set(userAgentHeader, "MacOutlook/16.107")
	req.Header.Set("Authorization", "Basic c2VjcmV0")
	req.Header.Set("SOAPAction", "CreateItem")
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	traceID := recorder.Header().Get(TraceIDHeader)
	require.NotEmpty(t, traceID, "the trace id must be returned to the caller")

	require.Eventually(t, func() bool {
		list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return err == nil && len(list.GetTraces()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
	require.NoError(t, err)
	summary := list.GetTraces()[0]
	assert.Equal(t, traceID, summary.GetTraceId())
	assert.Equal(t, "ews", summary.GetProtocol())
	assert.Equal(t, "CreateItem", summary.GetOperation())
	assert.Equal(t, "acc-1", summary.GetAccountId())
	assert.Equal(t, "gyula@pilab.hu", summary.GetPrincipal())
	assert.Equal(t, int32(http.StatusOK), summary.GetStatus())
	assert.Equal(t, []string{"NoError"}, summary.GetResponseCodes())
	assert.Equal(t, "MacOutlook/16.107", summary.GetUserAgent())
	assert.NotEmpty(t, summary.GetSessionId())
	assert.Equal(t, uint32(1), summary.GetSessionSeq())
	assert.Equal(t, "Exchange2013_SP1", exClientVersion(t, rec, traceID))

	ex, err := rec.Get(context.Background(), traceID, true)
	require.NoError(t, err)
	assert.Equal(t, "<request/>", string(ex.GetRequestBody()))
	assert.Contains(t, string(ex.GetResponseBody()), "<response><request/></response>")
	assert.Equal(t, "x=1", ex.GetQuery())
	assert.Equal(t, "Exchange2013_SP1", ex.GetClient().GetVersion())
	assert.NotEmpty(t, ex.GetRequestSha256())
	assert.NotEmpty(t, ex.GetResponseSha256())

	// Credentials must never be copied into a trace.
	assert.Empty(t, findHeader(ex.GetRequestHeaders(), "Authorization"))
	assert.Equal(t, "CreateItem", findHeader(ex.GetRequestHeaders(), "SOAPAction"))
	assert.Equal(t, "example.com", findHeader(ex.GetRequestHeaders(), "Host"))
}

// A handler that rejects before reading the body (auth failure, bad content
// type) must not leave a trace that looks like the client sent nothing.
func TestMiddlewareMarksUnreadRequestBody(t *testing.T) {
	rec := testRecorder(t, nil)
	e := echo.New()
	e.Use(rec.Middleware())
	e.POST("/Microsoft-Server-ActiveSync", func(c echo.Context) error {
		SetProtocol(c, "eas")
		SetOperation(c, "FolderSync")
		return c.String(http.StatusUnauthorized, "unauthorized")
	})

	req := httptest.NewRequest(http.MethodPost, "/Microsoft-Server-ActiveSync?Cmd=FolderSync",
		strings.NewReader("not-read-by-the-handler"))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Eventually(t, func() bool {
		list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return len(list.GetTraces()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
	require.NoError(t, err)
	summary := list.GetTraces()[0]
	assert.Equal(t, int64(0), summary.GetRequestBytes())
	assert.True(t, summary.GetRequestTruncated(), "an unread body must be flagged, not reported as empty")

	ex, err := rec.Get(context.Background(), summary.GetTraceId(), true)
	require.NoError(t, err)
	assert.Empty(t, ex.GetRequestBody())
	assert.Empty(t, ex.GetRequestSha256(), "an unread body must not claim a full-content hash")
}

func TestMiddlewareTruncatesLargeBodies(t *testing.T) {
	rec := testRecorder(t, func(c *Config) {
		c.MaxRequestBytes = 4
		c.MaxResponseBytes = 4
	})
	e := echo.New()
	e.Use(rec.Middleware())
	e.POST("/upload", func(c echo.Context) error {
		_, _ = io.ReadAll(c.Request().Body)
		return c.String(http.StatusOK, "0123456789")
	})

	req := httptest.NewRequest(http.MethodPost, "/upload", strings.NewReader("abcdefghij"))
	recorder := httptest.NewRecorder()
	e.ServeHTTP(recorder, req)

	require.Eventually(t, func() bool {
		list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return len(list.GetTraces()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
	summary := list.GetTraces()[0]
	assert.True(t, summary.GetRequestTruncated())
	assert.True(t, summary.GetResponseTruncated())
	assert.Equal(t, int64(10), summary.GetRequestBytes(), "the byte count stays exact")
	assert.Equal(t, int64(10), summary.GetResponseBytes())

	ex, err := rec.Get(context.Background(), summary.GetTraceId(), true)
	require.NoError(t, err)
	assert.Equal(t, "abcd", string(ex.GetRequestBody()))
	assert.Equal(t, "0123", string(ex.GetResponseBody()))
	assert.Empty(t, ex.GetRequestSha256(), "a truncated body must not claim a full-content hash")
}

func TestMiddlewareSkipsConfiguredPaths(t *testing.T) {
	rec := testRecorder(t, nil)
	e := echo.New()
	e.Use(rec.Middleware(WithSkipPaths("/health")))
	e.GET("/health", func(c echo.Context) error { return c.NoContent(http.StatusOK) })
	e.GET("/mail", func(c echo.Context) error { return c.NoContent(http.StatusOK) })

	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/mail", nil))
	require.Eventually(t, func() bool {
		list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return len(list.GetTraces()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetTraces(), 1)
	assert.Equal(t, "/mail", list.GetTraces()[0].GetPath())
}

func TestMiddlewareSessionSequence(t *testing.T) {
	rec := testRecorder(t, nil)
	e := echo.New()
	e.Use(rec.Middleware())
	e.GET("/api", func(c echo.Context) error {
		SetAccount(c, "acc-1", "gyula@pilab.hu")
		return c.NoContent(http.StatusOK)
	})

	for i := 0; i < 3; i++ {
		e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/api", nil))
	}
	require.Eventually(t, func() bool {
		list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return len(list.GetTraces()) == 3
	}, 2*time.Second, 10*time.Millisecond)

	list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
	require.NoError(t, err)
	var seqs []uint32
	for _, s := range list.GetTraces() {
		seqs = append(seqs, s.GetSessionSeq())
	}
	// Newest first: 3, 2, 1.
	assert.Equal(t, []uint32{3, 2, 1}, seqs)
}

func headerCanonicalName(h *tracev1.Header) string {
	if h.GetName() != "" {
		return h.GetName()
	}
	switch h.GetWellKnown() {
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_TYPE:
		return "Content-Type"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_ENCODING:
		return "Content-Encoding"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_LENGTH:
		return "Content-Length"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_ACCEPT:
		return "Accept"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_USER_AGENT:
		return userAgentHeader
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_SOAP_ACTION:
		return "SOAPAction"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_MS_AS_PROTOCOL_VERSION:
		return "MS-ASProtocolVersion"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_FORWARDED_FOR:
		return "X-Forwarded-For"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_REAL_IP:
		return "X-Real-IP"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_REQUEST_ID:
		return "X-Request-Id"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_PIMAIL_TRACE_ID:
		return "X-PiMail-Trace-Id"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_ACCEPT_ENCODING:
		return "Accept-Encoding"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_AUTHORIZATION:
		return "Authorization"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_COOKIE:
		return "Cookie"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_SET_COOKIE:
		return "Set-Cookie"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_HOST:
		return "Host"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_REFERER:
		return "Referer"
	case tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CACHE_CONTROL:
		return "Cache-Control"
	}
	return ""
}

func findHeader(headers []*tracev1.Header, name string) string {
	for _, h := range headers {
		if strings.EqualFold(headerCanonicalName(h), name) {
			return h.GetValue()
		}
	}
	return ""
}

func exClientVersion(t *testing.T, rec *Recorder, traceID string) string {
	t.Helper()
	ex, err := rec.Get(context.Background(), traceID, false)
	require.NoError(t, err)
	return ex.GetClient().GetVersion()
}

func TestResponseCaptureFlushForwarding(t *testing.T) {
	under := httptest.NewRecorder()
	capture := newResponseCapture(under, 1024, nil)
	_, ok := any(capture).(http.Flusher)
	assert.True(t, ok, "streaming handlers require Flusher on the wrapped writer")
	capture.WriteHeader(http.StatusOK)
	_, _ = capture.Write([]byte("event\n"))
	capture.Flush()
	assert.Equal(t, "event\n", under.Body.String())
}

func TestRecorderSamplingAlwaysKeepsMutations(t *testing.T) {
	rec := testRecorder(t, func(c *Config) {
		c.SampleRatio = 1e-9
		c.AlwaysOperations = []string{"SendItem"}
	})
	assert.True(t, rec.shouldRecord("SendItem"))
	assert.False(t, rec.shouldRecord("FindItem"))
}

func TestNewServiceRejectsEmptyTraceID(t *testing.T) {
	svc := NewService(nil)
	_, err := svc.GetTrace(context.Background(), &tracev1.GetTraceRequest{})
	require.Error(t, err)
}

func TestMiddlewareCustomTraceIDHeader(t *testing.T) {
	rec := testRecorder(t, func(c *Config) { c.TraceIDHeader = "X-Request-Trace" })
	e := echo.New()
	e.Use(rec.Middleware())
	e.GET("/", func(c echo.Context) error { return c.String(http.StatusOK, "ok") })

	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	assert.NotEmpty(t, w.Header().Get("X-Request-Trace"))
	assert.Empty(t, w.Header().Get(TraceIDHeader))
}
