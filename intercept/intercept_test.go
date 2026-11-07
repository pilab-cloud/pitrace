package intercept

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

func testRec(t *testing.T) *trace.Recorder {
	t.Helper()
	rec, err := trace.NewBuilder().WithEnabled(true).WithDir(t.TempDir()).Build()
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rec.Close(ctx)
	})
	return rec
}

func start(t *testing.T, cfg Config, rec *trace.Recorder) *Server {
	t.Helper()
	cfg.Listen = "127.0.0.1:0"
	srv, err := New(cfg, rec, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, srv.Listen())
	go func() { _ = srv.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv
}

func waitTrace(t *testing.T, rec *trace.Recorder) *tracev1.HTTPExchange {
	t.Helper()
	var id string
	require.Eventually(t, func() bool {
		list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		if err != nil || len(list.GetTraces()) == 0 {
			return false
		}
		id = list.GetTraces()[0].GetTraceId()
		return true
	}, 3*time.Second, 20*time.Millisecond)
	ex, err := rec.Get(context.Background(), id, true)
	require.NoError(t, err)
	return ex
}

func TestHTTPInterceptorTerminatesTLSAndRecords(t *testing.T) {
	var gotHost, gotXFF string
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost, gotXFF = r.Host, r.Header.Get("X-Forwarded-For")
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"echo":` + string(body) + `,"access_token":"tok-secret"}`))
	}))
	defer upstream.Close()

	rec := testRec(t)
	srv := start(t, Config{
		Upstream:                   upstream.URL,
		SelfSigned:                 true,
		UpstreamInsecureSkipVerify: true,
	}, rec)

	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // self-signed test cert
		ForceAttemptHTTP2: true,
	}}
	resp, err := client.Post("https://"+srv.Addr().String()+"/v1/login?api_key=qs-secret",
		"application/json", strings.NewReader(`{"user":"bob","password":"hunter2"}`))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// Transparent: the client still sees the real body and no trace header.
	assert.Contains(t, string(body), "tok-secret")
	assert.Empty(t, resp.Header.Get(trace.TraceIDHeader))
	assert.Equal(t, 2, resp.ProtoMajor, "client side negotiates h2")
	assert.Equal(t, srv.Addr().String(), gotHost, "client Host is preserved")
	assert.Contains(t, gotXFF, "127.0.0.1")

	ex := waitTrace(t, rec)
	assert.Equal(t, "/v1/login", ex.GetSummary().GetPath())
	assert.NotContains(t, ex.GetQuery(), "qs-secret")
	assert.NotContains(t, string(ex.GetRequestBody()), "hunter2")
	assert.NotContains(t, string(ex.GetResponseBody()), "tok-secret")
	labels := ex.GetSummary().GetLabels()
	assert.Equal(t, "1.3", labels["tls.version"])
	assert.NotEmpty(t, labels["upstream"])
}

func TestHTTPInterceptorUpstreamDown(t *testing.T) {
	rec := testRec(t)
	srv := start(t, Config{Upstream: "http://127.0.0.1:1", SelfSigned: true}, rec)
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}} //nolint:gosec // test
	resp, err := client.Get("https://" + srv.Addr().String() + "/x")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusBadGateway, resp.StatusCode)

	ex := waitTrace(t, rec)
	assert.Contains(t, ex.GetSummary().GetError(), "upstream")
	assert.EqualValues(t, 502, ex.GetSummary().GetStatus())
}

func pongServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { ln.Close() })
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		buf := make([]byte, 5)
		_, _ = io.ReadFull(c, buf)
		_, _ = c.Write([]byte("PONG:" + string(buf)))
	}()
	return ln.Addr().String()
}

func TestTCPInterceptorRelaysAndRecords(t *testing.T) {
	for _, capture := range []bool{false, true} {
		rec := testRec(t)
		srv := start(t, Config{
			Mode:                 ModeTCP,
			Upstream:             pongServer(t),
			SelfSigned:           true,
			CaptureStreamPayload: capture,
			ProtocolName:         "smtp",
		}, rec)

		c, err := tls.Dial("tcp", srv.Addr().String(), &tls.Config{InsecureSkipVerify: true, ServerName: "mail.example"}) //nolint:gosec // test
		require.NoError(t, err)
		_, err = c.Write([]byte("hello"))
		require.NoError(t, err)
		got, err := io.ReadAll(c)
		require.NoError(t, err)
		c.Close()
		assert.Equal(t, "PONG:hello", string(got))

		ex := waitTrace(t, rec)
		s := ex.GetSummary()
		assert.Equal(t, "smtp", s.GetProtocol())
		assert.Equal(t, "STREAM", s.GetMethod())
		assert.EqualValues(t, 5, s.GetRequestBytes())
		assert.EqualValues(t, 10, s.GetResponseBytes())
		assert.Equal(t, "mail.example", s.GetLabels()["tls.sni"])
		if capture {
			assert.Equal(t, "hello", string(ex.GetRequestBody()))
			assert.Equal(t, "PONG:hello", string(ex.GetResponseBody()))
		} else {
			assert.Empty(t, ex.GetRequestBody(), "payload must not be stored by default")
		}
	}
}

func TestConfigValidation(t *testing.T) {
	_, err := New(Config{Upstream: "http://x"}, nil, zerolog.Nop())
	assert.ErrorContains(t, err, "plain_text", "TLS or explicit plain-text is required")
	_, err = New(Config{Upstream: "ftp://x", SelfSigned: true}, nil, zerolog.Nop())
	assert.ErrorContains(t, err, "invalid upstream")
	_, err = New(Config{Mode: ModeTCP, SelfSigned: true}, nil, zerolog.Nop())
	assert.ErrorContains(t, err, "upstream")
}
