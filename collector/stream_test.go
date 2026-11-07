package collector_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

// proxyThrough serves handler behind the recorder on top of the harness
// collector and returns a client for it.
func proxyThrough(t *testing.T, h *harness, handler http.Handler) *httptest.Server {
	t.Helper()
	rec, err := trace.NewRecorderWithStorage(trace.Config{
		Enabled:              true,
		MaxRequestBodyBytes:  -1,
		MaxRequestBytes:      64 << 10,
		MaxResponseBytes:     64 << 10,
		MaxResponseBodyBytes: 64 << 10,
		StreamThresholdBytes: 64 << 10,
	}, zerolog.Nop(), h.remote)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(rec.HTTPMiddleware()(handler))
	t.Cleanup(srv.Close)
	return srv
}

func echoSize(w http.ResponseWriter, r *http.Request) {
	n, _ := io.Copy(io.Discard, r.Body)
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(bytes.Repeat([]byte{0xAB}, int(n)))
}

func TestOpaqueBodiesStreamPastBufferLimits(t *testing.T) {
	h := newHarness(t)
	srv := proxyThrough(t, h, http.HandlerFunc(echoSize))

	payload := make([]byte, 12<<20)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	resp, err := http.Post(srv.URL+"/upload", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(got) != len(payload) {
		t.Fatalf("client received %d bytes, want %d", len(got), len(payload))
	}

	ctx := context.Background()
	var id string
	waitFor(t, "exchange stored", func() bool {
		l, err := h.remote.List(ctx, &tracev1.ListTracesRequest{})
		if err != nil || len(l.GetTraces()) != 1 {
			return false
		}
		id = l.GetTraces()[0].GetTraceId()
		return true
	})
	ex, err := h.remote.Read(ctx, id, true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(ex.GetRequestBody(), payload) {
		t.Fatalf("request body: got %d bytes, want the full %d", len(ex.GetRequestBody()), len(payload))
	}
	if len(ex.GetResponseBody()) != len(payload) {
		t.Fatalf("response body: got %d bytes, want %d", len(ex.GetResponseBody()), len(payload))
	}
	if s := ex.GetSummary(); s.GetRequestTruncated() || s.GetResponseTruncated() {
		t.Fatal("streamed bodies must not be marked truncated")
	}
}

func TestSecretBearingBodiesAreNotStreamed(t *testing.T) {
	h := newHarness(t)
	srv := proxyThrough(t, h, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
	}))

	body := `{"password":"hunter2","pad":"` + strings.Repeat("x", 200<<10) + `"}`
	resp, err := http.Post(srv.URL+"/login", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	ctx := context.Background()
	var id string
	waitFor(t, "exchange stored", func() bool {
		l, err := h.remote.List(ctx, &tracev1.ListTracesRequest{})
		if err != nil || len(l.GetTraces()) != 1 {
			return false
		}
		id = l.GetTraces()[0].GetTraceId()
		return true
	})
	ex, err := h.remote.Read(ctx, id, true)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ex.GetRequestBody(), []byte("hunter2")) {
		t.Fatal("secret reached the store")
	}
	if !ex.GetSummary().GetRequestTruncated() {
		t.Fatal("JSON body over the buffer cap should be recorded truncated")
	}
}

func TestSlowCollectorNeverStallsTraffic(t *testing.T) {
	h := newHarness(t)
	h.srv.Stop() // collector gone: uploads cannot complete
	srv := proxyThrough(t, h, http.HandlerFunc(echoSize))

	payload := bytes.Repeat([]byte{1}, 8<<20)
	start := time.Now()
	resp, err := http.Post(srv.URL+"/upload", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if int(n) != len(payload) {
		t.Fatalf("client received %d bytes, want %d", n, len(payload))
	}
	if d := time.Since(start); d > 10*time.Second {
		t.Fatalf("proxying took %s with the collector down", d)
	}
}
