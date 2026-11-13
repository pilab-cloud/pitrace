package trace

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

func TestKeyMasker(t *testing.T) {
	m := newKeyMasker(DefaultRedactKeys)

	assert.Equal(t, "a=1&access_token=%5Bredacted%5D&b=%20x",
		m.maskQuery("a=1&access_token=abc123&b=%20x"))
	assert.Equal(t, "user=bob", m.maskQuery("user=bob"))

	got := string(m.maskJSON([]byte(`{"user":"bob","Password":"p\"w","n":{"api_key": "k"},"ok":1}`)))
	assert.Equal(t, `{"user":"bob","Password":"[redacted]","n":{"api_key": "[redacted]"},"ok":1}`, got)

	// A secret cut off by truncation must not leak its prefix.
	got = string(m.maskJSON([]byte(`{"user":"bob","token":"abcdef`)))
	assert.Equal(t, `{"user":"bob","token":"[redacted]"`, got)

	assert.Equal(t, "password=%5Bredacted%5D&u=1",
		string(m.maskBody("application/x-www-form-urlencoded", []byte("password=1&u=1"))))
	assert.Equal(t, "binary", string(m.maskBody("application/zip", []byte("binary"))))
}

func TestMiddlewareMasksSecrets(t *testing.T) {
	rec := testRecorder(t, nil)
	e := echo.New()
	e.Use(rec.Middleware())
	e.POST("/login", func(c echo.Context) error {
		_, _ = io.Copy(io.Discard, c.Request().Body)
		c.Response().Header().Set("Content-Type", "application/json")
		return c.String(http.StatusOK, `{"access_token":"tok-secret","user":"bob"}`)
	})

	req := httptest.NewRequest(http.MethodPost, "/login?api_key=qs-secret&x=1",
		strings.NewReader(`{"user":"bob","password":"hunter2"}`))
	req.Header.Set("Content-Type", "application/json")
	e.ServeHTTP(httptest.NewRecorder(), req)

	var id string
	require.Eventually(t, func() bool {
		list, err := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		if err != nil || len(list.GetTraces()) != 1 {
			return false
		}
		id = list.GetTraces()[0].GetTraceId()
		return true
	}, 2*time.Second, 10*time.Millisecond)

	ex, err := rec.Get(context.Background(), id, true)
	require.NoError(t, err)
	assert.NotContains(t, ex.GetQuery(), "qs-secret")
	assert.Contains(t, ex.GetQuery(), "x=1")
	assert.NotContains(t, string(ex.GetRequestBody()), "hunter2")
	assert.Contains(t, string(ex.GetRequestBody()), `"user":"bob"`)
	assert.NotContains(t, string(ex.GetResponseBody()), "tok-secret")
}

func TestShouldRedactFlagsGateBuiltinTypes(t *testing.T) {
	var r Recorder
	off := Config{}
	assert.False(t, r.shouldRedact("image/png", 1, off, false), "download flag off must not redact")
	assert.False(t, r.shouldRedact("application/zip", 1, off, true), "upload flag off must not redact")

	on := Config{RedactDownloads: true, RedactUploads: true}
	assert.True(t, r.shouldRedact("image/png", 1, on, false))
	assert.True(t, r.shouldRedact("multipart/form-data; boundary=x", 1, on, true))
	assert.False(t, r.shouldRedact("application/json", 1, on, false))

	explicit := Config{RedactContentTypes: []string{"application/x-secret"}}
	assert.True(t, r.shouldRedact("application/x-secret; v=1", 1, explicit, false))
}

func TestTailBacklogOrderAfterRingWrap(t *testing.T) {
	rec := testRecorder(t, nil)
	total := defaultRingSize + 10
	for i := 0; i < total; i++ {
		rec.publish(&tracev1.TraceSummary{TraceId: string(rune('a' + i%26)), SessionSeq: uint32(i)})
	}
	ch, cancel := rec.Tail(context.Background(), "", "", 5)
	defer cancel()
	for want := total - 5; want < total; want++ {
		select {
		case s := <-ch:
			assert.Equal(t, uint32(want), s.GetSessionSeq())
		case <-time.After(time.Second):
			t.Fatal("backlog entry missing")
		}
	}
}

func TestTailCancelStopsWatcher(t *testing.T) {
	rec := testRecorder(t, nil)
	before := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		_, cancel := rec.Tail(context.Background(), "", "", 0)
		cancel()
	}
	require.Eventually(t, func() bool {
		return runtime.NumGoroutine() <= before+2
	}, 2*time.Second, 10*time.Millisecond)
}
