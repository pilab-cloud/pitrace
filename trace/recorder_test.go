package trace

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

func testRecorder(t *testing.T, mutate func(*Config)) *Recorder {
	t.Helper()
	cfg := Config{
		Enabled:          true,
		Dir:              t.TempDir(),
		MaxRequestBytes:  4096,
		MaxResponseBytes: 4096,
		QueueSize:        16,
		SampleRatio:      1,
		RotateInterval:   time.Hour,
		MaxAge:           time.Hour,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	rec, err := NewRecorder(cfg, zerolog.Nop())
	require.NoError(t, err)
	require.NotNil(t, rec)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = rec.Close(ctx)
	})
	return rec
}

func exchange(id, operation, body, response string, started time.Time) *tracev1.HTTPExchange {
	return &tracev1.HTTPExchange{
		SchemaVersion: 1,
		Summary: &tracev1.TraceSummary{
			TraceId:       id,
			StartedAt:     timestamppb.New(started),
			Protocol:      "ews",
			Operation:     operation,
			AccountId:     "acc-1",
			Principal:     "gyula@pilab.hu",
			Method:        "POST",
			Path:          "/EWS/Exchange.asmx",
			Status:        200,
			SessionId:     "sess-1",
			UserAgent:     "MacOutlook/16.107",
			ResponseCodes: []string{"NoError"},
		},
		RequestHeaders: []*tracev1.Header{{Name: userAgentHeader, Value: "MacOutlook/16.107"}},
		RequestBody:    []byte(body),
		ResponseBody:   []byte(response),
		RequestSha256:  hashHex([]byte(body)),
		ResponseSha256: hashHex([]byte(response)),
	}
}

func TestRecorderRoundTrip(t *testing.T) {
	rec := testRecorder(t, nil)
	now := time.Now().UTC()

	rec.Record(exchange("trace-1", "CreateItem", "<soap/>", "<ok/>", now.Add(-time.Second)))
	rec.Record(exchange("trace-2", "FindItem", "<find/>", "<items/>", now))
	require.NoError(t, rec.Close(context.Background()))

	ctx := context.Background()
	list, err := rec.List(ctx, &tracev1.ListTracesRequest{})
	require.NoError(t, err)
	require.Len(t, list.GetTraces(), 2)
	assert.Equal(t, "trace-2", list.GetTraces()[0].GetTraceId(), "newest first")
	assert.Equal(t, uint32(2), list.GetTotal())

	ex, err := rec.Get(ctx, "trace-1", true)
	require.NoError(t, err)
	assert.Equal(t, "CreateItem", ex.GetSummary().GetOperation())
	assert.Equal(t, "<soap/>", string(ex.GetRequestBody()))
	assert.Equal(t, "<ok/>", string(ex.GetResponseBody()))

	metaOnly, err := rec.Get(ctx, "trace-1", false)
	require.NoError(t, err)
	assert.Empty(t, metaOnly.GetRequestBody())
	assert.Empty(t, metaOnly.GetResponseBody())
}

func TestRecorderListFilters(t *testing.T) {
	rec := testRecorder(t, nil)
	now := time.Now().UTC()
	rec.Record(exchange("trace-1", "CreateItem", "a", "b", now.Add(-2*time.Hour)))
	rec.Record(exchange("trace-2", "CreateItem", "c", "d", now.Add(-time.Hour)))
	rec.Record(exchange("trace-3", "FindItem", "e", "f", now))
	require.NoError(t, rec.Close(context.Background()))

	ctx := context.Background()
	byOp, err := rec.List(ctx, &tracev1.ListTracesRequest{Operation: "CreateItem"})
	require.NoError(t, err)
	assert.Len(t, byOp.GetTraces(), 2)

	bySince, err := rec.List(ctx, &tracev1.ListTracesRequest{
		Since: timestamppb.New(now.Add(-90 * time.Minute)),
	})
	require.NoError(t, err)
	require.Len(t, bySince.GetTraces(), 2)
	assert.Equal(t, "trace-3", bySince.GetTraces()[0].GetTraceId())

	byText, err := rec.List(ctx, &tracev1.ListTracesRequest{Text: "MacOutlook"})
	require.NoError(t, err)
	assert.Len(t, byText.GetTraces(), 3)

	bySession, err := rec.List(ctx, &tracev1.ListTracesRequest{SessionId: "other"})
	require.NoError(t, err)
	assert.Empty(t, bySession.GetTraces())
}

func TestRecorderDedupesRepeatedPolling(t *testing.T) {
	rec := testRecorder(t, nil)
	now := time.Now().UTC()
	first := exchange("trace-1", "SyncFolderItems", "same", "same", now.Add(-time.Second))
	second := exchange("trace-2", "SyncFolderItems", "same", "same", now)
	rec.Record(first)
	rec.Record(second)
	require.NoError(t, rec.Close(context.Background()))

	stored, err := rec.Get(context.Background(), "trace-2", true)
	require.NoError(t, err)
	assert.Equal(t, "trace-1", stored.GetDedupOf())
	assert.Empty(t, stored.GetRequestBody(), "repeated payload is stored by reference")
	assert.Empty(t, stored.GetResponseBody())
}

func TestRecorderPrunesByAgeAndSize(t *testing.T) {
	dir := t.TempDir()
	oldFile := filepath.Join(dir, "2020-01-01")
	require.NoError(t, os.MkdirAll(oldFile, 0o700))
	oldPath := filepath.Join(oldFile, "00-00-00.pb.zst")
	require.NoError(t, os.WriteFile(oldPath, []byte("old"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(oldFile, "00-00-00.index.jsonl"), []byte("{}\n"), 0o600))
	old := time.Now().Add(-48 * time.Hour)
	require.NoError(t, os.Chtimes(oldPath, old, old))

	rec := testRecorder(t, func(c *Config) { c.Dir = dir; c.MaxAge = 24 * time.Hour })
	removed, _, err := rec.Prune(context.Background(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 1, removed)
	_, err = os.Stat(oldPath)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(oldFile, "00-00-00.index.jsonl"))
	assert.True(t, os.IsNotExist(err))
}

func TestRecorderDisabled(t *testing.T) {
	rec, err := NewRecorder(Config{Enabled: false}, zerolog.Nop())
	require.NoError(t, err)
	assert.Nil(t, rec)
	assert.False(t, rec.Enabled())
}

func TestRecorderDropsInsteadOfBlocking(t *testing.T) {
	rec := testRecorder(t, func(c *Config) { c.QueueSize = 1 })
	// The writer is running, so filling the queue deterministically is racy;
	// assert the invariant that matters: Record always returns promptly.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			rec.Record(exchange("trace", "FindItem", "x", "y", time.Now()))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Record blocked on a full queue")
	}
}

func TestTailDeliversBacklogAndLive(t *testing.T) {
	rec := testRecorder(t, nil)
	now := time.Now().UTC()
	rec.Record(exchange("trace-1", "FindItem", "a", "b", now.Add(-time.Second)))
	require.Eventually(t, func() bool {
		list, _ := rec.List(context.Background(), &tracev1.ListTracesRequest{})
		return len(list.GetTraces()) == 1
	}, 2*time.Second, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, stop := rec.Tail(ctx, "ews", "", 5)
	defer stop()

	select {
	case s := <-ch:
		assert.Equal(t, "trace-1", s.GetTraceId())
	case <-time.After(2 * time.Second):
		t.Fatal("backlog was not delivered")
	}

	rec.Record(exchange("trace-2", "CreateItem", "c", "d", now))
	select {
	case s := <-ch:
		assert.Equal(t, "trace-2", s.GetTraceId())
	case <-time.After(2 * time.Second):
		t.Fatal("live record was not delivered")
	}
}
