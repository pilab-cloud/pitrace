package trace

import (
	"context"
	"testing"
	"time"
)

func TestBodyUploadAbandonsWhenConsumerStalls(t *testing.T) {
	cancelled := false
	u := newBodyUpload(func() { cancelled = true }, 2, 20*time.Millisecond)

	start := time.Now()
	ok := true
	for i := 0; i < 10 && ok; i++ {
		ok = u.Write(make([]byte, 100))
	}
	if ok {
		t.Fatal("writer was never told to stop although nothing consumes the queue")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("stalled consumer held the writer for %s", time.Since(start))
	}
	if !cancelled {
		t.Fatal("abort must cancel the upload stream")
	}
	if u.Write([]byte("x")) {
		t.Fatal("writes after abort must fail")
	}
}

func TestBodyUploadWaitHonoursContext(t *testing.T) {
	u := newBodyUpload(func() {}, 1, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := u.Wait(ctx); err == nil {
		t.Fatal("Wait must fail when the sender never finishes")
	}
}
