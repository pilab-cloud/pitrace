package trace

import (
	"context"
	"strings"
	"sync"
	"time"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

const (
	uploadQueueDepth  = 32 // chunks of up to remoteChunkSize in flight per body
	uploadWriteWait   = 250 * time.Millisecond
	uploadKeepResult  = 10 * time.Minute
	defaultStreamFrom = 256 << 10
)

// BodyStreamer is implemented by storage backends that accept a body while it
// is still being produced, so large payloads are never held in memory.
type BodyStreamer interface {
	// OpenBody starts a streamed upload of one body of the given exchange.
	OpenBody(traceID string, started time.Time, kind tracev1.BodyKind) *BodyUpload
}

// BodyUpload is the producer side of one streamed body. Write never blocks the
// traffic being recorded for long: when the collector cannot keep up the
// upload is abandoned (and the exchange is recorded as truncated) instead.
type BodyUpload struct {
	q      chan []byte
	dead   chan struct{} // closed when the upload failed or was aborted
	fin    chan struct{} // closed by Close: no more data will be written
	done   chan struct{} // closed when the sender goroutine finished
	cancel context.CancelFunc
	wait   time.Duration

	mu     sync.Mutex
	closed bool
	isDead bool

	// set by the sender before done is closed
	size int64
	err  error
}

func newBodyUpload(cancel context.CancelFunc, depth int, wait time.Duration) *BodyUpload {
	return &BodyUpload{
		q:      make(chan []byte, depth),
		dead:   make(chan struct{}),
		fin:    make(chan struct{}),
		done:   make(chan struct{}),
		cancel: cancel,
		wait:   wait,
	}
}

// Write queues a copy of p. It returns false once the upload is no longer
// usable; the caller then stops streaming.
func (u *BodyUpload) Write(p []byte) bool {
	for len(p) > 0 {
		n := min(len(p), remoteChunkSize)
		chunk := append([]byte(nil), p[:n]...)
		p = p[n:]
		if !u.send(chunk) {
			return false
		}
	}
	return true
}

func (u *BodyUpload) send(chunk []byte) bool {
	u.mu.Lock()
	if u.closed || u.isDead {
		u.mu.Unlock()
		return false
	}
	u.mu.Unlock()
	select {
	case u.q <- chunk:
		return true
	case <-u.dead:
		return false
	default:
	}
	t := time.NewTimer(u.wait)
	defer t.Stop()
	select {
	case u.q <- chunk:
		return true
	case <-u.dead:
		return false
	case <-t.C:
		u.Abort()
		return false
	}
}

// Close marks the end of the body; the upload finishes in the background.
func (u *BodyUpload) Close() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.closed || u.isDead {
		return
	}
	u.closed = true
	close(u.fin)
}

// Abort abandons the upload; the collector discards the partial object.
func (u *BodyUpload) Abort() {
	u.mu.Lock()
	if !u.isDead {
		u.isDead = true
		close(u.dead)
	}
	u.mu.Unlock()
	u.cancel()
}

// Wait blocks until the collector confirmed the body and returns its size.
func (u *BodyUpload) Wait(ctx context.Context) (int64, error) {
	select {
	case <-u.done:
		return u.size, u.err
	case <-ctx.Done():
		u.Abort()
		return 0, ctx.Err()
	}
}

// bodySpill is the part of a capture that streams oversized bodies out.
type bodySpill struct {
	open      func() *BodyUpload // nil: this body is not eligible
	threshold int64
	up        *BodyUpload
	failed    bool
}

// streamable reports whether a body may bypass the recorder's post-processing
// (key masking, decompression, redaction, gzip). Only opaque payloads
// qualify, because a live tee would otherwise store secrets unmasked.
func (r *Recorder) streamable(contentType, encoding string, upload bool) bool {
	if r.streamer() == nil || encoding != "" || contentType == "" || r.cfg.RedactThresholdBytes > 0 {
		return false
	}
	ct := strings.ToLower(contentType)
	for _, masked := range []string{"json", "x-www-form-urlencoded", "xml"} {
		if strings.Contains(ct, masked) {
			return false
		}
	}
	if strings.HasPrefix(ct, "text/") {
		return false
	}
	return !r.shouldRedact(contentType, 0, r.cfg, upload)
}

func (r *Recorder) streamer() BodyStreamer {
	if r == nil {
		return nil
	}
	bs, _ := r.storage.(BodyStreamer)
	return bs
}

func (r *Recorder) streamThreshold() int64 {
	if r.cfg.StreamThresholdBytes > 0 {
		return r.cfg.StreamThresholdBytes
	}
	return defaultStreamFrom
}

// spillFor returns a spill for one body, or nil-open (inactive) when the body
// is not eligible.
func (r *Recorder) spillFor(eligible bool, traceID string, started time.Time, kind tracev1.BodyKind) bodySpill {
	sp := bodySpill{threshold: r.streamThreshold()}
	if bs := r.streamer(); eligible && bs != nil {
		sp.open = func() *BodyUpload { return bs.OpenBody(traceID, started, kind) }
	}
	return sp
}
