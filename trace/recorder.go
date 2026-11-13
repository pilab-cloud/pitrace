package trace

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// Recorder writes bounded, compressed request/response records and serves the
// read side used by TraceService.
//
// Writes are asynchronous: a full queue drops records and increments Dropped
// rather than slowing a mail request down.
type Recorder struct {
	cfg Config
	log zerolog.Logger

	ch     chan *tracev1.HTTPExchange
	wg     sync.WaitGroup
	sendMu sync.RWMutex
	closed atomic.Bool

	dropped     atomic.Uint64
	writeErrors atomic.Uint64

	// Sequence numbers per session, owned by the middleware side.
	seqMu sync.Mutex
	seq   map[string]uint32

	// Live tail state, written by the writer goroutine.
	ringMu    sync.RWMutex
	ring      [defaultRingSize]*tracev1.TraceSummary
	ringNext  int // slot the next summary is written to
	ringCount int

	subsMu sync.RWMutex
	subs   map[*subscriber]struct{}

	// Dedup state.
	mu   sync.Mutex
	last map[string]lastHashes

	mask *keyMasker

	storage Storage
	now     func() time.Time
}

type lastHashes struct {
	traceID string
	request string
	respond string
}

type subscriber struct {
	ch       chan *tracev1.TraceSummary
	protocol string
	account  string
}

func (s *subscriber) matches(summary *tracev1.TraceSummary) bool {
	if s.protocol != "" && !strings.EqualFold(s.protocol, summary.GetProtocol()) {
		return false
	}
	if s.account != "" && !strings.EqualFold(s.account, summary.GetAccountId()) {
		return false
	}
	return true
}

// NewRecorder creates the trace directory/backend and starts the writer. A
// disabled config returns a nil recorder: Middleware and Record are nil-safe.
func NewRecorder(cfg Config, logger zerolog.Logger) (*Recorder, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg = cfg.withDefaults()
	var st Storage
	var err error
	switch cfg.StorageType {
	case "s3":
		st, err = NewS3Storage(cfg, logger, cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3Bucket, cfg.S3Prefix, cfg.S3UseSSL)
	case "remote":
		st, err = NewRemoteStorage(cfg, logger)
	default:
		st, err = NewFileStorage(cfg, logger)
	}
	if err != nil {
		return nil, err
	}
	return NewRecorderWithStorage(cfg, logger, st)
}

// NewRecorderWithStorage builds a recorder on a caller-supplied backend.
func NewRecorderWithStorage(cfg Config, logger zerolog.Logger, st Storage) (*Recorder, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	cfg = cfg.withDefaults()
	r := &Recorder{
		cfg:     cfg,
		log:     logger,
		ch:      make(chan *tracev1.HTTPExchange, cfg.QueueSize),
		seq:     make(map[string]uint32),
		subs:    make(map[*subscriber]struct{}),
		last:    make(map[string]lastHashes),
		storage: st,
		now:     time.Now,
	}
	if !cfg.DisableKeyRedaction {
		r.mask = newKeyMasker(cfg.RedactKeys)
	}
	r.wg.Add(1)
	go r.writeLoop()

	// Startup prune is intentionally left to the caller/explicit schedule to
	// avoid racing tests and unbounded goroutines.
	return r, nil
}

// Config returns the effective configuration.
func (r *Recorder) Config() Config {
	if r == nil {
		return Config{}
	}
	return r.cfg
}

// Enabled reports whether recording is active.
func (r *Recorder) Enabled() bool { return r != nil && r.cfg.Enabled && !r.closed.Load() }

// Dropped is the number of records discarded because the queue was full.
func (r *Recorder) Dropped() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// WriteErrors is the number of records that could not be persisted.
func (r *Recorder) WriteErrors() uint64 {
	if r == nil {
		return 0
	}
	return r.writeErrors.Load()
}

// Record enqueues one exchange. It never blocks: dropping a trace is always
// preferable to stalling a mail request.
func (r *Recorder) Record(ex *tracev1.HTTPExchange) {
	if r == nil || ex == nil || ex.Summary == nil {
		return
	}
	r.sendMu.RLock()
	defer r.sendMu.RUnlock()
	if r.closed.Load() {
		return
	}
	select {
	case r.ch <- ex:
	default:
		r.dropped.Add(1)
	}
}

// Close flushes queued records and closes the active segment.
func (r *Recorder) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.sendMu.Lock()
	if r.closed.Load() {
		r.sendMu.Unlock()
		return nil
	}
	r.closed.Store(true)
	close(r.ch)
	r.sendMu.Unlock()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	if r.storage != nil {
		_ = r.storage.Close(ctx)
	}

	r.subsMu.Lock()
	for sub := range r.subs {
		close(sub.ch)
		delete(r.subs, sub)
	}
	r.subsMu.Unlock()
	return nil
}

func (r *Recorder) writeLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case ex, ok := <-r.ch:
			if !ok {
				return
			}
			r.writeRecord(ex)
		case now := <-ticker.C:
			if _, _, err := r.storage.Prune(context.Background(), now); err != nil {
				r.log.Warn().Err(err).Msg("trace prune failed")
			}
		}
	}
}

func (r *Recorder) writeRecord(ex *tracev1.HTTPExchange) {
	// Deduplicate polling: when a session repeats the same request and the
	// same response, keep only the reference and drop the payloads.
	r.mu.Lock()
	if sid := ex.Summary.GetSessionId(); sid != "" {
		prev := r.last[sid]
		if prev.request != "" && prev.request == ex.GetRequestSha256() && prev.respond == ex.GetResponseSha256() {
			ex.DedupOf = prev.traceID
			ex.RequestBody = nil
			ex.ResponseBody = nil
		}
		if len(r.last) > 10000 {
			r.last = make(map[string]lastHashes)
		}
		r.last[sid] = lastHashes{
			traceID: ex.Summary.GetTraceId(),
			request: ex.GetRequestSha256(),
			respond: ex.GetResponseSha256(),
		}
	}
	r.mu.Unlock()

	if err := r.storage.Write(context.Background(), ex); err != nil {
		r.writeErrors.Add(1)
		r.log.Error().Err(err).Msg("trace: persist record failed")
		return
	}

	r.publish(ex.Summary)
}

func (r *Recorder) publish(summary *tracev1.TraceSummary) {
	if summary == nil {
		return
	}
	r.ringMu.Lock()
	r.ring[r.ringNext] = summary
	r.ringNext = (r.ringNext + 1) % defaultRingSize
	if r.ringCount < defaultRingSize {
		r.ringCount++
	}
	r.ringMu.Unlock()

	r.subsMu.RLock()
	for sub := range r.subs {
		if !sub.matches(summary) {
			continue
		}
		select {
		case sub.ch <- summary:
		default:
		}
	}
	r.subsMu.RUnlock()
}

// List returns index summaries, newest first.
func (r *Recorder) List(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	if r == nil || r.storage == nil {
		return &tracev1.ListTracesResponse{}, nil
	}
	return r.storage.List(ctx, req)
}

// Get loads one exchange from its segment.
func (r *Recorder) Get(ctx context.Context, traceID string, includeBodies bool) (*tracev1.HTTPExchange, error) {
	if r == nil || r.storage == nil {
		return nil, ErrTraceNotFound
	}
	return r.storage.Read(ctx, traceID, includeBodies)
}

// Tail replays up to backlog recent summaries and then follows live records.
func (r *Recorder) Tail(ctx context.Context, protocol, account string, backlog int) (<-chan *tracev1.TraceSummary, func()) {
	sub := &subscriber{
		ch:       make(chan *tracev1.TraceSummary, 128),
		protocol: protocol,
		account:  account,
	}
	if r == nil {
		close(sub.ch)
		return sub.ch, func() {}
	}
	if backlog < 0 {
		backlog = 0
	}
	if backlog > cap(sub.ch) {
		backlog = cap(sub.ch)
	}

	r.subsMu.Lock()
	r.subs[sub] = struct{}{}
	r.subsMu.Unlock()

	if backlog > 0 {
		r.ringMu.RLock()
		recent := make([]*tracev1.TraceSummary, 0, r.ringCount)
		for i := r.ringCount; i > 0; i-- {
			recent = append(recent, r.ring[(r.ringNext-i+defaultRingSize)%defaultRingSize])
		}
		r.ringMu.RUnlock()
		start := len(recent) - backlog
		if start < 0 {
			start = 0
		}
		for _, s := range recent[start:] {
			if sub.matches(s) {
				select {
				case sub.ch <- s:
				default:
				}
			}
		}
	}

	var once sync.Once
	done := make(chan struct{})
	cancel := func() {
		once.Do(func() {
			close(done)
			r.subsMu.Lock()
			if _, ok := r.subs[sub]; ok {
				delete(r.subs, sub)
				close(sub.ch)
			}
			r.subsMu.Unlock()
		})
	}
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-done:
		}
	}()
	return sub.ch, cancel
}

// Prune delegates to storage.
func (r *Recorder) Prune(ctx context.Context, now time.Time) (int, int64, error) {
	if r == nil || r.storage == nil {
		return 0, 0, nil
	}
	return r.storage.Prune(ctx, now)
}
