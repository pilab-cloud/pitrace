package trace

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

const (
	remoteWindow    = 256 // unacknowledged exchanges kept for resend
	remoteChunkSize = 64 << 10
	remoteMinWait   = 500 * time.Millisecond
	remoteMaxWait   = 15 * time.Second
	remoteSlotWait  = time.Second

	// uploadResolveWait bounds how long Write waits for the collector to
	// confirm a streamed body once the exchange is over.
	uploadResolveWait = 10 * time.Second

	fieldRequestBody  = 8
	fieldResponseBody = 9
)

// ErrCollectorBusy is returned by RemoteStorage.Write when the collector is
// unreachable and the resend window is full.
var ErrCollectorBusy = errors.New("trace: collector unavailable and send window full")

type pendingExchange struct {
	begin    *tracev1.ExchangeBegin
	reqBody  []byte
	respBody []byte
}

// RemoteStorage ships exchanges to a hackergate collector over one long-lived
// IngestService stream. Delivery is at-least-once: exchanges stay in memory
// until acknowledged and are re-sent after a reconnect (the collector
// deduplicates by trace id). Reads are served by the collector.
//
// The window is small on purpose: the pending acknowledgements of a full
// window fit in the default HTTP/2 flow-control window, so the collector never
// blocks while answering.
type RemoteStorage struct {
	log    zerolog.Logger
	src    *tracev1.SourceInfo
	conn   *grpc.ClientConn
	ingest tracev1.IngestServiceClient
	reader tracev1.TraceServiceClient
	bodies tracev1.BodyServiceClient

	slots chan struct{}

	upMu    sync.Mutex
	uploads map[uploadKey]*BodyUpload

	mu       sync.Mutex // guards everything below and serialises stream sends
	stream   tracev1.IngestService_PushClient
	cancelFn context.CancelFunc
	inflight []*pendingExchange

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRemoteStorage dials the collector lazily; it never blocks on the network.
// extra dial options are appended (tests use them to plug in a dialer).
func NewRemoteStorage(cfg Config, logger zerolog.Logger, extra ...grpc.DialOption) (*RemoteStorage, error) {
	if cfg.CollectorEndpoint == "" {
		return nil, errors.New("trace: collector_endpoint required for remote storage")
	}
	creds, err := collectorCreds(cfg)
	if err != nil {
		return nil, err
	}
	opts := append([]grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second, PermitWithoutStream: true}),
	}, extra...)
	conn, err := grpc.NewClient(cfg.CollectorEndpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("trace: collector dial: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &RemoteStorage{
		log:     logger,
		conn:    conn,
		ingest:  tracev1.NewIngestServiceClient(conn),
		reader:  tracev1.NewTraceServiceClient(conn),
		bodies:  tracev1.NewBodyServiceClient(conn),
		slots:   make(chan struct{}, remoteWindow),
		uploads: make(map[uploadKey]*BodyUpload),
		ctx:     ctx,
		cancel:  cancel,
		src: &tracev1.SourceInfo{
			SourceId:  cfg.Source.ID,
			Namespace: cfg.Source.Namespace,
			Pod:       cfg.Source.Pod,
			Workload:  cfg.Source.Workload,
			Node:      cfg.Source.Node,
		},
	}
	r.wg.Add(1)
	go r.run()
	return r, nil
}

func collectorCreds(cfg Config) (credentials.TransportCredentials, error) {
	if cfg.CollectorInsecure {
		return insecure.NewCredentials(), nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CollectorCAFile != "" {
		pem, err := os.ReadFile(cfg.CollectorCAFile)
		if err != nil {
			return nil, fmt.Errorf("trace: collector ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("trace: collector ca: no certificates found")
		}
		tc.RootCAs = pool
	}
	return credentials.NewTLS(tc), nil
}

// stripBodies returns a shallow copy of ex without the two body fields.
func stripBodies(ex *tracev1.HTTPExchange) *tracev1.HTTPExchange {
	out := &tracev1.HTTPExchange{}
	om := out.ProtoReflect()
	ex.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if n := fd.Number(); n != fieldRequestBody && n != fieldResponseBody {
			om.Set(fd, v)
		}
		return true
	})
	return out
}

type uploadKey struct {
	traceID string
	kind    tracev1.BodyKind
}

// OpenBody implements BodyStreamer: the body is sent on its own PutBody stream
// as the caller writes it. The exchange that references it is later passed to
// Write, which waits for the collector's confirmation.
func (r *RemoteStorage) OpenBody(traceID string, started time.Time, kind tracev1.BodyKind) *BodyUpload {
	ctx, cancel := context.WithCancel(r.ctx)
	u := newBodyUpload(cancel, uploadQueueDepth, uploadWriteWait)
	key := uploadKey{traceID, kind}
	r.upMu.Lock()
	r.uploads[key] = u
	r.upMu.Unlock()

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		u.size, u.err = r.sendBody(ctx, u, traceID, started, kind)
		if u.err != nil {
			u.Abort() // unblock any writer still queueing
		}
		close(u.done)
		// Write normally consumes the entry; this only frees abandoned ones.
		time.AfterFunc(uploadKeepResult, func() { r.dropUpload(key, u) })
	}()
	return u
}

func (r *RemoteStorage) dropUpload(key uploadKey, u *BodyUpload) {
	r.upMu.Lock()
	if r.uploads[key] == u {
		delete(r.uploads, key)
	}
	r.upMu.Unlock()
}

func (r *RemoteStorage) sendBody(ctx context.Context, u *BodyUpload, traceID string, started time.Time, kind tracev1.BodyKind) (int64, error) {
	st, err := r.ingest.PutBody(ctx)
	if err != nil {
		return 0, err
	}
	hdr := &tracev1.PutBodyHeader{TraceId: traceID, Kind: kind, StartedAt: timestamppb.New(started)}
	if err := st.Send(&tracev1.PutBodyRequest{Msg: &tracev1.PutBodyRequest_Header{Header: hdr}}); err != nil {
		return 0, err
	}
	var sent int64
	for {
		select {
		case chunk := <-u.q:
			if err := st.Send(&tracev1.PutBodyRequest{Msg: &tracev1.PutBodyRequest_Data{Data: chunk}}); err != nil {
				return 0, err
			}
			sent += int64(len(chunk))
		case <-u.fin:
			// Close was called: flush what is still queued, then finish.
			for {
				select {
				case chunk := <-u.q:
					if err := st.Send(&tracev1.PutBodyRequest{Msg: &tracev1.PutBodyRequest_Data{Data: chunk}}); err != nil {
						return 0, err
					}
					sent += int64(len(chunk))
					continue
				default:
				}
				break
			}
			resp, err := st.CloseAndRecv()
			if err != nil {
				return 0, err
			}
			if int64(resp.GetSize()) != sent {
				return 0, fmt.Errorf("trace: collector stored %d of %d bytes", resp.GetSize(), sent)
			}
			return sent, nil
		case <-u.dead:
			return 0, errors.New("trace: body upload aborted")
		}
	}
}

type uploadState int

const (
	uploadNone uploadState = iota // the body was not streamed
	uploadOK
	uploadFailed
)

// resolveUpload waits for the streamed body of an exchange, if there was one,
// and reports its stored size.
func (r *RemoteStorage) resolveUpload(ctx context.Context, traceID string, kind tracev1.BodyKind) (uint64, uploadState) {
	key := uploadKey{traceID, kind}
	r.upMu.Lock()
	u := r.uploads[key]
	delete(r.uploads, key)
	r.upMu.Unlock()
	if u == nil {
		return 0, uploadNone
	}
	wctx, cancel := context.WithTimeout(ctx, uploadResolveWait)
	defer cancel()
	size, err := u.Wait(wctx)
	if err != nil {
		r.log.Warn().Err(err).Str("trace_id", traceID).Msg("streamed body upload failed; recording the first bytes only")
		return 0, uploadFailed
	}
	return uint64(size), uploadOK
}

// Write queues one exchange. It returns ErrCollectorBusy only when the
// collector has been unreachable long enough to fill the resend window.
func (r *RemoteStorage) Write(ctx context.Context, ex *tracev1.HTTPExchange) error {
	select {
	case r.slots <- struct{}{}:
	default:
		t := time.NewTimer(remoteSlotWait)
		defer t.Stop()
		select {
		case r.slots <- struct{}{}:
		case <-t.C:
			return ErrCollectorBusy
		case <-ctx.Done():
			return ctx.Err()
		case <-r.ctx.Done():
			return errors.New("trace: remote storage closed")
		}
	}
	p := &pendingExchange{
		begin: &tracev1.ExchangeBegin{
			Exchange:         stripBodies(ex),
			RequestBodySize:  uint64(len(ex.GetRequestBody())),
			ResponseBodySize: uint64(len(ex.GetResponseBody())),
		},
		reqBody:  ex.GetRequestBody(),
		respBody: ex.GetResponseBody(),
	}
	// A body that was streamed during the exchange is referenced, not re-sent;
	// when its upload failed the first captured bytes go inline instead.
	id := ex.GetSummary().GetTraceId()
	sum := p.begin.Exchange.Summary
	switch size, st := r.resolveUpload(ctx, id, tracev1.BodyKind_BODY_KIND_REQUEST); st {
	case uploadOK:
		p.begin.RequestBodyUploaded, p.begin.RequestBodySize, p.reqBody = true, size, nil
	case uploadFailed:
		sum.RequestTruncated = true
	}
	switch size, st := r.resolveUpload(ctx, id, tracev1.BodyKind_BODY_KIND_RESPONSE); st {
	case uploadOK:
		p.begin.ResponseBodyUploaded, p.begin.ResponseBodySize, p.respBody = true, size, nil
	case uploadFailed:
		sum.ResponseTruncated = true
	}
	r.mu.Lock()
	r.inflight = append(r.inflight, p)
	if r.stream != nil {
		if err := sendPending(r.stream, p); err != nil {
			// The stream is dead; the manager reconnects and re-sends.
			r.cancelFn()
		}
	}
	r.mu.Unlock()
	return nil
}

func sendPending(st tracev1.IngestService_PushClient, p *pendingExchange) error {
	if err := st.Send(&tracev1.PushRequest{Msg: &tracev1.PushRequest_Begin{Begin: p.begin}}); err != nil {
		return err
	}
	for _, part := range []struct {
		kind tracev1.BodyKind
		data []byte
	}{
		{tracev1.BodyKind_BODY_KIND_REQUEST, p.reqBody},
		{tracev1.BodyKind_BODY_KIND_RESPONSE, p.respBody},
	} {
		for len(part.data) > 0 {
			n := min(len(part.data), remoteChunkSize)
			chunk := &tracev1.BodyChunk{Kind: part.kind, Data: part.data[:n]}
			if err := st.Send(&tracev1.PushRequest{Msg: &tracev1.PushRequest_Chunk{Chunk: chunk}}); err != nil {
				return err
			}
			part.data = part.data[n:]
		}
	}
	return nil
}

// run owns the stream lifecycle: connect, say hello, re-send everything
// unacknowledged, then read acknowledgements until the stream ends.
func (r *RemoteStorage) run() {
	defer r.wg.Done()
	wait := remoteMinWait
	for r.ctx.Err() == nil {
		sctx, cancel := context.WithCancel(r.ctx)
		started := time.Now()
		acked, err := r.session(sctx, cancel)
		cancel()
		if r.ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, io.EOF) {
			r.log.Warn().Err(err).Msg("collector stream lost")
			r.dropPoison(err)
		}
		if acked > 0 || time.Since(started) > 5*time.Second {
			wait = remoteMinWait
		}
		select {
		case <-time.After(wait):
		case <-r.ctx.Done():
			return
		}
		if acked == 0 {
			wait = min(wait*2, remoteMaxWait)
		}
	}
}

func (r *RemoteStorage) session(ctx context.Context, cancel context.CancelFunc) (acked int, err error) {
	st, err := r.ingest.Push(ctx)
	if err != nil {
		return 0, err
	}
	r.mu.Lock()
	err = st.Send(&tracev1.PushRequest{Msg: &tracev1.PushRequest_Hello{Hello: r.src}})
	for i := 0; err == nil && i < len(r.inflight); i++ {
		err = sendPending(st, r.inflight[i])
	}
	if err != nil {
		r.mu.Unlock()
		return 0, err
	}
	r.stream, r.cancelFn = st, cancel
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.stream, r.cancelFn = nil, nil
		r.mu.Unlock()
	}()
	for {
		ack, err := st.Recv()
		if err != nil {
			return acked, err
		}
		r.ack(ack.GetTraceId())
		acked++
	}
}

func (r *RemoteStorage) ack(traceID string) {
	r.mu.Lock()
	for i, p := range r.inflight {
		if p.begin.GetExchange().GetSummary().GetTraceId() == traceID {
			r.inflight = append(r.inflight[:i], r.inflight[i+1:]...)
			r.mu.Unlock()
			<-r.slots
			return
		}
	}
	r.mu.Unlock()
}

// dropPoison removes the oldest unacknowledged exchange when the collector
// rejected it permanently; re-sending it would wedge the stream forever.
func (r *RemoteStorage) dropPoison(err error) {
	switch status.Code(err) {
	case codes.InvalidArgument, codes.ResourceExhausted:
	default:
		return
	}
	r.mu.Lock()
	if len(r.inflight) == 0 {
		r.mu.Unlock()
		return
	}
	p := r.inflight[0]
	r.inflight = r.inflight[1:]
	r.mu.Unlock()
	<-r.slots
	r.log.Error().Err(err).Str("trace_id", p.begin.GetExchange().GetSummary().GetTraceId()).
		Msg("collector rejected exchange; dropped")
}

// Read fetches the metadata with GetTrace and streams the bodies with GetBody,
// so body size is not limited by gRPC's per-message cap.
func (r *RemoteStorage) Read(ctx context.Context, traceID string, includeBodies bool) (*tracev1.HTTPExchange, error) {
	no := false
	resp, err := r.reader.GetTrace(ctx, &tracev1.GetTraceRequest{TraceId: traceID, IncludeBodies: &no})
	if status.Code(err) == codes.NotFound {
		return nil, ErrTraceNotFound
	}
	if err != nil {
		return nil, err
	}
	ex := resp.GetTrace()
	if !includeBodies {
		return ex, nil
	}
	if ex.RequestBody, err = r.fetchBody(ctx, traceID, tracev1.BodyKind_BODY_KIND_REQUEST); err != nil {
		return nil, err
	}
	if ex.ResponseBody, err = r.fetchBody(ctx, traceID, tracev1.BodyKind_BODY_KIND_RESPONSE); err != nil {
		return nil, err
	}
	return ex, nil
}

func (r *RemoteStorage) fetchBody(ctx context.Context, traceID string, kind tracev1.BodyKind) ([]byte, error) {
	st, err := r.bodies.GetBody(ctx, &tracev1.GetBodyRequest{TraceId: traceID, Kind: kind})
	if err != nil {
		return nil, err
	}
	var buf []byte
	for {
		c, err := st.Recv()
		if errors.Is(err, io.EOF) {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
		buf = append(buf, c.GetData()...)
	}
}

func (r *RemoteStorage) List(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	return r.reader.ListTraces(ctx, req)
}

// Prune is a no-op: the collector owns retention.
func (r *RemoteStorage) Prune(context.Context, time.Time) (int, int64, error) { return 0, 0, nil }

// Close waits for unacknowledged exchanges until ctx expires, then shuts down.
func (r *RemoteStorage) Close(ctx context.Context) error {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var drainErr error
drain:
	for {
		r.mu.Lock()
		n := len(r.inflight)
		r.mu.Unlock()
		if n == 0 {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			drainErr = fmt.Errorf("trace: %d exchanges not delivered to collector", n)
			break drain
		}
	}
	r.cancel()
	r.wg.Wait()
	if err := r.conn.Close(); err != nil && drainErr == nil {
		drainErr = err
	}
	return drainErr
}
