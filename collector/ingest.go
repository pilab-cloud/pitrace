package collector

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

const (
	// DefaultMaxBodyBytes bounds one inline body (sent as Push chunks).
	DefaultMaxBodyBytes = 64 << 20
	// DefaultMaxStreamBytes bounds one body sent through PutBody.
	DefaultMaxStreamBytes = 1 << 30
)

// Ingest implements IngestService.
type Ingest struct {
	tracev1.UnimplementedIngestServiceServer

	meta      MetaStore
	bodies    BodyStore
	log       zerolog.Logger
	maxBody   int64
	maxStream int64
	storeWait time.Duration
}

// WithMaxStreamBytes overrides DefaultMaxStreamBytes.
func WithMaxStreamBytes(n int64) IngestOption { return func(i *Ingest) { i.maxStream = n } }

// IngestOption tunes an Ingest.
type IngestOption func(*Ingest)

// WithMaxBodyBytes overrides DefaultMaxBodyBytes.
func WithMaxBodyBytes(n int64) IngestOption { return func(i *Ingest) { i.maxBody = n } }

func NewIngest(meta MetaStore, bodies BodyStore, logger zerolog.Logger, opts ...IngestOption) *Ingest {
	i := &Ingest{meta: meta, bodies: bodies, log: logger, maxBody: DefaultMaxBodyBytes, maxStream: DefaultMaxStreamBytes, storeWait: 30 * time.Second}
	for _, o := range opts {
		o(i)
	}
	return i
}

// Push implements IngestService.
func (s *Ingest) Push(stream tracev1.IngestService_PushServer) error {
	var src *tracev1.SourceInfo
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch m := msg.GetMsg().(type) {
		case *tracev1.PushRequest_Hello:
			src = m.Hello
		case *tracev1.PushRequest_Begin:
			ack, err := s.ingestOne(stream, m.Begin, src)
			if err != nil {
				return err
			}
			if err := stream.Send(ack); err != nil {
				return err
			}
		default:
			return status.Error(codes.InvalidArgument, "body chunk without a preceding begin")
		}
	}
}

func (s *Ingest) ingestOne(stream tracev1.IngestService_PushServer, b *tracev1.ExchangeBegin, src *tracev1.SourceInfo) (*tracev1.PushAck, error) {
	ctx := stream.Context()
	ex := b.GetExchange()
	id := ex.GetSummary().GetTraceId()
	switch {
	case !ValidTraceID(id):
		return nil, status.Error(codes.InvalidArgument, "missing or malformed trace id")
	case len(ex.GetRequestBody()) > 0 || len(ex.GetResponseBody()) > 0:
		return nil, status.Error(codes.InvalidArgument, "bodies must be sent as chunks")
	case !b.GetRequestBodyUploaded() && int64(b.GetRequestBodySize()) > s.maxBody,
		!b.GetResponseBodyUploaded() && int64(b.GetResponseBodySize()) > s.maxBody:
		return nil, status.Errorf(codes.ResourceExhausted, "inline body larger than %d bytes", s.maxBody)
	}
	if ex.Summary.StartedAt == nil {
		ex.Summary.StartedAt = timestamppb.Now()
	}
	stampSource(ex.Summary, src)
	started := ex.Summary.StartedAt.AsTime()

	rec := &Record{
		Exchange:         ex,
		RequestBodySize:  int64(b.GetRequestBodySize()),
		ResponseBodySize: int64(b.GetResponseBodySize()),
	}
	var written []string
	cleanup := func() {
		// The stream context may be gone; deletion is best effort.
		bg, cancel := context.WithTimeout(context.Background(), s.storeWait)
		defer cancel()
		for _, k := range written {
			_ = s.bodies.Delete(bg, k)
		}
	}

	for _, part := range []struct {
		kind     tracev1.BodyKind
		size     int64
		key      *string
		uploaded bool
		truncate *bool
	}{
		{tracev1.BodyKind_BODY_KIND_REQUEST, rec.RequestBodySize, &rec.RequestBodyKey, b.GetRequestBodyUploaded(), &ex.Summary.RequestTruncated},
		{tracev1.BodyKind_BODY_KIND_RESPONSE, rec.ResponseBodySize, &rec.ResponseBodyKey, b.GetResponseBodyUploaded(), &ex.Summary.ResponseTruncated},
	} {
		if part.size == 0 {
			continue
		}
		key := BodyKey(started, id, part.kind)
		if part.uploaded {
			// Already stored by PutBody; keep the metadata even when the
			// object is missing or short (the stream was cut off).
			if got, err := s.bodies.Stat(ctx, key); err == nil && got == part.size {
				*part.key = key
			} else {
				s.log.Warn().Str("trace_id", id).Str("key", key).Int64("want", part.size).Err(err).Msg("uploaded body missing or short")
				*part.truncate = true
				if part.kind == tracev1.BodyKind_BODY_KIND_REQUEST {
					rec.RequestBodySize = 0
				} else {
					rec.ResponseBodySize = 0
				}
			}
			continue
		}
		if err := s.recvBody(ctx, stream, part.kind, part.size, key); err != nil {
			cleanup()
			return nil, err
		}
		*part.key = key
		written = append(written, key)
	}

	if err := s.meta.Insert(ctx, rec); err != nil {
		if errors.Is(err, ErrDuplicate) {
			// A re-sent exchange: the identical objects were just overwritten.
			return &tracev1.PushAck{TraceId: id, Stored: true}, nil
		}
		cleanup()
		s.log.Error().Err(err).Str("trace_id", id).Msg("metadata insert failed")
		return nil, status.Error(codes.Unavailable, "metadata store unavailable")
	}
	return &tracev1.PushAck{TraceId: id, Stored: true}, nil
}

// PutBody implements the client-streaming body upload: the bytes go from the
// gRPC stream into the object store as they arrive.
func (s *Ingest) PutBody(stream tracev1.IngestService_PutBodyServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := first.GetHeader()
	if h == nil || !ValidTraceID(h.GetTraceId()) || h.GetStartedAt() == nil {
		return status.Error(codes.InvalidArgument, "first message must be a header with trace id and start time")
	}
	if h.GetKind() != tracev1.BodyKind_BODY_KIND_REQUEST && h.GetKind() != tracev1.BodyKind_BODY_KIND_RESPONSE {
		return status.Error(codes.InvalidArgument, "body kind required")
	}
	key := BodyKey(h.GetStartedAt().AsTime(), h.GetTraceId(), h.GetKind())
	r := &chunkReader{stream: stream, limit: s.maxStream}
	if err := s.bodies.Put(stream.Context(), key, r, -1); err != nil {
		if errors.Is(r.err, errStreamTooLarge) {
			return status.Errorf(codes.ResourceExhausted, "body larger than %d bytes", s.maxStream)
		}
		if st, ok := status.FromError(r.err); ok && r.err != nil {
			return st.Err() // the client's own error (e.g. cancellation)
		}
		s.log.Error().Err(err).Str("key", key).Msg("streamed body put failed")
		return status.Error(codes.Unavailable, "body store unavailable")
	}
	return stream.SendAndClose(&tracev1.PutBodyResponse{Size: uint64(r.n)})
}

var errStreamTooLarge = errors.New("collector: stream exceeds size limit")

// chunkReader adapts a PutBody stream to an io.Reader without buffering more
// than one message.
type chunkReader struct {
	stream tracev1.IngestService_PutBodyServer
	limit  int64
	buf    []byte
	n      int64
	err    error // the first non-EOF error seen
}

func (c *chunkReader) Read(p []byte) (int, error) {
	for len(c.buf) == 0 {
		msg, err := c.stream.Recv()
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			c.err = err
			return 0, err
		}
		c.buf = msg.GetData()
	}
	n := copy(p, c.buf)
	c.buf = c.buf[n:]
	c.n += int64(n)
	if c.n > c.limit {
		c.err = errStreamTooLarge
		return n, errStreamTooLarge
	}
	return n, nil
}

// recvBody pipes exactly size bytes of chunks of one kind into the body store
// without buffering the body.
func (s *Ingest) recvBody(ctx context.Context, stream tracev1.IngestService_PushServer, kind tracev1.BodyKind, size int64, key string) error {
	pr, pw := io.Pipe()
	putErr := make(chan error, 1)
	go func() {
		err := s.bodies.Put(ctx, key, pr, size)
		// Unblocks the writer below if Put gave up early.
		_ = pr.CloseWithError(errPutDone(err))
		putErr <- err
	}()

	fail := func(err error) error {
		_ = pw.CloseWithError(err)
		<-putErr
		return err
	}

	var got int64
	for got < size {
		msg, err := stream.Recv()
		if err != nil {
			return fail(err)
		}
		chunk := msg.GetChunk()
		switch {
		case chunk == nil || chunk.GetKind() != kind:
			return fail(status.Error(codes.InvalidArgument, "unexpected message while receiving body"))
		case got+int64(len(chunk.GetData())) > size:
			return fail(status.Error(codes.InvalidArgument, "body longer than declared size"))
		}
		if _, err := pw.Write(chunk.GetData()); err != nil {
			<-putErr
			s.log.Error().Err(err).Str("key", key).Msg("body store write failed")
			return status.Error(codes.Unavailable, "body store unavailable")
		}
		got += int64(len(chunk.GetData()))
	}
	_ = pw.Close()
	if err := <-putErr; err != nil {
		s.log.Error().Err(err).Str("key", key).Msg("body store put failed")
		return status.Error(codes.Unavailable, "body store unavailable")
	}
	return nil
}

func errPutDone(err error) error {
	if err != nil {
		return err
	}
	return io.ErrClosedPipe
}

// stampSource records where an exchange came from without overriding labels
// the producer already set.
func stampSource(s *tracev1.TraceSummary, src *tracev1.SourceInfo) {
	if src == nil {
		return
	}
	if s.Labels == nil {
		s.Labels = make(map[string]string)
	}
	set := func(k, v string) {
		if v == "" {
			return
		}
		if _, ok := s.Labels[k]; !ok {
			s.Labels[k] = v
		}
	}
	set("source.id", src.GetSourceId())
	set("k8s.namespace", src.GetNamespace())
	set("k8s.pod", src.GetPod())
	set("k8s.workload", src.GetWorkload())
	set("k8s.node", src.GetNode())
}
