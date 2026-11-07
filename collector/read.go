package collector

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

const bodyChunkSize = 64 << 10

// maxInlineBodies bounds the request+response bytes GetTrace puts into one
// message (gRPC clients default to a 4 MiB message limit and the collector
// would have to hold the bodies in memory). Bigger bodies are read with
// BodyService.GetBody, which streams them.
const maxInlineBodies = 3 << 20

// Reader implements TraceService (list/get) on top of the stores. TailTraces
// is not served by the collector.
type Reader struct {
	tracev1.UnimplementedTraceServiceServer

	meta   MetaStore
	bodies BodyStore
}

func NewReader(meta MetaStore, bodies BodyStore) *Reader {
	return &Reader{meta: meta, bodies: bodies}
}

func (r *Reader) ListTraces(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	traces, total, err := r.meta.List(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &tracev1.ListTracesResponse{Traces: traces, Total: total}, nil
}

func (r *Reader) GetTrace(ctx context.Context, req *tracev1.GetTraceRequest) (*tracev1.GetTraceResponse, error) {
	if req.GetTraceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "trace_id required")
	}
	rec, err := r.meta.Get(ctx, req.GetTraceId())
	if errors.Is(err, trace.ErrTraceNotFound) {
		return nil, status.Error(codes.NotFound, "trace not found")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	ex := rec.Exchange
	if req.IncludeBodies == nil || req.GetIncludeBodies() {
		if rec.RequestBodySize+rec.ResponseBodySize > maxInlineBodies {
			return nil, status.Errorf(codes.ResourceExhausted,
				"bodies are %d bytes, too large to inline; stream them with BodyService.GetBody", rec.RequestBodySize+rec.ResponseBodySize)
		}
		if ex.RequestBody, err = r.readAll(ctx, rec.RequestBodyKey); err != nil {
			return nil, err
		}
		if ex.ResponseBody, err = r.readAll(ctx, rec.ResponseBodyKey); err != nil {
			return nil, err
		}
	}
	return &tracev1.GetTraceResponse{Trace: ex}, nil
}

func (r *Reader) readAll(ctx context.Context, key string) ([]byte, error) {
	if key == "" {
		return nil, nil
	}
	rc, err := r.bodies.Get(ctx, key)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "body unavailable: "+err.Error())
	}
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, maxInlineBodies))
	if err != nil {
		return nil, status.Error(codes.Unavailable, "body read failed: "+err.Error())
	}
	return data, nil
}

// Bodies implements BodyService: stored bodies are streamed in 64 KiB chunks.
type Bodies struct {
	tracev1.UnimplementedBodyServiceServer

	meta   MetaStore
	bodies BodyStore
}

func NewBodies(meta MetaStore, bodies BodyStore) *Bodies { return &Bodies{meta: meta, bodies: bodies} }

func (b *Bodies) GetBody(req *tracev1.GetBodyRequest, stream tracev1.BodyService_GetBodyServer) error {
	ctx := stream.Context()
	rec, err := b.meta.Get(ctx, req.GetTraceId())
	if errors.Is(err, trace.ErrTraceNotFound) {
		return status.Error(codes.NotFound, "trace not found")
	}
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	var key string
	switch req.GetKind() {
	case tracev1.BodyKind_BODY_KIND_REQUEST:
		key = rec.RequestBodyKey
	case tracev1.BodyKind_BODY_KIND_RESPONSE:
		key = rec.ResponseBodyKey
	default:
		return status.Error(codes.InvalidArgument, "kind required")
	}
	if key == "" {
		return nil // an exchange without that body is an empty stream
	}
	rc, err := b.bodies.Get(ctx, key)
	if err != nil {
		return status.Error(codes.Unavailable, "body unavailable: "+err.Error())
	}
	defer rc.Close()
	buf := make([]byte, bodyChunkSize)
	for {
		n, err := rc.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&tracev1.BodyChunk{Kind: req.GetKind(), Data: buf[:n]}); sendErr != nil {
				return sendErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return status.Error(codes.Unavailable, "body read failed: "+err.Error())
		}
	}
}

// Register mounts every collector service on a gRPC server.
func Register(s grpc.ServiceRegistrar, meta MetaStore, bodies BodyStore, ingest *Ingest) {
	tracev1.RegisterIngestServiceServer(s, ingest)
	tracev1.RegisterTraceServiceServer(s, NewReader(meta, bodies))
	tracev1.RegisterBodyServiceServer(s, NewBodies(meta, bodies))
}
