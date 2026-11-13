package trace

import (
	"context"
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// Service is the gRPC read side of a Recorder. Daemons serve it on a
// loopback-only port; CLI tools use it so no tool reads trace files directly.
type Service struct {
	tracev1.UnimplementedTraceServiceServer

	recorder *Recorder
}

// NewService wraps a recorder. A nil recorder produces empty responses, which
// keeps the service mountable even when tracing is disabled.
func NewService(recorder *Recorder) *Service {
	return &Service{recorder: recorder}
}

// ListTraces returns metadata for matching exchanges, newest first.
func (s *Service) ListTraces(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	if s.recorder == nil {
		return &tracev1.ListTracesResponse{}, nil
	}
	return s.recorder.List(ctx, req)
}

// GetTrace returns one exchange, optionally with captured bodies.
func (s *Service) GetTrace(ctx context.Context, req *tracev1.GetTraceRequest) (*tracev1.GetTraceResponse, error) {
	id := strings.TrimSpace(req.GetTraceId())
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "trace_id is required")
	}
	if s.recorder == nil {
		return nil, status.Error(codes.NotFound, "tracing is disabled")
	}
	// include_bodies defaults to true; list/triage callers turn it off.
	includeBodies := req.IncludeBodies == nil || req.GetIncludeBodies()
	ex, err := s.recorder.Get(ctx, id, includeBodies)
	if err != nil {
		if errors.Is(err, ErrTraceNotFound) {
			return nil, status.Error(codes.NotFound, "trace not found")
		}
		return nil, status.Errorf(codes.Internal, "read trace: %v", err)
	}
	return &tracev1.GetTraceResponse{Trace: ex}, nil
}

// TailTraces follows live summaries (no bodies) as they are recorded.
func (s *Service) TailTraces(req *tracev1.TailTracesRequest, stream tracev1.TraceService_TailTracesServer) error {
	if s.recorder == nil {
		return status.Error(codes.Unavailable, "tracing is disabled")
	}
	ch, cancel := s.recorder.Tail(stream.Context(), req.GetProtocol(), req.GetAccountId(), int(req.GetBacklog()))
	defer cancel()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case summary, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(&tracev1.TailTracesResponse{Trace: summary}); err != nil {
				return err
			}
		}
	}
}
