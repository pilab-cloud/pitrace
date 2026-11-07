package trace

import (
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// StreamRecord describes one non-HTTP connection (a raw TCP/TLS stream seen by
// an interceptor). It is stored as an exchange: the client-to-server bytes are
// the request body and the server-to-client bytes the response body.
type StreamRecord struct {
	Protocol   string // defaults to "tcp"
	ClientAddr string
	Started    time.Time
	Duration   time.Duration
	// ClientToServer and ServerToClient are the captured payloads. Leave them
	// nil to record metadata only; payloads of text protocols can contain
	// credentials, which the key masker cannot find.
	ClientToServer []byte
	ServerToClient []byte
	// BytesIn/BytesOut are the exact transferred byte counts.
	BytesIn, BytesOut int64
	Truncated         bool
	Labels            map[string]string
	Error             string
}

// RecordStream enqueues one stream record. Like Record it never blocks.
func (r *Recorder) RecordStream(s StreamRecord) {
	if !r.Enabled() || !r.shouldRecord("") {
		return
	}
	protocol := s.Protocol
	if protocol == "" {
		protocol = "tcp"
	}
	if s.Started.IsZero() {
		s.Started = time.Now().Add(-s.Duration)
	}
	in := capBytes(s.ClientToServer, r.cfg.MaxRequestBytes)
	out := capBytes(s.ServerToClient, r.cfg.MaxResponseBytes)

	client := &tracev1.ClientInfo{RemoteAddr: s.ClientAddr, RealIp: s.ClientAddr}
	sessionID := sessionKey("", "", s.ClientAddr, protocol, "")
	ex := &tracev1.HTTPExchange{
		SchemaVersion: 1,
		Summary: &tracev1.TraceSummary{
			TraceId:           uuid.NewString(),
			StartedAt:         timestamppb.New(s.Started),
			DurationMs:        s.Duration.Milliseconds(),
			Protocol:          protocol,
			Method:            "STREAM",
			ClientIp:          s.ClientAddr,
			RequestBytes:      s.BytesIn,
			ResponseBytes:     s.BytesOut,
			RequestTruncated:  s.Truncated || int64(len(in)) < s.BytesIn && len(in) > 0,
			ResponseTruncated: s.Truncated || int64(len(out)) < s.BytesOut && len(out) > 0,
			SessionId:         sessionID,
			SessionSeq:        r.nextSessionSeq(sessionID),
			Streamed:          true,
			Labels:            s.Labels,
			Error:             s.Error,
		},
		Client:      client,
		RequestBody: in, ResponseBody: out,
	}
	r.Record(ex)
}

func capBytes(b []byte, max int64) []byte {
	if max > 0 && int64(len(b)) > max {
		return b[:max]
	}
	return b
}
