// Package collector is the stateless server side of the hackergate trace
// pipeline. Sidecars stream recorded exchanges to it (IngestService); it
// writes the bodies to an object store as they arrive and the metadata,
// headers and search fields to a database. Because it keeps no local state,
// any number of replicas can run behind one load balancer.
package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// ErrDuplicate is returned by MetaStore.Insert when the trace id already
// exists, which happens when a sidecar re-sends an unacknowledged exchange.
var ErrDuplicate = errors.New("collector: duplicate trace id")

// Record is what the metadata store keeps for one exchange. The bodies live in
// the BodyStore under the given keys.
type Record struct {
	// Exchange holds everything but the bodies.
	Exchange *tracev1.HTTPExchange

	RequestBodyKey   string
	ResponseBodyKey  string
	RequestBodySize  int64
	ResponseBodySize int64
}

// MetaStore persists and searches exchange metadata.
type MetaStore interface {
	// Insert stores a record; a repeated trace id returns ErrDuplicate.
	Insert(ctx context.Context, rec *Record) error
	// Get returns one record or trace.ErrTraceNotFound.
	Get(ctx context.Context, traceID string) (*Record, error)
	// List returns summaries newest first plus the total match count.
	List(ctx context.Context, req *tracev1.ListTracesRequest) ([]*tracev1.TraceSummary, uint32, error)
	Close(ctx context.Context) error
}

// BodyStore persists bodies as streams.
type BodyStore interface {
	// Put stores the bytes read from r. A size >= 0 is exact; -1 means the
	// length is unknown and r is consumed as a stream (multipart upload).
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	// Stat returns the stored size or trace.ErrTraceNotFound.
	Stat(ctx context.Context, key string) (int64, error)
	Delete(ctx context.Context, key string) error
}

// BodyKey is the object key of one body. The date prefix keeps keys spread out
// and lets a lifecycle rule expire whole days.
func BodyKey(started time.Time, traceID string, kind tracev1.BodyKind) string {
	suffix := "req"
	if kind == tracev1.BodyKind_BODY_KIND_RESPONSE {
		suffix = "resp"
	}
	return fmt.Sprintf("bodies/%s/%s.%s", started.UTC().Format("2006/01/02"), traceID, suffix)
}

// ValidTraceID reports whether id is safe to embed in an object key: ids come
// from the network, so only a conservative alphabet is accepted.
func ValidTraceID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for _, c := range id {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

func listWindow(req *tracev1.ListTracesRequest) (limit, offset int) {
	limit = int(req.GetLimit())
	if limit <= 0 {
		limit = defaultListLimit
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	return limit, int(req.GetOffset())
}
