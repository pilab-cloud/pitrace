package collector

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"

	"google.golang.org/protobuf/proto"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

// MemoryMeta is an in-process MetaStore for tests and the collector's -memory
// development mode. It is not durable.
type MemoryMeta struct {
	mu   sync.RWMutex
	recs map[string]*Record
}

func NewMemoryMeta() *MemoryMeta { return &MemoryMeta{recs: make(map[string]*Record)} }

func (m *MemoryMeta) Insert(_ context.Context, rec *Record) error {
	id := rec.Exchange.GetSummary().GetTraceId()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.recs[id]; ok {
		return ErrDuplicate
	}
	m.recs[id] = &Record{
		Exchange:         proto.Clone(rec.Exchange).(*tracev1.HTTPExchange),
		RequestBodyKey:   rec.RequestBodyKey,
		ResponseBodyKey:  rec.ResponseBodyKey,
		RequestBodySize:  rec.RequestBodySize,
		ResponseBodySize: rec.ResponseBodySize,
	}
	return nil
}

func (m *MemoryMeta) Get(_ context.Context, traceID string) (*Record, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	rec, ok := m.recs[traceID]
	if !ok {
		return nil, trace.ErrTraceNotFound
	}
	cp := *rec
	cp.Exchange = proto.Clone(rec.Exchange).(*tracev1.HTTPExchange)
	return &cp, nil
}

func (m *MemoryMeta) List(_ context.Context, req *tracev1.ListTracesRequest) ([]*tracev1.TraceSummary, uint32, error) {
	m.mu.RLock()
	var matched []*tracev1.TraceSummary
	for _, rec := range m.recs {
		if s := rec.Exchange.GetSummary(); trace.MatchSummary(s, req) {
			matched = append(matched, s)
		}
	}
	m.mu.RUnlock()
	sort.Slice(matched, func(i, j int) bool {
		return matched[i].GetStartedAt().AsTime().After(matched[j].GetStartedAt().AsTime())
	})
	total := uint32(len(matched))
	limit, offset := listWindow(req)
	if offset >= len(matched) {
		return nil, total, nil
	}
	matched = matched[offset:]
	if len(matched) > limit {
		matched = matched[:limit]
	}
	out := make([]*tracev1.TraceSummary, len(matched))
	for i, s := range matched {
		out[i] = proto.Clone(s).(*tracev1.TraceSummary)
	}
	return out, total, nil
}

func (m *MemoryMeta) Close(context.Context) error { return nil }

// MemoryBodies is an in-process BodyStore.
type MemoryBodies struct {
	mu   sync.RWMutex
	objs map[string][]byte
}

func NewMemoryBodies() *MemoryBodies { return &MemoryBodies{objs: make(map[string][]byte)} }

func (b *MemoryBodies) Put(_ context.Context, key string, r io.Reader, size int64) error {
	var data []byte
	var err error
	if size < 0 {
		data, err = io.ReadAll(r)
	} else {
		data, err = io.ReadAll(io.LimitReader(r, size+1))
	}
	if err != nil {
		return err
	}
	if size >= 0 && int64(len(data)) != size {
		return io.ErrUnexpectedEOF
	}
	b.mu.Lock()
	b.objs[key] = data
	b.mu.Unlock()
	return nil
}

func (b *MemoryBodies) Get(_ context.Context, key string) (io.ReadCloser, error) {
	b.mu.RLock()
	data, ok := b.objs[key]
	b.mu.RUnlock()
	if !ok {
		return nil, trace.ErrTraceNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *MemoryBodies) Stat(_ context.Context, key string) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	data, ok := b.objs[key]
	if !ok {
		return 0, trace.ErrTraceNotFound
	}
	return int64(len(data)), nil
}

func (b *MemoryBodies) Delete(_ context.Context, key string) error {
	b.mu.Lock()
	delete(b.objs, key)
	b.mu.Unlock()
	return nil
}

// Len is the number of stored objects (tests).
func (b *MemoryBodies) Len() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.objs)
}
