package trace

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	minio "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// Storage is the persistence backend for trace records. Implementations may
// store segments on a shared filesystem (NFS) or in an S3-compatible bucket
// (MinIO / silo).
type Storage interface {
	// Write persists one exchange.
	Write(ctx context.Context, ex *tracev1.HTTPExchange) error
	// Read loads one exchange by trace id.
	Read(ctx context.Context, traceID string, includeBodies bool) (*tracev1.HTTPExchange, error)
	// List returns index summaries matching req.
	List(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error)
	// Prune removes old/large segments and returns counts.
	Prune(ctx context.Context, now time.Time) (removed int, freed int64, err error)
	// Close releases backend resources.
	Close(ctx context.Context) error
}

// ─── FileStorage ─────────────────────────────────────────────

type segmentWriter struct {
	root  string
	rel   string
	file  *os.File
	index *os.File
	size  int64
}

// FileStorage persists trace records as length-prefixed zstd frames on a
// (possibly shared/NFS) filesystem, exactly as the original recorder did.
type FileStorage struct {
	cfg     Config
	log     zerolog.Logger
	now     func() time.Time
	encoder *zstd.Encoder

	mu        sync.Mutex
	seg       *segmentWriter
	segKey    string
	segOpened time.Time
	lastPrune atomic.Int64
}

// NewFileStorage creates a filesystem-backed storage rooted at cfg.Dir.
func NewFileStorage(cfg Config, logger zerolog.Logger) (*FileStorage, error) {
	cfg = cfg.withDefaults()
	if err := ensureDir(cfg.Dir); err != nil {
		return nil, err
	}
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return nil, err
	}
	now := time.Now
	fs := &FileStorage{cfg: cfg, log: logger, now: now, encoder: enc}
	return fs, nil
}

// Write persists one exchange as a length-prefixed zstd frame and appends an
// index entry.
func (f *FileStorage) Write(ctx context.Context, ex *tracev1.HTTPExchange) error {
	now := f.now()
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.ensureSegment(now); err != nil {
		return err
	}

	payload, err := proto.Marshal(ex)
	if err != nil {
		return err
	}
	compressed := f.encoder.EncodeAll(payload, nil)
	offset := f.seg.size

	var header [4]byte
	binary.LittleEndian.PutUint32(header[:], uint32(len(compressed)))
	if _, err := f.seg.file.Write(header[:]); err != nil {
		return err
	}
	if _, err := f.seg.file.Write(compressed); err != nil {
		return err
	}
	length := int64(len(header) + len(compressed))
	f.seg.size += length

	entry := indexEntryFrom(ex, f.seg.rel, offset, length)
	line, err := json.Marshal(entry)
	if err == nil {
		_, _ = f.seg.index.Write(append(line, '\n'))
	}
	return nil
}

// Read loads one exchange by trace id from the filesystem segments.
func (f *FileStorage) Read(ctx context.Context, traceID string, includeBodies bool) (*tracev1.HTTPExchange, error) {
	if strings.TrimSpace(traceID) == "" {
		return nil, ErrTraceNotFound
	}
	for _, file := range indexFiles(f.cfg.Dir) {
		entries, err := readIndexFile(file)
		if err != nil {
			continue
		}
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].TraceID != traceID {
				continue
			}
			ex, err := f.readRecord(entries[i])
			if err != nil {
				return nil, err
			}
			if !includeBodies {
				ex.RequestBody = nil
				ex.ResponseBody = nil
			}
			return ex, nil
		}
	}
	return nil, ErrTraceNotFound
}

func (f *FileStorage) readRecord(entry *indexEntry) (*tracev1.HTTPExchange, error) {
	if entry.Length < 4 || entry.Length > maxRecordBytes {
		return nil, errors.New("trace: invalid record length")
	}
	file, err := os.Open(filepath.Join(f.cfg.Dir, entry.File))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var header [4]byte
	if _, err := file.ReadAt(header[:], entry.Offset); err != nil {
		return nil, err
	}
	compressedLen := int64(binary.LittleEndian.Uint32(header[:]))
	if compressedLen <= 0 || compressedLen+4 > entry.Length {
		return nil, errors.New("trace: invalid frame")
	}
	compressed := make([]byte, compressedLen)
	if _, err := file.ReadAt(compressed, entry.Offset+4); err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	payload, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	ex := &tracev1.HTTPExchange{}
	if err := proto.Unmarshal(payload, ex); err != nil {
		return nil, err
	}
	return ex, nil
}

// List returns index summaries matching req, newest first.
func (f *FileStorage) List(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := int(req.GetOffset())

	var matched []*indexEntry
	for _, file := range indexFiles(f.cfg.Dir) {
		entries, err := readIndexFile(file)
		if err != nil {
			continue
		}
		for i := len(entries) - 1; i >= 0; i-- {
			if entries[i].matches(req) {
				matched = append(matched, entries[i])
			}
		}
	}

	total := len(matched)
	if offset >= total {
		return &tracev1.ListTracesResponse{Total: uint32(total)}, nil
	}
	matched = matched[offset:]
	if len(matched) > limit {
		matched = matched[:limit]
	}

	out := make([]*tracev1.TraceSummary, 0, len(matched))
	for _, e := range matched {
		s := e.summary()
		out = append(out, s)
	}
	return &tracev1.ListTracesResponse{Traces: out, Total: uint32(total)}, nil
}

// Prune deletes whole segments older than MaxAge or over MaxBytes.
func (f *FileStorage) Prune(ctx context.Context, now time.Time) (int, int64, error) {
	if now.IsZero() {
		now = f.now()
	}
	type segment struct {
		path string
		mod  time.Time
		size int64
	}
	var segments []segment
	walkErr := filepath.WalkDir(f.cfg.Dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || filepath.Ext(path) != ".zst" {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		segments = append(segments, segment{path: path, mod: info.ModTime(), size: info.Size()})
		return nil
	})
	if walkErr != nil {
		return 0, 0, walkErr
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i].mod.Before(segments[j].mod) })

	var total int64
	for _, s := range segments {
		total += s.size
	}
	removed := 0
	var freed int64
	for _, s := range segments {
		if now.Sub(s.mod) <= f.cfg.MaxAge && total <= f.cfg.MaxBytes {
			continue
		}
		if err := os.Remove(s.path); err != nil {
			continue
		}
		_ = os.Remove(strings.TrimSuffix(s.path, ".pb.zst") + ".index.jsonl")
		removed++
		freed += s.size
		total -= s.size
	}
	f.lastPrune.Store(now.UnixNano())
	return removed, freed, nil
}

// Close flushes and closes the active segment file.
func (f *FileStorage) Close(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.seg != nil {
		_ = f.seg.file.Sync()
		_ = f.seg.file.Close()
		_ = f.seg.index.Sync()
		_ = f.seg.index.Close()
		f.seg = nil
	}
	if f.encoder != nil {
		f.encoder.Close()
	}
	return nil
}

func (f *FileStorage) ensureSegment(now time.Time) error {
	key := now.UTC().Format("2006-01-02/15")
	if f.seg != nil && key == f.segKey &&
		now.Sub(f.segOpened) < f.cfg.RotateInterval &&
		f.seg.size < f.cfg.RotateBytes {
		return nil
	}
	if f.seg != nil {
		_ = f.seg.file.Sync()
		_ = f.seg.file.Close()
		_ = f.seg.index.Sync()
		_ = f.seg.index.Close()
		f.seg = nil
	}

	dir := segmentDir(f.cfg.Dir, now)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	rel := now.UTC().Format("2006-01-02/15-04-05") + ".pb.zst"
	path := filepath.Join(f.cfg.Dir, rel)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	index, err := os.OpenFile(strings.TrimSuffix(path, ".pb.zst")+".index.jsonl", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		_ = file.Close()
		return err
	}

	f.seg = &segmentWriter{root: f.cfg.Dir, rel: rel, file: file, index: index, size: info.Size()}
	f.segKey = key
	f.segOpened = now
	return nil
}

// ─── S3Storage (MinIO / silo) ───────────────────────────────

// S3Storage persists each trace record as an object in an S3-compatible bucket
// and keeps a per-day JSONL index object for listing/search.
type S3Storage struct {
	cfg    Config
	log    zerolog.Logger
	client *minio.Client
	bucket string
	prefix string
	now    func() time.Time
	enc    *zstd.Encoder
}

// NewS3Storage creates an S3-compatible (MinIO/silo) storage backend.
func NewS3Storage(cfg Config, logger zerolog.Logger, endpoint, accessKey, secretKey, bucket, prefix string, useSSL bool) (*S3Storage, error) {
	cfg = cfg.withDefaults()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	// ensure bucket exists
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	if prefix == "" {
		prefix = "traces"
	}
	s := &S3Storage{cfg: cfg, log: logger, client: client, bucket: bucket, prefix: prefix, now: time.Now, enc: enc}
	return s, nil
}

func (s *S3Storage) objectKey(traceID string) string {
	return fmt.Sprintf("%s/records/%s.pb.zst", s.prefix, traceID)
}

func (s *S3Storage) indexKey(t time.Time) string {
	return fmt.Sprintf("%s/index/%s.jsonl", s.prefix, t.UTC().Format("2006-01-02"))
}

// Write persists one exchange as a zstd object and appends to the daily index.
func (s *S3Storage) Write(ctx context.Context, ex *tracev1.HTTPExchange) error {
	payload, err := proto.Marshal(ex)
	if err != nil {
		return err
	}
	compressed := s.enc.EncodeAll(payload, nil)

	// put record object
	_, err = s.client.PutObject(ctx, s.bucket, s.objectKey(ex.Summary.GetTraceId()), bytes.NewReader(compressed), int64(len(compressed)), minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		return err
	}

	// append to daily index
	entry := indexEntryFrom(ex, s.objectKey(ex.Summary.GetTraceId()), 0, int64(len(compressed)))
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')

	// read-modify-write index object
	idxKey := s.indexKey(s.now())
	var existing []byte
	obj, err := s.client.GetObject(ctx, s.bucket, idxKey, minio.GetObjectOptions{})
	if err == nil {
		b, _ := io.ReadAll(obj)
		existing = b
		obj.Close()
	}
	existing = append(existing, line...)
	_, err = s.client.PutObject(ctx, s.bucket, idxKey, bytes.NewReader(existing), int64(len(existing)), minio.PutObjectOptions{ContentType: "application/jsonl"})
	return err
}

// Read loads one exchange by trace id from its S3 object.
func (s *S3Storage) Read(ctx context.Context, traceID string, includeBodies bool) (*tracev1.HTTPExchange, error) {
	obj, err := s.client.GetObject(ctx, s.bucket, s.objectKey(traceID), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	compressed, err := io.ReadAll(obj)
	if err != nil {
		return nil, err
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()
	payload, err := decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	ex := &tracev1.HTTPExchange{}
	if err := proto.Unmarshal(payload, ex); err != nil {
		return nil, err
	}
	if !includeBodies {
		ex.RequestBody = nil
		ex.ResponseBody = nil
	}
	return ex, nil
}

// List returns index summaries matching req from all daily index objects.
func (s *S3Storage) List(ctx context.Context, req *tracev1.ListTracesRequest) (*tracev1.ListTracesResponse, error) {
	// list index objects under prefix/index/
	var matched []*indexEntry
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix + "/index/", Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		o, err := s.client.GetObject(ctx, s.bucket, obj.Key, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(o)
		o.Close()
		// parse lines
		for _, line := range splitLines(b) {
			entry := &indexEntry{}
			if err := json.Unmarshal(line, entry); err != nil {
				continue
			}
			if entry.matches(req) {
				matched = append(matched, entry)
			}
		}
	}

	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	offset := int(req.GetOffset())
	total := len(matched)
	if offset >= total {
		return &tracev1.ListTracesResponse{Total: uint32(total)}, nil
	}
	matched = matched[offset:]
	if len(matched) > limit {
		matched = matched[:limit]
	}
	out := make([]*tracev1.TraceSummary, 0, len(matched))
	for _, e := range matched {
		s := e.summary()
		out = append(out, s)
	}
	return &tracev1.ListTracesResponse{Traces: out, Total: uint32(total)}, nil
}

// Prune removes objects older than MaxAge.
func (s *S3Storage) Prune(ctx context.Context, now time.Time) (int, int64, error) {
	// Prune index/record objects older than MaxAge
	cutoff := now.Add(-s.cfg.MaxAge)
	removed := 0
	var freed int64
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: s.prefix + "/", Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		if obj.LastModified.After(cutoff) {
			continue
		}
		// size
		info, err := s.client.StatObject(ctx, s.bucket, obj.Key, minio.StatObjectOptions{})
		if err == nil {
			freed += info.Size
		}
		if err := s.client.RemoveObject(ctx, s.bucket, obj.Key, minio.RemoveObjectOptions{}); err == nil {
			removed++
		}
	}
	return removed, freed, nil
}

// Close releases encoder resources.
func (s *S3Storage) Close(ctx context.Context) error {
	if s.enc != nil {
		s.enc.Close()
	}
	return nil
}

func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, c := range b {
		if c == '\n' {
			if i > start {
				out = append(out, b[start:i])
			}
			start = i + 1
		}
	}
	if start < len(b) {
		out = append(out, b[start:])
	}
	return out
}
