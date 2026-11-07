package collector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"google.golang.org/protobuf/proto"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

// MongoConfig configures MongoMeta.
type MongoConfig struct {
	URI        string
	Database   string // default "hackergate"
	Collection string // default "exchanges"
	// Retention is how long records live; 0 keeps them forever.
	Retention time.Duration
}

// MongoMeta is the MongoDB MetaStore. Every queryable field is a top-level
// scalar (or the label_set/header arrays) so the indexes below cover the
// filters; the full summary and exchange are kept as proto bytes, which makes
// the schema trivially forward compatible.
type MongoMeta struct {
	client    *mongo.Client
	coll      *mongo.Collection
	retention time.Duration
}

type headerDoc struct {
	Name  string `bson:"name"`
	Value string `bson:"value"`
}

// exchangeDoc is the stored document. protocol, account_id and operation are
// lower-cased: filters on them are case-insensitive, and equality on a
// normalised field stays indexable (the original casing lives in summary_pb).
type exchangeDoc struct {
	ID         string    `bson:"_id"`
	StartedAt  time.Time `bson:"started_at"`
	ExpireAt   time.Time `bson:"expire_at,omitempty"`
	DurationMS int64     `bson:"duration_ms"`
	Protocol   string    `bson:"protocol,omitempty"`
	Operation  string    `bson:"operation,omitempty"`
	AccountID  string    `bson:"account_id,omitempty"`
	Principal  string    `bson:"principal,omitempty"`
	Method     string    `bson:"method,omitempty"`
	Path       string    `bson:"path,omitempty"`
	Status     int32     `bson:"status"`
	ClientIP   string    `bson:"client_ip,omitempty"`
	UserAgent  string    `bson:"user_agent,omitempty"`
	SessionID  string    `bson:"session_id,omitempty"`
	Error      string    `bson:"error,omitempty"`
	Streamed   bool      `bson:"streamed,omitempty"`
	// LabelSet holds "key=value" strings: label keys may contain dots or
	// dollar signs, which are awkward as document field names.
	LabelSet    []string    `bson:"label_set,omitempty"`
	ReqHeaders  []headerDoc `bson:"req_headers,omitempty"`
	RespHeaders []headerDoc `bson:"resp_headers,omitempty"`
	// Text is the lower-cased haystack of the free-text search.
	Text string `bson:"text,omitempty"`

	SummaryPB  []byte `bson:"summary_pb"`
	ExchangePB []byte `bson:"exchange_pb"`

	RequestBodyKey   string `bson:"req_body_key,omitempty"`
	ResponseBodyKey  string `bson:"resp_body_key,omitempty"`
	RequestBodySize  int64  `bson:"req_body_size,omitempty"`
	ResponseBodySize int64  `bson:"resp_body_size,omitempty"`
}

// NewMongoMeta connects and ensures the indexes exist.
func NewMongoMeta(ctx context.Context, cfg MongoConfig) (*MongoMeta, error) {
	if cfg.URI == "" {
		return nil, errors.New("collector: mongo uri required")
	}
	if cfg.Database == "" {
		cfg.Database = "hackergate"
	}
	if cfg.Collection == "" {
		cfg.Collection = "exchanges"
	}
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("collector: mongo connect: %w", err)
	}
	m := &MongoMeta{client: client, coll: client.Database(cfg.Database).Collection(cfg.Collection), retention: cfg.Retention}
	if err := m.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return m, nil
}

func (m *MongoMeta) ensureIndexes(ctx context.Context) error {
	desc := func(field string) bson.D { return bson.D{{Key: field, Value: 1}, {Key: "started_at", Value: -1}} }
	models := []mongo.IndexModel{
		{Keys: bson.D{{Key: "started_at", Value: -1}}},
		{Keys: desc("protocol")},
		{Keys: desc("account_id")},
		{Keys: desc("session_id")},
		{Keys: desc("status")},
		{Keys: desc("label_set")},
		// Replaced when the retention changes: see the conflict handling below.
		{Keys: bson.D{{Key: "expire_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(0).SetName("expire_at_ttl")},
	}
	if _, err := m.coll.Indexes().CreateMany(ctx, models); err != nil {
		return fmt.Errorf("collector: mongo indexes: %w", err)
	}
	return nil
}

func headerDocs(hs []*tracev1.Header) []headerDoc {
	out := make([]headerDoc, 0, len(hs))
	for _, h := range hs {
		name := h.GetName()
		if name == "" && h.GetWellKnown() != tracev1.WellKnownHeader_WELL_KNOWN_HEADER_UNSPECIFIED {
			name = wellKnownName(h.GetWellKnown())
		}
		if name == "" {
			continue
		}
		out = append(out, headerDoc{Name: strings.ToLower(name), Value: h.GetValue()})
	}
	return out
}

func wellKnownName(h tracev1.WellKnownHeader) string {
	n := strings.TrimPrefix(h.String(), "WELL_KNOWN_HEADER_")
	return strings.ToLower(strings.ReplaceAll(n, "_", "-"))
}

func textHaystack(s *tracev1.TraceSummary) string {
	return strings.ToLower(strings.Join([]string{
		s.GetPath(), s.GetOperation(), s.GetUserAgent(), strings.Join(s.GetResponseCodes(), " "),
	}, " "))
}

func toDoc(rec *Record, retention time.Duration) (*exchangeDoc, error) {
	ex := rec.Exchange
	s := ex.GetSummary()
	exPB, err := proto.Marshal(ex)
	if err != nil {
		return nil, err
	}
	sumPB, err := proto.Marshal(s)
	if err != nil {
		return nil, err
	}
	started := s.GetStartedAt().AsTime()
	d := &exchangeDoc{
		ID:               s.GetTraceId(),
		StartedAt:        started,
		DurationMS:       s.GetDurationMs(),
		Protocol:         strings.ToLower(s.GetProtocol()),
		Operation:        strings.ToLower(s.GetOperation()),
		AccountID:        strings.ToLower(s.GetAccountId()),
		Principal:        s.GetPrincipal(),
		Method:           s.GetMethod(),
		Path:             s.GetPath(),
		Status:           s.GetStatus(),
		ClientIP:         s.GetClientIp(),
		UserAgent:        s.GetUserAgent(),
		SessionID:        s.GetSessionId(),
		Error:            s.GetError(),
		Streamed:         s.GetStreamed(),
		ReqHeaders:       headerDocs(ex.GetRequestHeaders()),
		RespHeaders:      headerDocs(ex.GetResponseHeaders()),
		Text:             textHaystack(s),
		SummaryPB:        sumPB,
		ExchangePB:       exPB,
		RequestBodyKey:   rec.RequestBodyKey,
		ResponseBodyKey:  rec.ResponseBodyKey,
		RequestBodySize:  rec.RequestBodySize,
		ResponseBodySize: rec.ResponseBodySize,
	}
	for k, v := range s.GetLabels() {
		d.LabelSet = append(d.LabelSet, k+"="+v)
	}
	if retention > 0 {
		d.ExpireAt = started.Add(retention)
	}
	return d, nil
}

func (m *MongoMeta) Insert(ctx context.Context, rec *Record) error {
	d, err := toDoc(rec, m.retention)
	if err != nil {
		return err
	}
	if _, err := m.coll.InsertOne(ctx, d); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return ErrDuplicate
		}
		return err
	}
	return nil
}

func (m *MongoMeta) Get(ctx context.Context, traceID string) (*Record, error) {
	var d exchangeDoc
	err := m.coll.FindOne(ctx, bson.D{{Key: "_id", Value: traceID}}).Decode(&d)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, trace.ErrTraceNotFound
	}
	if err != nil {
		return nil, err
	}
	ex := &tracev1.HTTPExchange{}
	if err := proto.Unmarshal(d.ExchangePB, ex); err != nil {
		return nil, fmt.Errorf("collector: corrupt exchange %s: %w", traceID, err)
	}
	return &Record{
		Exchange:         ex,
		RequestBodyKey:   d.RequestBodyKey,
		ResponseBodyKey:  d.ResponseBodyKey,
		RequestBodySize:  d.RequestBodySize,
		ResponseBodySize: d.ResponseBodySize,
	}, nil
}

// listFilter translates a ListTracesRequest into a Mongo filter with the same
// semantics as trace.MatchSummary.
func listFilter(req *tracev1.ListTracesRequest) bson.D {
	f := bson.D{}
	if req == nil {
		return f
	}
	started := bson.D{}
	if req.GetSince() != nil {
		started = append(started, bson.E{Key: "$gte", Value: req.GetSince().AsTime()})
	}
	if req.GetUntil() != nil {
		started = append(started, bson.E{Key: "$lte", Value: req.GetUntil().AsTime()})
	}
	if len(started) > 0 {
		f = append(f, bson.E{Key: "started_at", Value: started})
	}
	if p := req.GetProtocol(); p != "" {
		f = append(f, bson.E{Key: "protocol", Value: strings.ToLower(p)})
	}
	if a := req.GetAccountId(); a != "" {
		f = append(f, bson.E{Key: "account_id", Value: strings.ToLower(a)})
	}
	if op := req.GetOperation(); op != "" {
		f = append(f, bson.E{Key: "operation", Value: strings.ToLower(op)})
	}
	if s := req.GetSessionId(); s != "" {
		f = append(f, bson.E{Key: "session_id", Value: s})
	}
	status := bson.D{}
	if req.GetMinStatus() > 0 {
		status = append(status, bson.E{Key: "$gte", Value: req.GetMinStatus()})
	}
	if req.GetMaxStatus() > 0 {
		status = append(status, bson.E{Key: "$lte", Value: req.GetMaxStatus()})
	}
	if len(status) > 0 {
		f = append(f, bson.E{Key: "status", Value: status})
	}
	if text := strings.ToLower(strings.TrimSpace(req.GetText())); text != "" {
		f = append(f, bson.E{Key: "text", Value: bson.D{{Key: "$regex", Value: regexp.QuoteMeta(text)}}})
	}
	if len(req.GetLabels()) > 0 {
		all := make(bson.A, 0, len(req.GetLabels()))
		for k, v := range req.GetLabels() {
			all = append(all, k+"="+v)
		}
		f = append(f, bson.E{Key: "label_set", Value: bson.D{{Key: "$all", Value: all}}})
	}
	return f
}

func (m *MongoMeta) List(ctx context.Context, req *tracev1.ListTracesRequest) ([]*tracev1.TraceSummary, uint32, error) {
	filter := listFilter(req)
	total, err := m.coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	limit, offset := listWindow(req)
	cur, err := m.coll.Find(ctx, filter, options.Find().
		SetProjection(bson.D{{Key: "summary_pb", Value: 1}}).
		SetSort(bson.D{{Key: "started_at", Value: -1}, {Key: "_id", Value: -1}}).
		SetSkip(int64(offset)).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, 0, err
	}
	defer cur.Close(ctx)

	var out []*tracev1.TraceSummary
	for cur.Next(ctx) {
		var d struct {
			SummaryPB []byte `bson:"summary_pb"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, 0, err
		}
		s := &tracev1.TraceSummary{}
		if err := proto.Unmarshal(d.SummaryPB, s); err != nil {
			return nil, 0, err
		}
		out = append(out, s)
	}
	return out, uint32(total), cur.Err()
}

func (m *MongoMeta) Close(ctx context.Context) error { return m.client.Disconnect(ctx) }

// Ping checks the database is reachable (readiness probes).
func (m *MongoMeta) Ping(ctx context.Context) error { return m.client.Ping(ctx, nil) }
