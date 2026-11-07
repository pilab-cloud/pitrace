package collector

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

func TestListFilterMirrorsMatchSemantics(t *testing.T) {
	since := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	f := listFilter(&tracev1.ListTracesRequest{
		Since:     timestamppb.New(since),
		Protocol:  "HTTP",
		MinStatus: 500,
		Text:      "a.b(c",
		Labels:    map[string]string{"k8s.pod": "p1"},
	})
	got := map[string]any{}
	for _, e := range f {
		got[e.Key] = e.Value
	}
	if got["protocol"] != "http" {
		t.Errorf("protocol must be lower-cased: %v", got["protocol"])
	}
	if _, ok := got["started_at"]; !ok {
		t.Error("missing time filter")
	}
	if re := got["text"].(bson.D)[0].Value; re != `a\.b\(c` {
		t.Errorf("free text must be regex-quoted, got %v", re)
	}
	if all := got["label_set"].(bson.D)[0].Value.(bson.A); len(all) != 1 || all[0] != "k8s.pod=p1" {
		t.Errorf("label filter = %v", all)
	}
	if len(listFilter(nil)) != 0 {
		t.Error("nil request must match everything")
	}
}

func TestHeaderDocsResolveWellKnownNames(t *testing.T) {
	got := headerDocs([]*tracev1.Header{
		{WellKnown: tracev1.WellKnownHeader_WELL_KNOWN_HEADER_X_FORWARDED_FOR, Value: "1.2.3.4"},
		{Name: "X-Custom", Value: "v"},
		{},
	})
	if len(got) != 2 || got[0].Name != "x-forwarded-for" || got[1].Name != "x-custom" {
		t.Fatalf("headerDocs = %+v", got)
	}
}

// Backend contract: run against real servers with
//
//	MONGO_URI=mongodb://localhost:27017 MINIO_ENDPOINT=localhost:9000 \
//	MINIO_ACCESS_KEY=... MINIO_SECRET_KEY=... go test ./collector -run Integration
func TestMongoMetaIntegration(t *testing.T) {
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		t.Skip("MONGO_URI not set")
	}
	ctx := context.Background()
	m, err := NewMongoMeta(ctx, MongoConfig{URI: uri, Database: "hackergate_test", Collection: "t" + time.Now().Format("150405"), Retention: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.coll.Drop(ctx); _ = m.Close(ctx) }()

	rec := &Record{
		Exchange: &tracev1.HTTPExchange{
			Summary: &tracev1.TraceSummary{
				TraceId: "m1", StartedAt: timestamppb.Now(), Protocol: "HTTP", Path: "/Cart/Items", Status: 503,
				Labels: map[string]string{"k8s.pod": "p1"},
			},
		},
		RequestBodyKey: "bodies/x.req", RequestBodySize: 4,
	}
	if err := m.Insert(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := m.Insert(ctx, rec); err != ErrDuplicate {
		t.Fatalf("second insert err = %v, want ErrDuplicate", err)
	}
	got, err := m.Get(ctx, "m1")
	if err != nil || got.RequestBodyKey != "bodies/x.req" || got.Exchange.GetSummary().GetPath() != "/Cart/Items" {
		t.Fatalf("get = %+v, %v", got, err)
	}
	list, total, err := m.List(ctx, &tracev1.ListTracesRequest{Protocol: "http", MinStatus: 500, Text: "cart/ITEMS", Labels: map[string]string{"k8s.pod": "p1"}})
	if err != nil || total != 1 || len(list) != 1 {
		t.Fatalf("list = %v total=%d err=%v", list, total, err)
	}
	if _, total, _ := m.List(ctx, &tracev1.ListTracesRequest{Text: "nope"}); total != 0 {
		t.Fatalf("non-matching text returned %d", total)
	}
}

func TestMinioBodiesIntegration(t *testing.T) {
	ep := os.Getenv("MINIO_ENDPOINT")
	if ep == "" {
		t.Skip("MINIO_ENDPOINT not set")
	}
	ctx := context.Background()
	b, err := NewMinioBodies(ctx, MinioConfig{
		Endpoint: ep, AccessKey: os.Getenv("MINIO_ACCESS_KEY"), SecretKey: os.Getenv("MINIO_SECRET_KEY"),
		Bucket: "hackergate-test", Retention: 24 * time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 300_000)
	key := "bodies/2025/01/01/it.req"
	if err := b.Put(ctx, key, bytes.NewReader(data), int64(len(data))); err != nil {
		t.Fatal(err)
	}
	rc, err := b.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(rc)
	_ = rc.Close()
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatal("round trip mismatch")
	}
	if err := b.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, key); err == nil {
		t.Fatal("deleted object still readable")
	}
}
