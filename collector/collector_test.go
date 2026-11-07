package collector_test

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.pilab.hu/cloud/pitrace/collector"
	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

type harness struct {
	meta   *collector.MemoryMeta
	bodies *collector.MemoryBodies
	lis    *bufconn.Listener
	srv    *grpc.Server
	remote *trace.RemoteStorage
}

func (h *harness) serve() {
	h.srv = grpc.NewServer()
	collector.Register(h.srv, h.meta, h.bodies, collector.NewIngest(h.meta, h.bodies, zerolog.Nop(), collector.WithMaxBodyBytes(16<<20)))
	go func() { _ = h.srv.Serve(h.lis) }()
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{meta: collector.NewMemoryMeta(), bodies: collector.NewMemoryBodies(), lis: bufconn.Listen(1 << 20)}
	h.serve()
	cfg := trace.Config{
		CollectorEndpoint: "passthrough:///bufnet",
		CollectorInsecure: true,
		Source:            trace.Source{ID: "sidecar-1", Namespace: "shop", Pod: "cart-7d9", Workload: "cart"},
	}
	remote, err := trace.NewRemoteStorage(cfg, zerolog.Nop(), grpc.WithContextDialer(
		func(ctx context.Context, _ string) (net.Conn, error) { return h.lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	h.remote = remote
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = remote.Close(ctx)
		h.srv.Stop()
	})
	return h
}

func exchange(id string, status int32, req, resp []byte) *tracev1.HTTPExchange {
	return &tracev1.HTTPExchange{
		Summary: &tracev1.TraceSummary{
			TraceId:   id,
			StartedAt: timestamppb.Now(),
			Protocol:  "HTTP",
			Method:    "POST",
			Path:      "/cart/items",
			Status:    status,
			Labels:    map[string]string{"host": "cart.shop"},
		},
		RequestHeaders: []*tracev1.Header{{WellKnown: tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_TYPE, Value: "application/json"}},
		RequestBody:    req,
		ResponseBody:   resp,
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPushStoresBodiesAndMetadata(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	big := bytes.Repeat([]byte("0123456789abcdef"), 20000) // 320 KB: several chunks

	if err := h.remote.Write(ctx, exchange("t1", 201, big, []byte(`{"ok":true}`))); err != nil {
		t.Fatal(err)
	}
	if err := h.remote.Write(ctx, exchange("t2", 500, nil, nil)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "both exchanges stored", func() bool {
		resp, err := h.remote.List(ctx, &tracev1.ListTracesRequest{})
		return err == nil && resp.GetTotal() == 2
	})

	got, err := h.remote.Read(ctx, "t1", true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetRequestBody(), big) || string(got.GetResponseBody()) != `{"ok":true}` {
		t.Fatal("bodies did not round-trip")
	}
	lbl := got.GetSummary().GetLabels()
	if lbl["k8s.namespace"] != "shop" || lbl["k8s.pod"] != "cart-7d9" || lbl["host"] != "cart.shop" {
		t.Fatalf("labels = %v", lbl)
	}
	if got.GetRequestHeaders()[0].GetValue() != "application/json" {
		t.Fatal("headers lost")
	}

	noBody, err := h.remote.Read(ctx, "t1", false)
	if err != nil || len(noBody.GetRequestBody()) != 0 {
		t.Fatalf("include_bodies=false returned bodies (err %v)", err)
	}
	if _, err := h.remote.Read(ctx, "missing", false); err != trace.ErrTraceNotFound {
		t.Fatalf("missing trace err = %v", err)
	}

	errs, err := h.remote.List(ctx, &tracev1.ListTracesRequest{MinStatus: 500})
	if err != nil || errs.GetTotal() != 1 || errs.GetTraces()[0].GetTraceId() != "t2" {
		t.Fatalf("status filter: %v %v", errs, err)
	}
	byLabel, _ := h.remote.List(ctx, &tracev1.ListTracesRequest{Labels: map[string]string{"k8s.pod": "cart-7d9"}})
	if byLabel.GetTotal() != 2 {
		t.Fatalf("label filter total = %d", byLabel.GetTotal())
	}
}

func TestBodiesLargerThanGRPCMessageLimit(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	big := bytes.Repeat([]byte("x"), 6<<20) // above the 4 MiB per-message default

	if err := h.remote.Write(ctx, exchange("big", 200, big, big)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "big exchange stored", func() bool { _, err := h.remote.Read(ctx, "big", false); return err == nil })
	got, err := h.remote.Read(ctx, "big", true)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.GetRequestBody(), big) || !bytes.Equal(got.GetResponseBody(), big) {
		t.Fatal("large bodies did not round-trip")
	}
}

func TestResendAfterCollectorRestartIsIdempotent(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.remote.Write(ctx, exchange("a", 200, []byte("one"), []byte("1"))); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "first stored", func() bool { _, err := h.remote.Read(ctx, "a", false); return err == nil })

	// Kill the collector, write while it is down, bring it back on the same
	// stores (as a restarted replica would see them).
	h.srv.Stop()
	if err := h.remote.Write(ctx, exchange("b", 200, []byte("two"), []byte("2"))); err != nil {
		t.Fatal(err)
	}
	h.lis = bufconn.Listen(1 << 20)
	h.serve()
	waitFor(t, "buffered exchange delivered after reconnect", func() bool {
		got, err := h.remote.Read(ctx, "b", true)
		return err == nil && string(got.GetRequestBody()) == "two"
	})
	resp, _ := h.remote.List(ctx, &tracev1.ListTracesRequest{})
	if resp.GetTotal() != 2 || h.bodies.Len() != 4 {
		t.Fatalf("total=%d objects=%d, want 2 and 4", resp.GetTotal(), h.bodies.Len())
	}
}

func TestIngestRejectsInlineBodiesAndOversize(t *testing.T) {
	meta, bodies := collector.NewMemoryMeta(), collector.NewMemoryBodies()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	collector.Register(srv, meta, bodies, collector.NewIngest(meta, bodies, zerolog.Nop(), collector.WithMaxBodyBytes(10)))
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///x", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cl := tracev1.NewIngestServiceClient(conn)

	try := func(b *tracev1.ExchangeBegin) error {
		st, err := cl.Push(context.Background())
		if err != nil {
			return err
		}
		if err := st.Send(&tracev1.PushRequest{Msg: &tracev1.PushRequest_Begin{Begin: b}}); err != nil {
			return err
		}
		_, err = st.Recv()
		return err
	}
	inline := exchange("x", 200, []byte("inline"), nil)
	if err := try(&tracev1.ExchangeBegin{Exchange: inline}); err == nil {
		t.Fatal("inline body accepted")
	}
	if err := try(&tracev1.ExchangeBegin{Exchange: exchange("y", 200, nil, nil), RequestBodySize: 11}); err == nil {
		t.Fatal("oversize body accepted")
	}
	if bodies.Len() != 0 {
		t.Fatal("rejected exchanges left objects behind")
	}
}
