package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.pilab.hu/cloud/pitrace/collector"
	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/trace"
)

func cli(t *testing.T, args ...string) (string, int) {
	t.Helper()
	meta, bodies := collector.NewMemoryMeta(), collector.NewMemoryBodies()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	collector.Register(srv, meta, bodies, collector.NewIngest(meta, bodies, zerolog.Nop()))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	remote, err := trace.NewRemoteStorage(trace.Config{CollectorEndpoint: "passthrough:///bufnet", CollectorInsecure: true},
		zerolog.Nop(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close(context.Background()) })
	require.NoError(t, remote.Write(context.Background(), &tracev1.HTTPExchange{
		Summary: &tracev1.TraceSummary{TraceId: "t1", StartedAt: timestamppb.Now(), Method: "POST", Path: "/cart", Status: 500, RequestBytes: 7},
		RequestHeaders: []*tracev1.Header{
			{WellKnown: tracev1.WellKnownHeader_WELL_KNOWN_HEADER_CONTENT_TYPE, Value: "application/json"},
			{WellKnown: tracev1.WellKnownHeader_WELL_KNOWN_HEADER_HOST, Value: "cart.shop"},
		},
		RequestBody: []byte(`{"a":"it's"}`), ResponseBody: []byte("boom"),
	}))
	require.Eventually(t, func() bool {
		r, err := remote.List(context.Background(), &tracev1.ListTracesRequest{})
		return err == nil && r.GetTotal() == 1
	}, 5*time.Second, 10*time.Millisecond)

	var out, errb bytes.Buffer
	dial := grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) })
	code := run(context.Background(), append([]string{"-collector", "passthrough:///bufnet", "-insecure"}, args...), &out, &errb, dial)
	return out.String() + errb.String(), code
}

func TestList(t *testing.T) {
	out, code := cli(t, "list", "-status", "5xx")
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "t1")
	assert.Contains(t, out, "/cart")
	out, _ = cli(t, "list", "-status", "2xx")
	assert.NotContains(t, out, "t1")
}

func TestBodyAndCurl(t *testing.T) {
	out, code := cli(t, "body", "t1", "-kind", "resp")
	assert.Equal(t, 0, code)
	assert.Equal(t, "boom", out)

	out, code = cli(t, "curl", "t1")
	assert.Equal(t, 0, code, out)
	assert.Contains(t, out, "-H 'Content-Type: application/json'")
	assert.Contains(t, out, `'https://cart.shop/cart'`)
	assert.Contains(t, out, `'{"a":"it'\''s"}'`)
}

func TestGetAndErrors(t *testing.T) {
	out, code := cli(t, "get", "t1", "-bodies")
	assert.Equal(t, 0, code, out)
	assert.True(t, strings.Contains(out, "boom") && strings.Contains(out, "Host: cart.shop"))

	out, code = cli(t, "get", "nope")
	assert.Equal(t, 1, code)
	assert.Contains(t, out, "not found")
}

func TestParseStatus(t *testing.T) {
	lo, hi, err := parseStatus("400-499")
	assert.NoError(t, err)
	assert.Equal(t, [2]int32{400, 499}, [2]int32{lo, hi})
	_, _, err = parseStatus("abc")
	assert.Error(t, err)
}
