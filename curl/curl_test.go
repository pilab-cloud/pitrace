package curl

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

func TestProtoToCurlAndParse(t *testing.T) {
	req := httptest.NewRequest("POST", "https://example.com/path?q=1", strings.NewReader("hello"))
	req.Header.Set("Content-Type", "text/plain")
	ex := RequestToProto(req, nil, []byte("hello"), nil)
	cmd := ProtoToCurl(ex)
	if !strings.Contains(cmd, "curl") {
		t.Fatal("expected curl command")
	}
	parsed, err := ParseCurl(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Method != "POST" {
		t.Fatalf("method %s", parsed.Method)
	}
	if parsed.Header.Get("Content-Type") != "text/plain" {
		t.Fatalf("content-type %s", parsed.Header.Get("Content-Type"))
	}
	_ = http.MethodPost
	_ = tracev1.HTTPExchange{}
}

func TestProtoToRequest(t *testing.T) {
	req := httptest.NewRequest(httpMethodGet, "https://example.com/x", nil)
	ex := RequestToProto(req, nil, nil, nil)
	r2, err := ProtoToRequest(ex)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Method != httpMethodGet {
		t.Fatalf("method %s", r2.Method)
	}
}

func TestProtoToCurlUsesRecordedScheme(t *testing.T) {
	ex := &tracev1.HTTPExchange{
		Summary:        &tracev1.TraceSummary{Method: "GET", Path: "/x", Labels: map[string]string{"scheme": "http"}},
		RequestHeaders: []*tracev1.Header{{Name: "Host", Value: "svc"}},
	}
	if cmd := ProtoToCurl(ex); !strings.Contains(cmd, "'http://svc/x'") {
		t.Fatalf("want http scheme, got %s", cmd)
	}
	delete(ex.Summary.Labels, "scheme")
	if cmd := ProtoToCurl(ex); !strings.Contains(cmd, "'https://svc/x'") {
		t.Fatalf("want https default, got %s", cmd)
	}
}
