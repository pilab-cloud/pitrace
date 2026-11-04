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
	req := httptest.NewRequest("GET", "https://example.com/x", nil)
	ex := RequestToProto(req, nil, nil, nil)
	r2, err := ProtoToRequest(ex)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Method != "GET" {
		t.Fatalf("method %s", r2.Method)
	}
}
