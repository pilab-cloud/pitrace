package curl

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

const httpMethodGet = "GET"

// ProtoToCurl renders an HTTPExchange as a replayable curl command.
func ProtoToCurl(ex *tracev1.HTTPExchange) string {
	if ex == nil || ex.Summary == nil {
		return ""
	}
	s := ex.Summary
	var b strings.Builder
	b.WriteString("curl -i -X ")
	b.WriteString(s.GetMethod())
	b.WriteString(" '")
	b.WriteString(buildURL(s, ex))
	b.WriteString("'")
	for _, h := range ex.GetRequestHeaders() {
		b.WriteString(" \\\n  -H '")
		b.WriteString(h.GetName())
		b.WriteString(": ")
		b.WriteString(h.GetValue())
		b.WriteString("'")
	}
	if len(ex.GetRequestBody()) > 0 {
		body := ex.GetRequestBody()
		// Leave gzipped bodies as-is; the user can decompress if needed.
		b.WriteString(" \\\n  --data-binary '")
		b.WriteString(string(body))
		b.WriteString("'")
	}
	return b.String()
}

func buildURL(s *tracev1.TraceSummary, ex *tracev1.HTTPExchange) string {
	u := s.GetPath()
	if ex.GetQuery() != "" {
		u += "?" + ex.GetQuery()
	}
	// Host from headers
	host := ""
	for _, h := range ex.GetRequestHeaders() {
		if strings.EqualFold(h.GetName(), "Host") {
			host = h.GetValue()
		}
	}
	if host != "" {
		return "https://" + host + u
	}
	return u
}

// ProtoToRequest converts an HTTPExchange into an *http.Request.
func ProtoToRequest(ex *tracev1.HTTPExchange) (*http.Request, error) {
	if ex == nil || ex.Summary == nil {
		return nil, fmt.Errorf("nil exchange")
	}
	s := ex.Summary
	u, err := url.Parse(s.GetPath())
	if err != nil {
		return nil, err
	}
	if ex.GetQuery() != "" {
		u.RawQuery = ex.GetQuery()
	}
	host := ""
	for _, h := range ex.GetRequestHeaders() {
		if strings.EqualFold(h.GetName(), "Host") {
			host = h.GetValue()
		}
	}
	if host != "" {
		u.Host = host
		u.Scheme = "https"
	}
	req, err := http.NewRequest(s.GetMethod(), u.String(), bytes.NewReader(ex.GetRequestBody()))
	if err != nil {
		return nil, err
	}
	for _, h := range ex.GetRequestHeaders() {
		req.Header.Add(h.GetName(), h.GetValue())
	}
	if host != "" {
		req.Host = host
	}
	return req, nil
}

// RequestToProto converts an HTTP request/response pair into an HTTPExchange.
func RequestToProto(req *http.Request, resp *http.Response, reqBody, respBody []byte) *tracev1.HTTPExchange {
	if req == nil {
		return nil
	}
	summary := &tracev1.TraceSummary{
		StartedAt: nil,
		Protocol:  "http",
		Method:    req.Method,
		Path:      req.URL.Path,
	}
	if resp != nil {
		summary.Status = int32(resp.StatusCode)
	}
	ex := &tracev1.HTTPExchange{
		SchemaVersion: 1,
		Summary:       summary,
		Query:         req.URL.RawQuery,
		ContentType:   req.Header.Get("Content-Type"),
		RequestBody:   reqBody,
	}
	if resp != nil {
		ex.ResponseBody = respBody
		for k, v := range resp.Header {
			for _, val := range v {
				ex.ResponseHeaders = append(ex.ResponseHeaders, &tracev1.Header{Name: k, Value: val})
			}
		}
	}
	for k, v := range req.Header {
		for _, val := range v {
			ex.RequestHeaders = append(ex.RequestHeaders, &tracev1.Header{Name: k, Value: val})
		}
	}
	if req.URL.Host != "" {
		ex.RequestHeaders = append(ex.RequestHeaders, &tracev1.Header{Name: "Host", Value: req.URL.Host})
	}
	return ex
}

// ParseCurl does a best-effort parse of a curl command into an *http.Request.
func ParseCurl(cmd string) (*http.Request, error) {
	cmd = strings.TrimSpace(cmd)
	if !strings.HasPrefix(cmd, "curl") {
		return nil, fmt.Errorf("not a curl command")
	}
	method := httpMethodGet
	urlStr := ""
	headers := http.Header{}
	var body string
	parts := splitCurl(cmd)
	for i := 0; i < len(parts); i++ {
		p := parts[i]
		switch p {
		case "-X", "--request":
			i++
			if i < len(parts) {
				method = parts[i]
			}
		case "-H", "--header":
			i++
			if i < len(parts) {
				kv := parts[i]
				idx := strings.Index(kv, ":")
				if idx > 0 {
					headers.Add(strings.TrimSpace(kv[:idx]), strings.TrimSpace(kv[idx+1:]))
				}
			}
		case "-d", "--data", "--data-binary", "--data-raw":
			i++
			if i < len(parts) {
				body = parts[i]
				if method == httpMethodGet {
					method = "POST"
				}
			}
		default:
			if strings.HasPrefix(p, "http://") || strings.HasPrefix(p, "https://") {
				urlStr = p
			}
		}
	}
	if urlStr == "" {
		return nil, fmt.Errorf("no URL found")
	}
	req, err := http.NewRequest(method, urlStr, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		for _, val := range v {
			req.Header.Add(k, val)
		}
	}
	if body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	return req, nil
}

func splitCurl(cmd string) []string {
	var out []string
	var cur strings.Builder
	inSingle, inDouble := false, false
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		switch {
		case c == '\'' && !inDouble:
			inSingle = !inSingle
		case c == '"' && !inSingle:
			inDouble = !inDouble
		case (c == ' ' || c == '\t' || c == '\n') && !inSingle && !inDouble:
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		case c == '\\' && i+1 < len(cmd) && (cmd[i+1] == '\n'):
			i++ // skip line continuation
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}
