// Command hackergate is the operator CLI for the hackergate trace pipeline. It
// talks to a collector over gRPC to list traces, show one exchange, dump its
// bodies and print a replayable curl command.
//
//	hackergate -collector collector.hackergate:9470 list -since 1h -status 5xx
//	hackergate get <trace-id>
//	hackergate body <trace-id> -kind resp
//	hackergate curl <trace-id>
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.pilab.hu/cloud/pitrace/curl"
	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

const usage = `usage: hackergate [global flags] <command> [flags]

commands:
  list          list traces (filter by time, protocol, status, labels, ...)
  get <id>      show one exchange (headers; -bodies adds the bodies)
  body <id>     write one stored body to stdout (-kind req|resp)
  curl <id>     print a curl command that replays the request

global flags:
  -collector    collector address (env HACKERGATE_COLLECTOR)
  -insecure     plaintext gRPC (in-cluster dev only)
  -ca-file      CA bundle for verifying the collector
  -timeout      per-command timeout (default 30s)
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

type clients struct {
	traces tracev1.TraceServiceClient
	bodies tracev1.BodyServiceClient
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, extra ...grpc.DialOption) int {
	if err := execute(ctx, args, stdout, stderr, extra); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		fmt.Fprintln(stderr, "hackergate:", friendly(err))
		return 1
	}
	return 0
}

func friendly(err error) string {
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.NotFound:
			return "trace not found (expired or wrong id)"
		case codes.Unavailable:
			return "collector unavailable: " + st.Message()
		}
	}
	return err.Error()
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer, extra []grpc.DialOption) error {
	fs := flag.NewFlagSet("hackergate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() { fmt.Fprint(stderr, usage) }
	endpoint := fs.String("collector", os.Getenv("HACKERGATE_COLLECTOR"), "collector address")
	insecureTLS := fs.Bool("insecure", false, "plaintext gRPC")
	caFile := fs.String("ca-file", "", "CA bundle")
	timeout := fs.Duration("timeout", 30*time.Second, "per-command timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		fs.Usage()
		return errors.New("missing command")
	}
	cmd, cmdArgs := rest[0], rest[1:]
	if cmd == "help" {
		fs.Usage()
		return nil
	}
	if cmd != "list" && cmd != "get" && cmd != "body" && cmd != "curl" {
		fs.Usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
	if *endpoint == "" {
		return errors.New("no collector: pass -collector or set HACKERGATE_COLLECTOR")
	}

	creds, err := transportCreds(*insecureTLS, *caFile)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(*endpoint, append([]grpc.DialOption{grpc.WithTransportCredentials(creds)}, extra...)...)
	if err != nil {
		return err
	}
	defer conn.Close()
	c := &clients{traces: tracev1.NewTraceServiceClient(conn), bodies: tracev1.NewBodyServiceClient(conn)}

	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	cfs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	cfs.SetOutput(stderr)
	switch cmd {
	case "list":
		return cmdList(ctx, c, cfs, cmdArgs, stdout)
	case "get":
		return cmdGet(ctx, c, cfs, cmdArgs, stdout)
	case "body":
		return cmdBody(ctx, c, cfs, cmdArgs, stdout)
	default:
		return cmdCurl(ctx, c, cfs, cmdArgs, stdout)
	}
}

func transportCreds(insecureTLS bool, caFile string) (credentials.TransportCredentials, error) {
	if insecureTLS {
		return insecure.NewCredentials(), nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, errors.New("ca-file: no certificates found")
		}
		tc.RootCAs = pool
	}
	return credentials.NewTLS(tc), nil
}

type labelFlags map[string]string

func (l labelFlags) String() string { return fmt.Sprint(map[string]string(l)) }
func (l labelFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q must be key=value", v)
	}
	l[k] = val
	return nil
}

// parseStatus accepts "500", "5xx" or "400-499".
func parseStatus(s string) (lo, hi int32, err error) {
	if s == "" {
		return 0, 0, nil
	}
	bad := fmt.Errorf("invalid status %q (use 500, 5xx or 400-499)", s)
	if len(s) == 3 && strings.HasSuffix(strings.ToLower(s), "xx") {
		d, e := strconv.Atoi(s[:1])
		if e != nil {
			return 0, 0, bad
		}
		return int32(d * 100), int32(d*100 + 99), nil
	}
	if a, b, ok := strings.Cut(s, "-"); ok {
		x, e1 := strconv.Atoi(a)
		y, e2 := strconv.Atoi(b)
		if e1 != nil || e2 != nil || x > y {
			return 0, 0, bad
		}
		return int32(x), int32(y), nil
	}
	n, e := strconv.Atoi(s)
	if e != nil {
		return 0, 0, bad
	}
	return int32(n), int32(n), nil
}

func parseTime(s string, now time.Time) (*timestamppb.Timestamp, error) {
	if s == "" {
		return nil, nil
	}
	if d, err := time.ParseDuration(s); err == nil {
		return timestamppb.New(now.Add(-d)), nil
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return timestamppb.New(t), nil
		}
	}
	return nil, fmt.Errorf("invalid time %q (use a duration like 1h or an RFC3339 timestamp)", s)
}

func cmdList(ctx context.Context, c *clients, fs *flag.FlagSet, args []string, out io.Writer) error {
	labels := labelFlags{}
	var (
		since   = fs.String("since", "", "start: duration ago (1h) or RFC3339 time")
		until   = fs.String("until", "", "end: duration ago or RFC3339 time")
		proto   = fs.String("protocol", "", "protocol")
		account = fs.String("account", "", "account id")
		op      = fs.String("operation", "", "operation")
		stat    = fs.String("status", "", "status filter: 500, 5xx or 400-499")
		session = fs.String("session", "", "session id")
		text    = fs.String("text", "", "text search (path, operation, user agent, codes)")
		limit   = fs.Uint("limit", 50, "max results (1000 cap)")
		offset  = fs.Uint("offset", 0, "skip results")
		format  = fs.String("o", "table", "output: table or json")
	)
	fs.Var(labels, "label", "label filter key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	now := time.Now()
	st, err := parseTime(*since, now)
	if err != nil {
		return err
	}
	un, err := parseTime(*until, now)
	if err != nil {
		return err
	}
	lo, hi, err := parseStatus(*stat)
	if err != nil {
		return err
	}
	resp, err := c.traces.ListTraces(ctx, &tracev1.ListTracesRequest{
		Since: st, Until: un, Protocol: *proto, AccountId: *account, Operation: *op,
		MinStatus: lo, MaxStatus: hi, SessionId: *session, Text: *text,
		Limit: uint32(*limit), Offset: uint32(*offset), Labels: labels,
	})
	if err != nil {
		return err
	}
	if *format == "json" {
		return writeJSON(out, resp)
	}
	if *format != "table" {
		return fmt.Errorf("unknown output %q (table or json)", *format)
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "TRACE ID\tSTARTED\tMS\tSTATUS\tMETHOD\tPATH\tPROTOCOL\tOPERATION")
	for _, s := range resp.GetTraces() {
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%s\t%s\t%s\t%s\n", s.GetTraceId(),
			s.GetStartedAt().AsTime().Local().Format("01-02 15:04:05"), s.GetDurationMs(),
			s.GetStatus(), s.GetMethod(), s.GetPath(), s.GetProtocol(), s.GetOperation())
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if t := int(resp.GetTotal()); t > len(resp.GetTraces()) {
		fmt.Fprintf(out, "showing %d of %d (use -limit/-offset)\n", len(resp.GetTraces()), t)
	}
	return nil
}

func writeJSON(out io.Writer, m proto.Message) error {
	b, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(m)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, string(b))
	return err
}

func oneID(fs *flag.FlagSet, args []string) (string, error) {
	// Accept flags before or after the id.
	var id string
	var flags []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") {
			flags = append(flags, a)
			// A non-boolean flag takes the next arg as its value.
			if !strings.Contains(a, "=") {
				if f := fs.Lookup(strings.TrimLeft(a, "-")); f != nil {
					if bf, ok := f.Value.(interface{ IsBoolFlag() bool }); !ok || !bf.IsBoolFlag() {
						if i+1 < len(args) {
							i++
							flags = append(flags, args[i])
						}
					}
				}
			}
			continue
		}
		if id != "" {
			return "", errors.New("expected exactly one trace id")
		}
		id = a
	}
	if err := fs.Parse(flags); err != nil {
		return "", err
	}
	if id == "" {
		return "", errors.New("missing trace id")
	}
	return id, nil
}

func fetchMeta(ctx context.Context, c *clients, id string) (*tracev1.HTTPExchange, error) {
	no := false
	resp, err := c.traces.GetTrace(ctx, &tracev1.GetTraceRequest{TraceId: id, IncludeBodies: &no})
	if err != nil {
		return nil, err
	}
	return resp.GetTrace(), nil
}

func fetchBody(ctx context.Context, c *clients, id string, kind tracev1.BodyKind, gzipped bool, raw bool) ([]byte, error) {
	st, err := c.bodies.GetBody(ctx, &tracev1.GetBodyRequest{TraceId: id, Kind: kind})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	for {
		chunk, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		buf.Write(chunk.GetData())
	}
	if gzipped && !raw {
		zr, err := gzip.NewReader(&buf)
		if err != nil {
			return nil, fmt.Errorf("decompress body: %w", err)
		}
		defer zr.Close()
		return io.ReadAll(zr)
	}
	return buf.Bytes(), nil
}

func cmdGet(ctx context.Context, c *clients, fs *flag.FlagSet, args []string, out io.Writer) error {
	bodies := fs.Bool("bodies", false, "include request and response bodies")
	format := fs.String("o", "text", "output: text or json")
	id, err := oneID(fs, args)
	if err != nil {
		return err
	}
	ex, err := fetchMeta(ctx, c, id)
	if err != nil {
		return err
	}
	var reqBody, respBody []byte
	if *bodies {
		if reqBody, err = fetchBody(ctx, c, id, tracev1.BodyKind_BODY_KIND_REQUEST, ex.GetRequestBodyGzipped(), false); err != nil {
			return err
		}
		if respBody, err = fetchBody(ctx, c, id, tracev1.BodyKind_BODY_KIND_RESPONSE, ex.GetResponseBodyGzipped(), false); err != nil {
			return err
		}
		ex.RequestBody, ex.ResponseBody = reqBody, respBody
		ex.RequestBodyGzipped, ex.ResponseBodyGzipped = false, false
	}
	if *format == "json" {
		return writeJSON(out, ex)
	}
	if *format != "text" {
		return fmt.Errorf("unknown output %q (text or json)", *format)
	}
	s := ex.GetSummary()
	fmt.Fprintf(out, "Trace:     %s\n", s.GetTraceId())
	fmt.Fprintf(out, "Started:   %s (%d ms)\n", s.GetStartedAt().AsTime().Local().Format(time.RFC3339), s.GetDurationMs())
	fmt.Fprintf(out, "Request:   %s %s  [%s %s]\n", s.GetMethod(), s.GetPath(), s.GetProtocol(), s.GetOperation())
	fmt.Fprintf(out, "Status:    %d\n", s.GetStatus())
	fmt.Fprintf(out, "Client:    %s  %s\n", s.GetClientIp(), s.GetUserAgent())
	if s.GetAccountId() != "" {
		fmt.Fprintf(out, "Account:   %s (%s)\n", s.GetAccountId(), s.GetPrincipal())
	}
	if s.GetError() != "" {
		fmt.Fprintf(out, "Error:     %s\n", s.GetError())
	}
	for k, v := range s.GetLabels() {
		fmt.Fprintf(out, "Label:     %s=%s\n", k, v)
	}
	fmt.Fprintf(out, "Sizes:     request %d B, response %d B\n", s.GetRequestBytes(), s.GetResponseBytes())
	writeHeaders(out, "Request headers", ex.GetRequestHeaders())
	writeHeaders(out, "Response headers", ex.GetResponseHeaders())
	if *bodies {
		fmt.Fprintf(out, "\n--- request body ---\n%s\n--- response body ---\n%s\n", reqBody, respBody)
	}
	return nil
}

func writeHeaders(out io.Writer, title string, hs []*tracev1.Header) {
	if len(hs) == 0 {
		return
	}
	fmt.Fprintf(out, "\n%s:\n", title)
	for _, h := range hs {
		fmt.Fprintf(out, "  %s: %s\n", curl.HeaderName(h), h.GetValue())
	}
}

func cmdBody(ctx context.Context, c *clients, fs *flag.FlagSet, args []string, out io.Writer) error {
	kind := fs.String("kind", "resp", "which body: req or resp")
	raw := fs.Bool("raw", false, "do not decompress gzipped bodies")
	id, err := oneID(fs, args)
	if err != nil {
		return err
	}
	ex, err := fetchMeta(ctx, c, id)
	if err != nil {
		return err
	}
	var bk tracev1.BodyKind
	var gz bool
	switch *kind {
	case "req", "request":
		bk, gz = tracev1.BodyKind_BODY_KIND_REQUEST, ex.GetRequestBodyGzipped()
	case "resp", "response":
		bk, gz = tracev1.BodyKind_BODY_KIND_RESPONSE, ex.GetResponseBodyGzipped()
	default:
		return fmt.Errorf("unknown -kind %q (req or resp)", *kind)
	}
	b, err := fetchBody(ctx, c, id, bk, gz, *raw)
	if err != nil {
		return err
	}
	_, err = out.Write(b)
	return err
}

func cmdCurl(ctx context.Context, c *clients, fs *flag.FlagSet, args []string, out io.Writer) error {
	id, err := oneID(fs, args)
	if err != nil {
		return err
	}
	ex, err := fetchMeta(ctx, c, id)
	if err != nil {
		return err
	}
	if ex.GetSummary().GetRequestBytes() > 0 || ex.GetRequestBodyGzipped() {
		if ex.RequestBody, err = fetchBody(ctx, c, id, tracev1.BodyKind_BODY_KIND_REQUEST, ex.GetRequestBodyGzipped(), false); err != nil {
			return err
		}
		ex.RequestBodyGzipped = false
	}
	_, err = fmt.Fprintln(out, curl.ProtoToCurl(ex))
	return err
}
