// Command hackergate-sniffer is a TLS-terminating recording proxy: it accepts
// client connections, forwards them to another host and stores everything it
// sees with pitrace.
//
//	hackergate-sniffer -listen :8443 -cert tls.crt -key tls.key \
//	    -upstream https://backend.internal:443 -trace-dir /var/lib/pitrace
//
// With -collector the traffic is streamed to a hackergate collector instead of
// being written locally; the pod identity comes from the downward-API
// environment (HACKERGATE_NAMESPACE, _POD, _WORKLOAD, _NODE). Otherwise the
// stored traffic is readable through the loopback gRPC API (-grpc).
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"google.golang.org/grpc"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
	"go.pilab.hu/cloud/pitrace/intercept"
	"go.pilab.hu/cloud/pitrace/trace"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "hackergate-sniffer:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		icfg      intercept.Config
		routes    listFlag
		skipPaths listFlag
		hosts     listFlag

		traceDir      = flag.String("trace-dir", "/var/lib/pitrace", "directory for trace segments")
		grpcAddr      = flag.String("grpc", "127.0.0.1:9460", "loopback address of the trace read API (empty disables)")
		maxAge        = flag.Duration("max-age", 7*24*time.Hour, "retention by age")
		maxBytes      = flag.Int64("max-bytes", 1<<30, "retention by size")
		sample        = flag.Float64("sample", 1, "fraction of exchanges to record")
		redactUploads = flag.Bool("redact-uploads", false, "redact multipart/binary request bodies")
		redactDown    = flag.Bool("redact-downloads", false, "redact binary response bodies")
		debug         = flag.Bool("debug", false, "debug logging")

		maxRequest  = flag.Int64("max-request-body", -1, "reject requests larger than this many bytes (negative: unlimited, bodies stream through)")
		streamAbove = flag.Int64("stream-threshold", 256<<10, "stream opaque bodies larger than this to the collector instead of buffering them")

		collectorAddr     = flag.String("collector", os.Getenv("HACKERGATE_COLLECTOR"), "collector host:port; streams traffic there instead of the local trace dir")
		collectorInsecure = flag.Bool("collector-insecure", false, "connect to the collector without TLS")
		collectorCA       = flag.String("collector-ca", "", "CA bundle to verify the collector")
	)
	flag.StringVar(&icfg.Mode, "mode", "http", `"http" (reverse proxy) or "tcp" (stream relay)`)
	flag.StringVar(&icfg.Listen, "listen", ":8443", "listen address")
	flag.StringVar(&icfg.Upstream, "upstream", "", "upstream URL (http) or host:port (tcp)")
	flag.Var(&routes, "route", "host=upstream-url routing rule, repeatable (http mode)")
	flag.StringVar(&icfg.CertFile, "cert", "", "server certificate (PEM)")
	flag.StringVar(&icfg.KeyFile, "key", "", "server private key (PEM)")
	flag.BoolVar(&icfg.SelfSigned, "self-signed", false, "serve a generated self-signed certificate")
	flag.Var(&hosts, "self-signed-host", "name/IP for the self-signed certificate, repeatable")
	flag.BoolVar(&icfg.PlainText, "plain-text", false, "serve without TLS")
	flag.BoolVar(&icfg.UpstreamTLS, "upstream-tls", false, "tcp mode: dial the upstream over TLS")
	flag.StringVar(&icfg.UpstreamServerName, "upstream-server-name", "", "name to verify on the upstream certificate")
	flag.StringVar(&icfg.UpstreamCAFile, "upstream-ca", "", "extra CA bundle for the upstream")
	flag.BoolVar(&icfg.UpstreamInsecureSkipVerify, "upstream-insecure", false, "do not verify the upstream certificate")
	flag.BoolVar(&icfg.RewriteHost, "rewrite-host", false, "send the upstream host in the Host header")
	flag.Var(&skipPaths, "skip-path", "path prefix that is proxied but not recorded, repeatable")
	flag.BoolVar(&icfg.CaptureStreamPayload, "capture-stream-payload", false, "tcp mode: store relayed bytes (may contain credentials)")
	flag.StringVar(&icfg.ProtocolName, "protocol", "tcp", "tcp mode: protocol label")
	flag.DurationVar(&icfg.IdleTimeout, "idle-timeout", 0, "tcp mode: close idle relays after this long")
	flag.Parse()

	icfg.SelfSignedHosts = hosts
	icfg.SkipPaths = skipPaths
	if len(routes) > 0 {
		icfg.Routes = make(map[string]string, len(routes))
		for _, r := range routes {
			host, target, ok := strings.Cut(r, "=")
			if !ok || host == "" || target == "" {
				return fmt.Errorf("invalid -route %q, want host=url", r)
			}
			icfg.Routes[host] = target
		}
	}

	level := zerolog.InfoLevel
	if *debug {
		level = zerolog.DebugLevel
	}
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(level).With().Timestamp().Logger()

	builder := trace.NewBuilder().
		WithEnabled(true).
		WithDir(*traceDir)
	if *collectorAddr != "" {
		builder.WithRemoteStorage(*collectorAddr, *collectorInsecure, trace.Source{
			ID:        os.Getenv("HACKERGATE_POD"),
			Namespace: os.Getenv("HACKERGATE_NAMESPACE"),
			Pod:       os.Getenv("HACKERGATE_POD"),
			Workload:  os.Getenv("HACKERGATE_WORKLOAD"),
			Node:      os.Getenv("HACKERGATE_NODE"),
		})
		builder.WithCollectorCA(*collectorCA)
	}
	rec, err := builder.
		WithMaxRequestBody(*maxRequest).
		WithStreamThreshold(*streamAbove).
		WithMaxAge(*maxAge).
		WithMaxBytes(*maxBytes).
		WithSampleRatio(*sample).
		WithRedactUploads(*redactUploads).
		WithRedactDownloads(*redactDown).
		WithLogger(logger).
		Build()
	if err != nil {
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = rec.Close(ctx)
	}()

	srv, err := intercept.New(icfg, rec, logger)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *grpcAddr != "" {
		stopGRPC, err := serveGRPC(*grpcAddr, rec, logger)
		if err != nil {
			return err
		}
		defer stopGRPC()
	}

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for range hup {
			if err := srv.Reload(); err != nil {
				logger.Error().Err(err).Msg("certificate reload failed")
			} else {
				logger.Info().Msg("certificate reloaded")
			}
		}
	}()

	return srv.Run(ctx)
}

func serveGRPC(addr string, rec *trace.Recorder, logger zerolog.Logger) (func(), error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("invalid -grpc address: %w", err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, fmt.Errorf("-grpc must be a loopback address (traces contain request bodies), got %q", host)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	gs := grpc.NewServer()
	tracev1.RegisterTraceServiceServer(gs, trace.NewService(rec))
	go func() {
		if err := gs.Serve(ln); err != nil {
			logger.Error().Err(err).Msg("grpc serve")
		}
	}()
	logger.Info().Str("addr", addr).Msg("trace read API listening")
	return gs.GracefulStop, nil
}
