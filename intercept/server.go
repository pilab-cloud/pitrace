package intercept

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"go.pilab.hu/cloud/pitrace/trace"
)

// Server is one running interceptor.
type Server struct {
	cfg   Config
	rec   *trace.Recorder
	log   zerolog.Logger
	upTLS *tls.Config

	cert atomic.Pointer[tls.Certificate]

	mu      sync.Mutex
	ln      net.Listener
	httpSrv *http.Server
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
	closed  bool

	def    *route
	routes map[string]*route
}

type route struct {
	target *url.URL
	proxy  *httputil.ReverseProxy
}

// New validates cfg and prepares a Server. Nothing is bound until Listen.
func New(cfg Config, rec *trace.Recorder, logger zerolog.Logger) (*Server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	up, err := upstreamTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:   cfg,
		rec:   rec,
		log:   logger,
		upTLS: up,
		conns: make(map[net.Conn]struct{}),
	}
	if err := s.Reload(); err != nil {
		return nil, err
	}
	if cfg.Mode == ModeHTTP {
		if err := s.buildRoutes(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Reload re-reads the certificate files. It is a no-op for self-signed and
// plain-text servers, and safe to call while serving (wire it to SIGHUP).
func (s *Server) Reload() error {
	switch {
	case s.cfg.CertFile != "":
		c, err := tls.LoadX509KeyPair(s.cfg.CertFile, s.cfg.KeyFile)
		if err != nil {
			return fmt.Errorf("intercept: load certificate: %w", err)
		}
		s.cert.Store(&c)
	case s.cfg.SelfSigned && s.cert.Load() == nil:
		c, err := selfSignedCert(s.cfg.SelfSignedHosts)
		if err != nil {
			return err
		}
		s.cert.Store(c)
	}
	return nil
}

// CertFingerprint is the SHA-256 of the served leaf certificate (hex).
func (s *Server) CertFingerprint() string { return certFingerprint(s.cert.Load()) }

// Addr is the bound address, valid after Listen.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) serverTLS() *tls.Config {
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return s.cert.Load(), nil
		},
	}
	if s.cfg.Mode == ModeHTTP {
		cfg.NextProtos = []string{"h2", "http/1.1"}
	}
	return cfg
}

// Listen binds the listen address.
func (s *Server) Listen() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return errors.New("intercept: already listening")
	}
	ln, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

// Serve accepts connections until Shutdown. It returns nil after a clean
// shutdown.
func (s *Server) Serve() error {
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln == nil {
		return errors.New("intercept: Listen not called")
	}
	s.log.Info().Str("mode", s.cfg.Mode).Str("listen", ln.Addr().String()).
		Str("upstream", s.cfg.Upstream).Bool("tls", !s.cfg.PlainText).
		Str("cert_sha256", s.CertFingerprint()).Msg("interceptor serving")

	if s.cfg.Mode == ModeTCP {
		if !s.cfg.PlainText {
			ln = tls.NewListener(ln, s.serverTLS())
		}
		return s.serveTCP(ln)
	}

	srv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: s.cfg.HandshakeTimeout,
		IdleTimeout:       2 * time.Minute,
		TLSConfig:         s.serverTLS(),
	}
	s.mu.Lock()
	s.httpSrv = srv
	s.mu.Unlock()

	var err error
	if s.cfg.PlainText {
		err = srv.Serve(ln)
	} else {
		err = srv.ServeTLS(ln, "", "")
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Run listens and serves until ctx is cancelled, then shuts down gracefully.
func (s *Server) Run(ctx context.Context) error {
	if err := s.Listen(); err != nil {
		return err
	}
	errc := make(chan error, 1)
	go func() { errc <- s.Serve() }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Shutdown(sctx)
		return <-errc
	}
}

// Shutdown stops accepting, drains HTTP requests and closes relay connections.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	srv, ln := s.httpSrv, s.ln
	s.mu.Unlock()

	var err error
	if srv != nil {
		err = srv.Shutdown(ctx)
	} else if ln != nil {
		_ = ln.Close()
	}
	s.mu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
	}
	return err
}

func (s *Server) buildRoutes() error {
	tr := &http.Transport{
		TLSClientConfig:     s.upTLS,
		ForceAttemptHTTP2:   true,
		DisableCompression:  true, // forward Content-Encoding untouched
		MaxIdleConns:        256,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: s.cfg.HandshakeTimeout,
		DialContext:         (&net.Dialer{Timeout: s.cfg.DialTimeout, KeepAlive: 30 * time.Second}).DialContext,
	}
	mk := func(raw string) (*route, error) {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		return &route{target: u, proxy: s.newProxy(u, tr)}, nil
	}
	var err error
	if s.cfg.Upstream != "" {
		if s.def, err = mk(s.cfg.Upstream); err != nil {
			return err
		}
	}
	s.routes = make(map[string]*route, len(s.cfg.Routes))
	for host, raw := range s.cfg.Routes {
		r, err := mk(raw)
		if err != nil {
			return err
		}
		s.routes[strings.ToLower(host)] = r
	}
	return nil
}

func (s *Server) newProxy(target *url.URL, tr http.RoundTripper) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport: tr,
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			if !s.cfg.RewriteHost {
				pr.Out.Host = pr.In.Host
			}
			pr.SetXForwarded()
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode == http.StatusSwitchingProtocols ||
				strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
				if info := trace.InfoFromRequest(resp.Request); info != nil {
					info.Streamed = true
				}
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if info := trace.InfoFromRequest(r); info != nil {
				info.Error = "upstream: " + err.Error()
			}
			s.log.Warn().Err(err).Str("upstream", target.Host).Msg("upstream request failed")
			http.Error(w, "bad gateway", http.StatusBadGateway)
		},
	}
}

func (s *Server) pick(host string) *route {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	if r, ok := s.routes[strings.ToLower(host)]; ok {
		return r
	}
	return s.def
}

func (s *Server) handler() http.Handler {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		info := trace.InfoFromRequest(r)
		rt := s.pick(r.Host)
		if info != nil {
			info.AddLabel("host", r.Host)
			if r.TLS != nil {
				info.AddLabel("tls.version", tlsVersionName(r.TLS.Version))
				info.AddLabel("tls.sni", r.TLS.ServerName)
				info.AddLabel("tls.alpn", r.TLS.NegotiatedProtocol)
			}
			if rt != nil {
				info.AddLabel("upstream", rt.target.Host)
			}
		}
		if rt == nil {
			if info != nil {
				info.Error = "no upstream for host " + r.Host
			}
			http.Error(w, "no upstream for host", http.StatusMisdirectedRequest)
			return
		}
		rt.proxy.ServeHTTP(w, r)
	})
	return s.rec.HTTPMiddleware(
		trace.WithoutTraceHeader(),
		trace.WithSkipPaths(s.cfg.SkipPaths...),
	)(inner)
}
