// Package intercept is a recording TLS-terminating proxy. It accepts client
// connections, terminates TLS, forwards the traffic to another host and hands
// everything it sees to a trace.Recorder.
//
// Two modes are supported:
//
//   - "http" (default): a reverse proxy. Every request/response pair becomes a
//     normal trace record, with the same masking, caps and retention as the
//     middleware. HTTP/1.1, HTTP/2 and WebSocket/SSE pass through.
//   - "tcp": a byte-stream relay for any other protocol (SMTP over implicit
//     TLS, IMAPS, ...). One record is written per connection. Payload capture
//     is off unless explicitly enabled because text protocols carry
//     credentials the masker cannot recognise.
package intercept

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Mode values.
const (
	ModeHTTP = "http"
	ModeTCP  = "tcp"
)

// Config describes one interceptor.
type Config struct {
	// Mode is "http" (default) or "tcp".
	Mode string `mapstructure:"mode"`
	// Listen is the address clients connect to. Defaults to ":8443".
	Listen string `mapstructure:"listen"`

	// Upstream is where traffic is forwarded. http mode: a URL such as
	// https://backend:443. tcp mode: host:port.
	Upstream string `mapstructure:"upstream"`
	// Routes maps a request Host (without port) to an upstream URL, taking
	// precedence over Upstream. http mode only.
	Routes map[string]string `mapstructure:"routes"`

	// CertFile/KeyFile hold the server certificate presented to clients.
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	// SelfSigned generates an in-memory certificate for SelfSignedHosts
	// (default localhost, 127.0.0.1, ::1). For tests and lab use.
	SelfSigned      bool     `mapstructure:"self_signed"`
	SelfSignedHosts []string `mapstructure:"self_signed_hosts"`
	// PlainText serves without TLS. Must be requested explicitly.
	PlainText bool `mapstructure:"plain_text"`

	// UpstreamTLS makes tcp mode dial the upstream over TLS (http mode derives
	// it from the URL scheme).
	UpstreamTLS bool `mapstructure:"upstream_tls"`
	// UpstreamServerName overrides the name verified on the upstream
	// certificate.
	UpstreamServerName string `mapstructure:"upstream_server_name"`
	// UpstreamCAFile adds trusted roots for verifying the upstream.
	UpstreamCAFile string `mapstructure:"upstream_ca_file"`
	// UpstreamInsecureSkipVerify disables upstream certificate verification.
	UpstreamInsecureSkipVerify bool `mapstructure:"upstream_insecure_skip_verify"`

	// RewriteHost sends the upstream's host in the Host header instead of the
	// client's. http mode only.
	RewriteHost bool `mapstructure:"rewrite_host"`
	// SkipPaths are path prefixes that are proxied but never recorded.
	SkipPaths []string `mapstructure:"skip_paths"`

	// CaptureStreamPayload stores the relayed bytes in tcp mode; otherwise
	// only metadata and byte counts are recorded.
	CaptureStreamPayload bool `mapstructure:"capture_stream_payload"`
	// ProtocolName labels tcp records (default "tcp").
	ProtocolName string `mapstructure:"protocol_name"`

	HandshakeTimeout time.Duration `mapstructure:"handshake_timeout"`
	DialTimeout      time.Duration `mapstructure:"dial_timeout"`
	// IdleTimeout closes a tcp relay with no traffic. 0 disables it.
	IdleTimeout time.Duration `mapstructure:"idle_timeout"`
}

func (c Config) withDefaults() Config {
	if c.Mode == "" {
		c.Mode = ModeHTTP
	}
	c.Mode = strings.ToLower(c.Mode)
	if c.Listen == "" {
		c.Listen = ":8443"
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 10 * time.Second
	}
	if len(c.SelfSignedHosts) == 0 {
		c.SelfSignedHosts = []string{"localhost", "127.0.0.1", "::1"}
	}
	if c.ProtocolName == "" {
		c.ProtocolName = "tcp"
	}
	return c
}

func (c Config) validate() error {
	switch c.Mode {
	case ModeHTTP:
		if c.Upstream == "" && len(c.Routes) == 0 {
			return errors.New("intercept: upstream or routes required")
		}
		for _, raw := range append([]string{c.Upstream}, routeTargets(c.Routes)...) {
			if raw == "" {
				continue
			}
			u, err := url.Parse(raw)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return fmt.Errorf("intercept: invalid upstream URL %q", raw)
			}
		}
	case ModeTCP:
		if c.Upstream == "" {
			return errors.New("intercept: upstream host:port required in tcp mode")
		}
	default:
		return fmt.Errorf("intercept: unknown mode %q", c.Mode)
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return errors.New("intercept: cert_file and key_file must be set together")
	}
	haveCert := c.CertFile != "" || c.SelfSigned
	if !haveCert && !c.PlainText {
		return errors.New("intercept: set cert_file/key_file, self_signed, or plain_text")
	}
	if haveCert && c.PlainText {
		return errors.New("intercept: plain_text conflicts with a TLS certificate")
	}
	return nil
}

func routeTargets(routes map[string]string) []string {
	out := make([]string, 0, len(routes))
	for _, v := range routes {
		out = append(out, v)
	}
	return out
}
