package intercept

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"go.pilab.hu/cloud/pitrace/trace"
)

func (s *Server) serveTCP(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		s.mu.Lock()
		s.conns[c] = struct{}{}
		s.mu.Unlock()
		s.wg.Add(1)
		go s.relay(c)
	}
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// relay pumps one client connection to the upstream and records the stream.
func (s *Server) relay(client net.Conn) {
	defer s.wg.Done()
	defer s.untrack(client)
	defer client.Close()

	started := time.Now()
	rec := trace.StreamRecord{
		Protocol:   s.cfg.ProtocolName,
		ClientAddr: hostOf(client.RemoteAddr()),
		Started:    started,
		Labels:     map[string]string{"upstream": s.cfg.Upstream},
	}

	if tc, ok := client.(*tls.Conn); ok {
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HandshakeTimeout)
		err := tc.HandshakeContext(ctx)
		cancel()
		if err != nil {
			s.log.Debug().Err(err).Str("client", client.RemoteAddr().String()).Msg("client handshake failed")
			rec.Error = "client handshake: " + err.Error()
			rec.Duration = time.Since(started)
			s.rec.RecordStream(rec)
			return
		}
		st := tc.ConnectionState()
		rec.Labels["tls.version"] = tlsVersionName(st.Version)
		rec.Labels["tls.sni"] = st.ServerName
		rec.Labels["tls.alpn"] = st.NegotiatedProtocol
	}

	up, err := s.dialUpstream()
	if err != nil {
		s.log.Warn().Err(err).Str("upstream", s.cfg.Upstream).Msg("upstream dial failed")
		rec.Error = "upstream dial: " + err.Error()
		rec.Duration = time.Since(started)
		s.rec.RecordStream(rec)
		return
	}
	s.mu.Lock()
	s.conns[up] = struct{}{}
	s.mu.Unlock()
	defer s.untrack(up)
	defer up.Close()

	capMax := int64(0)
	if s.cfg.CaptureStreamPayload {
		capMax = s.rec.Config().MaxRequestBytes
	}
	in := &boundedBuf{max: capMax}
	outMax := int64(0)
	if s.cfg.CaptureStreamPayload {
		outMax = s.rec.Config().MaxResponseBytes
	}
	out := &boundedBuf{max: outMax}

	var cc, uc net.Conn = client, up
	if s.cfg.IdleTimeout > 0 {
		cc, uc = &idleConn{Conn: client, d: s.cfg.IdleTimeout}, &idleConn{Conn: up, d: s.cfg.IdleTimeout}
	}

	errc := make(chan error, 2)
	pump := func(dst, src net.Conn, tee *boundedBuf) {
		_, err := io.Copy(dst, io.TeeReader(src, tee))
		if cw, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = cw.CloseWrite()
		}
		errc <- err
	}
	go pump(uc, cc, in)
	go pump(cc, uc, out)

	// A clean EOF on one side is a half-close: keep relaying the other
	// direction. Any error tears the whole connection down.
	var relayErr error
	first := <-errc
	if first != nil {
		relayErr = first
		_ = client.Close()
		_ = up.Close()
	}
	if second := <-errc; relayErr == nil && second != nil {
		relayErr = second
	}

	rec.Duration = time.Since(started)
	rec.BytesIn, rec.BytesOut = in.total, out.total
	if s.cfg.CaptureStreamPayload {
		rec.ClientToServer, rec.ServerToClient = in.buf, out.buf
		rec.Truncated = in.truncated || out.truncated
	}
	if relayErr != nil && !isClosedErr(relayErr) {
		rec.Error = relayErr.Error()
	}
	s.rec.RecordStream(rec)
}

func (s *Server) dialUpstream() (net.Conn, error) {
	d := &net.Dialer{Timeout: s.cfg.DialTimeout, KeepAlive: 30 * time.Second}
	raw, err := d.Dial("tcp", s.cfg.Upstream)
	if err != nil {
		return nil, err
	}
	if !s.cfg.UpstreamTLS {
		return raw, nil
	}
	cfg := s.upTLS.Clone()
	if cfg.ServerName == "" {
		host, _, err := net.SplitHostPort(s.cfg.Upstream)
		if err != nil {
			host = s.cfg.Upstream
		}
		cfg.ServerName = host
	}
	tc := tls.Client(raw, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.HandshakeTimeout)
	defer cancel()
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tc, nil
}

func hostOf(a net.Addr) string {
	if a == nil {
		return ""
	}
	if h, _, err := net.SplitHostPort(a.String()); err == nil {
		return h
	}
	return a.String()
}

func isClosedErr(err error) bool {
	return errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) ||
		strings.Contains(err.Error(), "use of closed network connection")
}

// boundedBuf counts every byte written and keeps the first max of them.
type boundedBuf struct {
	max       int64
	buf       []byte
	total     int64
	truncated bool
}

func (b *boundedBuf) Write(p []byte) (int, error) {
	b.total += int64(len(p))
	if room := b.max - int64(len(b.buf)); room > 0 {
		take := int64(len(p))
		if take > room {
			take = room
			b.truncated = true
		}
		b.buf = append(b.buf, p[:take]...)
	} else if b.max > 0 {
		b.truncated = true
	}
	return len(p), nil
}

// idleConn refreshes a deadline on every read and write.
type idleConn struct {
	net.Conn
	d time.Duration
}

func (c *idleConn) Read(p []byte) (int, error) {
	_ = c.Conn.SetDeadline(time.Now().Add(c.d))
	return c.Conn.Read(p)
}

func (c *idleConn) Write(p []byte) (int, error) {
	_ = c.Conn.SetDeadline(time.Now().Add(c.d))
	return c.Conn.Write(p)
}

func (c *idleConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return nil
}
