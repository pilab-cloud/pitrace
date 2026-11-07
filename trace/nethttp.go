package trace

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type infoKey struct{}

// InfoFromContext returns the exchange info attached by HTTPMiddleware, or nil
// when the request is not being recorded.
func InfoFromContext(ctx context.Context) *Info {
	info, _ := ctx.Value(infoKey{}).(*Info)
	return info
}

// InfoFromRequest is shorthand for InfoFromContext(req.Context()).
func InfoFromRequest(req *http.Request) *Info { return InfoFromContext(req.Context()) }

// HTTPMiddleware is the net/http counterpart of Middleware: one record per
// request, bounded capture, never blocking the handler. Handlers annotate the
// exchange through InfoFromRequest.
func (r *Recorder) HTTPMiddleware(opts ...Option) func(http.Handler) http.Handler {
	if r == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	o := MiddlewareOptions{Now: time.Now}
	for _, opt := range opts {
		opt(&o)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.ClientIP == nil {
		o.ClientIP = remoteHost
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if !r.Enabled() || skipped(req.URL.Path, o.SkipPaths) {
				next.ServeHTTP(w, req)
				return
			}

			started := o.Now()
			info := &Info{}
			req = req.WithContext(context.WithValue(req.Context(), infoKey{}, info))

			traceID := uuid.NewString()
			reqCapture := r.newRequestCapture(w, req, traceID, started)
			if reqCapture != nil {
				req.Body = reqCapture
			}
			capture := r.newResponseCapture(w, traceID, started)
			if !o.SuppressTraceHeader {
				w.Header().Set(r.cfg.TraceIDHeader, traceID)
			}

			finish := func(err error) {
				r.finish(req, w.Header(), capture.statusCode(), o.ClientIP(req), info, traceID, started, o, reqCapture, capture, err)
			}
			defer func() {
				if p := recover(); p != nil {
					finish(fmt.Errorf("panic: %v", p))
					panic(p)
				}
			}()

			next.ServeHTTP(capture, req)
			finish(nil)
		})
	}
}

func remoteHost(req *http.Request) string {
	if host, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
		return host
	}
	return req.RemoteAddr
}
