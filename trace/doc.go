// Package trace provides reusable request/response tracing for Go services.
//
// It records HTTP exchanges asynchronously to compressed, indexed segments
// (file or S3), exposes a loopback gRPC read API (Service), and ships an Echo
// middleware (Recorder.Middleware) that captures request and response bodies
// without blocking handlers.
//
// Typical usage:
//
//	rec, svc, err := trace.NewBuilder().
//	    WithDir("/var/lib/pitrace").
//	    WithRedactUploads(true).
//	    BuildService()
//	// mount svc on a gRPC server
//	// e.Use(rec.Middleware())
//
// Handlers attach protocol metadata through the Set* helpers (SetProtocol,
// SetOperation, SetAccount, ...) and labels through AddLabel. Stored records
// are queryable via Service.ListTraces / GetTrace / TailTraces and the
// Recorder's Go API.
package trace
