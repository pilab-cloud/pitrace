// Package curl converts between captured HTTP exchanges (tracev1.HTTPExchange)
// and replayable curl commands / *http.Request values.
//
// It is used by diagnostic tooling to turn a stored trace into a command that
// can be replayed against a service, and to ingest hand-written curl commands
// into the tracing model.
package curl
