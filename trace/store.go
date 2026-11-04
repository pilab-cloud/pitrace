package trace

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	tracev1 "go.pilab.hu/cloud/pitrace/gen/trace/v1"
)

// ErrTraceNotFound is returned when no index entry matches a trace id.
var ErrTraceNotFound = errors.New("trace not found")

const maxRecordBytes = 512 << 20

type indexEntry struct {
	TraceID           string            `json:"trace_id"`
	StartedAt         time.Time         `json:"started_at"`
	DurationMS        int64             `json:"duration_ms"`
	Protocol          string            `json:"protocol,omitempty"`
	Operation         string            `json:"operation,omitempty"`
	AccountID         string            `json:"account_id,omitempty"`
	Principal         string            `json:"principal,omitempty"`
	Method            string            `json:"method,omitempty"`
	Path              string            `json:"path,omitempty"`
	Status            int32             `json:"status,omitempty"`
	RequestBytes      int64             `json:"request_bytes,omitempty"`
	ResponseBytes     int64             `json:"response_bytes,omitempty"`
	UserAgent         string            `json:"user_agent,omitempty"`
	ClientIP          string            `json:"client_ip,omitempty"`
	ResponseCodes     []string          `json:"response_codes,omitempty"`
	RequestTruncated  bool              `json:"request_truncated,omitempty"`
	ResponseTruncated bool              `json:"response_truncated,omitempty"`
	SessionID         string            `json:"session_id,omitempty"`
	SessionSeq        uint32            `json:"session_seq,omitempty"`
	Error             string            `json:"error,omitempty"`
	Streamed          bool              `json:"streamed,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	RequestRedacted   bool              `json:"request_redacted,omitempty"`
	ResponseRedacted  bool              `json:"response_redacted,omitempty"`
	Offset            int64             `json:"offset"`
	Length            int64             `json:"length"`
	File              string            `json:"file"`
}

func indexEntryFrom(ex *tracev1.HTTPExchange, file string, offset, length int64) *indexEntry {
	s := ex.GetSummary()
	e := &indexEntry{
		TraceID:           s.GetTraceId(),
		StartedAt:         s.GetStartedAt().AsTime(),
		DurationMS:        s.GetDurationMs(),
		Protocol:          s.GetProtocol(),
		Operation:         s.GetOperation(),
		AccountID:         s.GetAccountId(),
		Principal:         s.GetPrincipal(),
		Method:            s.GetMethod(),
		Path:              s.GetPath(),
		Status:            s.GetStatus(),
		RequestBytes:      s.GetRequestBytes(),
		ResponseBytes:     s.GetResponseBytes(),
		UserAgent:         s.GetUserAgent(),
		ClientIP:          s.GetClientIp(),
		ResponseCodes:     s.GetResponseCodes(),
		RequestTruncated:  s.GetRequestTruncated(),
		ResponseTruncated: s.GetResponseTruncated(),
		SessionID:         s.GetSessionId(),
		SessionSeq:        s.GetSessionSeq(),
		Error:             s.GetError(),
		Streamed:          s.GetStreamed(),
		Labels:            s.GetLabels(),
		RequestRedacted:   s.GetRequestRedacted(),
		ResponseRedacted:  s.GetResponseRedacted(),
		Offset:            offset,
		Length:            length,
		File:              file,
	}
	return e
}

func (e *indexEntry) summary() *tracev1.TraceSummary {
	return &tracev1.TraceSummary{
		TraceId:           e.TraceID,
		StartedAt:         timestamppb.New(e.StartedAt),
		DurationMs:        e.DurationMS,
		Protocol:          e.Protocol,
		Operation:         e.Operation,
		AccountId:         e.AccountID,
		Principal:         e.Principal,
		Method:            e.Method,
		Path:              e.Path,
		Status:            e.Status,
		RequestBytes:      e.RequestBytes,
		ResponseBytes:     e.ResponseBytes,
		UserAgent:         e.UserAgent,
		ClientIp:          e.ClientIP,
		ResponseCodes:     e.ResponseCodes,
		RequestTruncated:  e.RequestTruncated,
		ResponseTruncated: e.ResponseTruncated,
		SessionId:         e.SessionID,
		SessionSeq:        e.SessionSeq,
		Error:             e.Error,
		Streamed:          e.Streamed,
		Labels:            e.Labels,
		RequestRedacted:   e.RequestRedacted,
		ResponseRedacted:  e.ResponseRedacted,
	}
}

func (e *indexEntry) matches(req *tracev1.ListTracesRequest) bool {
	if req == nil {
		return true
	}
	if req.GetSince() != nil && e.StartedAt.Before(req.GetSince().AsTime()) {
		return false
	}
	if req.GetUntil() != nil && e.StartedAt.After(req.GetUntil().AsTime()) {
		return false
	}
	if p := req.GetProtocol(); p != "" && !strings.EqualFold(p, e.Protocol) {
		return false
	}
	if a := req.GetAccountId(); a != "" && !strings.EqualFold(a, e.AccountID) {
		return false
	}
	if op := req.GetOperation(); op != "" && !strings.EqualFold(op, e.Operation) {
		return false
	}
	if s := req.GetSessionId(); s != "" && s != e.SessionID {
		return false
	}
	if min := req.GetMinStatus(); min > 0 && e.Status < min {
		return false
	}
	if max := req.GetMaxStatus(); max > 0 && e.Status > max {
		return false
	}
	if text := strings.ToLower(strings.TrimSpace(req.GetText())); text != "" {
		haystack := strings.ToLower(strings.Join([]string{
			e.Path, e.Operation, e.UserAgent, strings.Join(e.ResponseCodes, " "),
		}, " "))
		if !strings.Contains(haystack, text) {
			return false
		}
	}
	// label filter: all provided key/value pairs must match
	for k, v := range req.GetLabels() {
		if e.Labels[k] != v {
			return false
		}
	}
	return true
}

// List returns index summaries, newest first.
func indexFiles(root string) []string {
	var files []string
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() || !strings.HasSuffix(path, ".index.jsonl") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	sort.Sort(sort.Reverse(sort.StringSlice(files)))
	return files
}

func readIndexFile(path string) ([]*indexEntry, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var entries []*indexEntry
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	for scanner.Scan() {
		entry := &indexEntry{}
		if err := json.Unmarshal(scanner.Bytes(), entry); err != nil {
			continue // a torn final line after a crash must not hide the rest
		}
		if entry.TraceID == "" || entry.File == "" {
			continue
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return entries, err
	}
	return entries, nil
}
