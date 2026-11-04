package trace

import (
	"strings"

	"github.com/labstack/echo/v4"
)

// TraceIDHeader is set on every recorded response. A user can quote it in a
// bug report and support can load the exact request/response pair.
const TraceIDHeader = "X-PiMail-Trace-Id"

const infoContextKey = "pimail.trace.info"

// Info is the protocol-neutral context a handler attaches to the exchange.
// The middleware creates it before the handler runs and reads it afterwards;
// handlers mutate it through the setters below.
type Info struct {
	Protocol      string
	Operation     string
	AccountID     string
	Principal     string
	ClientVersion string
	ResponseCodes []string
	Streamed      bool
	Labels        map[string]string
}

// InfoFrom returns the exchange info for c, creating it when the middleware is
// not installed (useful in unit tests).
func InfoFrom(c echo.Context) *Info {
	if v := c.Get(infoContextKey); v != nil {
		if info, ok := v.(*Info); ok {
			return info
		}
	}
	info := &Info{}
	c.Set(infoContextKey, info)
	return info
}

// SetProtocol names the protocol ("ews", "jmap", "smtp", ...).
func SetProtocol(c echo.Context, protocol string) { InfoFrom(c).Protocol = protocol }

// SetOperation names the operation inside the protocol (CreateItem, ...).
func SetOperation(c echo.Context, operation string) { InfoFrom(c).Operation = operation }

// SetAccount records the authenticated account and principal.
func SetAccount(c echo.Context, accountID, principal string) {
	info := InfoFrom(c)
	info.AccountID = accountID
	info.Principal = principal
}

// SetClientVersion records a protocol-supplied client version.
func SetClientVersion(c echo.Context, version string) { InfoFrom(c).ClientVersion = version }

// SetResponseCodes records the protocol result codes (EWS ResponseCode, ...).
func SetResponseCodes(c echo.Context, codes []string) {
	InfoFrom(c).ResponseCodes = dedupeStrings(codes)
}

// AddResponseCodes appends protocol result codes, keeping order and uniqueness.
func AddResponseCodes(c echo.Context, codes ...string) {
	info := InfoFrom(c)
	info.ResponseCodes = dedupeStrings(append(info.ResponseCodes, codes...))
}

// SetStreamed marks long-lived responses (SSE, EWS streaming events).
func SetStreamed(c echo.Context, streamed bool) { InfoFrom(c).Streamed = streamed }

// SetLabels replaces the labels attached to the exchange.
func SetLabels(c echo.Context, labels map[string]string) { InfoFrom(c).Labels = labels }

// AddLabel adds a single label.
func AddLabel(c echo.Context, key, value string) {
	info := InfoFrom(c)
	if info.Labels == nil {
		info.Labels = make(map[string]string)
	}
	info.Labels[key] = value
}

func dedupeStrings(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}
