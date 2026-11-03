package telemetry

import (
	"regexp"
	"unicode/utf8"
)

// how many characters of an error string are kept
const maxSanitizedRunes = 300

// The rules of docs/telemetry-contract.md §3.4, in their order. The monitor applies the same ones again to what it
// shows, so a sanitized string must come out of them unchanged.
var sanitizeRules = []struct {
	re   *regexp.Regexp
	repl string
}{
	// scheme://user:pass@host
	{regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]+@`), `${1}***@`},
	// Authorization: Bearer x — the scheme stays readable, the credential after it goes
	{regexp.MustCompile(`(?i)((?:proxy-)?authorization["']?\s*[:=]\s*["']?(?:(?:bearer|basic|digest|token|negotiate)\s+)?)[^\s"',;]+`), `${1}***`},
	// Cookie: a=1; b=2 — every cookie of the header
	{regexp.MustCompile(`(?i)((?:set-)?cookie["']?\s*[:=]\s*)[^"'\n]+`), `${1}***`},
	// the other names, in headers, env style (TOKEN=x) and JSON ("password":"x")
	{regexp.MustCompile(`(?i)(x-api-key|x-auth-token|api[-_]?key|token|password|passwd|secret)(["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;'"}\]]+)`), `${1}${2}***`},
}

// Sanitize masks the credentials an error string may hold (the broker URL carries its password, the core's URL
// may) and cuts it to 300 characters, so that the status, which has no authentication, holds no secret.
func Sanitize(s string) string {
	for _, r := range sanitizeRules {
		s = r.re.ReplaceAllString(s, r.repl)
	}
	if utf8.RuneCountInString(s) <= maxSanitizedRunes {
		return s
	}
	return string([]rune(s)[:maxSanitizedRunes]) + "…"
}
