package telemetry

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSanitize(t *testing.T) {
	tests := []struct {
		name, in, want string
	}{
		// the examples of docs/telemetry-contract.md §3.4, word for word
		{"amqp url", "dial amqp://automata:s3cret@rabbitmq:5672/", "dial amqp://***@rabbitmq:5672/"},
		{"bearer", "Authorization: Bearer eyJhbGci.x.y", "Authorization: Bearer ***"},
		{"basic", "authorization=Basic dXNlcjpwYXNz rest", "authorization=Basic *** rest"},
		{"several cookies", "Cookie: session=abc; theme=dark", "Cookie: ***"},
		{"env style", "TOKEN=abc PASSWORD=hunter2", "TOKEN=*** PASSWORD=***"},
		{"json password", `{"user":"a","password":"x y"}`, `{"user":"a","password":***}`},

		{"http url with user info", `failed to poll central service for job 4188: Get "http://u:p@localhost:8080/api/external/jobs/http/poll/4188": connection refused`,
			`failed to poll central service for job 4188: Get "http://***@localhost:8080/api/external/jobs/http/poll/4188": connection refused`},
		{"url without user info", "Get http://rustfs:9000/job-results/42.result: EOF", "Get http://rustfs:9000/job-results/42.result: EOF"},
		{"no secret", "central service returned status 503 for job 7", "central service returned status 503 for job 7"},
		{"api key", "X-Api-Key: k-123, next", "X-Api-Key: ***, next"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Sanitize(tt.in); got != tt.want {
				t.Errorf("Sanitize(%q)\n got  %q\n want %q", tt.in, got, tt.want)
			}
		})
	}
}

// the monitor sanitizes again what it shows, so a sanitized string must not change
func TestSanitizeLeavesASanitizedStringAsItIs(t *testing.T) {
	for _, in := range []string{
		"dial amqp://automata:s3cret@rabbitmq:5672/ and Authorization: Bearer abc",
		`{"password":"x","token":"y"}`,
		"Cookie: a=1; b=2",
		strings.Repeat("é", 400),
	} {
		once := Sanitize(in)
		if twice := Sanitize(once); twice != once {
			t.Errorf("Sanitize(Sanitize(%q)) = %q, want %q", in, twice, once)
		}
	}
}

func TestSanitizeCutsAt300Characters(t *testing.T) {
	in := strings.Repeat("é", 299) + "€€€"
	got := Sanitize(in)

	if !utf8.ValidString(got) {
		t.Fatalf("the cut broke a multibyte character: %q", got)
	}
	if n := utf8.RuneCountInString(got); n != 301 || !strings.HasSuffix(got, "€…") {
		t.Errorf("Sanitize kept %d characters ending with %q, want 300 and …", n, got[len(got)-8:])
	}
}

func TestSanitizeKeepsAString300CharactersLong(t *testing.T) {
	in := strings.Repeat("a", 300)
	if got := Sanitize(in); got != in {
		t.Errorf("Sanitize cut a string of 300 characters: %d left", len(got))
	}
}
