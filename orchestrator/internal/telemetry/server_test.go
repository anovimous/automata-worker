package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"
)

func serve(h http.Handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestStatusIsTheSnapshotOfTheTracker(t *testing.T) {
	tr, _ := newTestTracker()
	tr.SetPhase(PhaseIdle, "")
	rec := serve(handler(tr), http.MethodGet, "/status")

	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("GET /status = %d %s, want 200 application/json", rec.Code, rec.Header().Get("Content-Type"))
	}
	want, err := json.Marshal(tr.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, rec.Body.Bytes(), want) {
		t.Errorf("body = %s\nwant   %s", rec.Body, want)
	}
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var va, vb any
	if err := json.Unmarshal(a, &va); err != nil {
		t.Fatalf("not JSON: %s", a)
	}
	if err := json.Unmarshal(b, &vb); err != nil {
		t.Fatalf("not JSON: %s", b)
	}
	return reflect.DeepEqual(va, vb)
}

func TestHealthzAnswersOkWithinTheBudget(t *testing.T) {
	tr, _ := newTestTracker()
	rec := serve(handler(tr), http.MethodGet, "/healthz")

	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Errorf("GET /healthz = %d %q, want 200 ok", rec.Code, rec.Body)
	}
}

func TestHealthzAnswersStuckPastTheBudget(t *testing.T) {
	tr, clock := newTestTracker()
	tr.SetPhase(PhasePreparing, "WORDLISTS")
	clock.Advance(1602 * time.Second)
	rec := serve(handler(tr), http.MethodGet, "/healthz")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /healthz = %d, want 503", rec.Code)
	}
	if want := `{"reason":"STUCK","phase":"PREPARING","progressAgeSeconds":1602}`; !sameJSON(t, rec.Body.Bytes(), []byte(want)) {
		t.Errorf("body = %s, want %s", rec.Body, want)
	}
}

func TestOnlyGetAndHeadAreServed(t *testing.T) {
	tr, _ := newTestTracker()
	h := handler(tr)
	tests := []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/status", 200},
		{http.MethodHead, "/status", 200},
		{http.MethodHead, "/healthz", 200},
		{http.MethodPost, "/status", 405},
		{http.MethodPut, "/healthz", 405},
		{http.MethodGet, "/metrics", 404},
		{http.MethodGet, "/", 404},
	}
	for _, tt := range tests {
		rec := serve(h, tt.method, tt.path)
		if rec.Code != tt.want {
			t.Errorf("%s %s = %d, want %d", tt.method, tt.path, rec.Code, tt.want)
		}
		if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
			t.Errorf("%s %s Cache-Control = %q, want no-store", tt.method, tt.path, cc)
		}
	}
}

func TestServerServesUntilShutdown(t *testing.T) {
	tr, _ := newTestTracker()
	s := NewServer("127.0.0.1:0", tr)
	if err := s.Start(); err != nil {
		t.Fatalf("Start returned an error: %v", err)
	}

	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(body, []byte("ok")) {
		t.Errorf("GET /healthz = %d %q, want 200 ok", resp.StatusCode, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown returned an error: %v", err)
	}
	if _, err := http.Get("http://" + s.Addr() + "/healthz"); err == nil {
		t.Error("the server still answers after Shutdown")
	}
}

func TestStartFailsOnAnAddressInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	tr, _ := newTestTracker()

	if err := NewServer(ln.Addr().String(), tr).Start(); err == nil {
		t.Error("Start returned no error for an address in use")
	}
}

func TestAddrBeforeStartIsTheConfiguredOne(t *testing.T) {
	tr, _ := newTestTracker()
	if got := NewServer("127.0.0.1:9464", tr).Addr(); got != "127.0.0.1:9464" {
		t.Errorf("Addr() = %q, want the configured address", got)
	}
}
