package socket

import (
	"io"
	"net"
	"testing"
	"time"
)

func newSockets(t *testing.T) (*Socket, *Socket) {
	t.Helper()
	dir := t.TempDir()
	r, err := New(dir, 'r')
	if err != nil {
		t.Fatalf("New r: %v", err)
	}
	w, err := New(dir, 'w')
	if err != nil {
		t.Fatalf("New w: %v", err)
	}
	t.Cleanup(func() { r.Listener.Close(); w.Listener.Close() })
	return r, w
}

func TestNewRejectsAnUnknownMode(t *testing.T) {
	if _, err := New(t.TempDir(), 'x'); err == nil {
		t.Error("expected an error, got none")
	}
}

func TestAcceptConnsReturnsBothConnections(t *testing.T) {
	r, w := newSockets(t)
	for _, s := range []*Socket{r, w} {
		go func(path string) {
			if c, err := net.Dial("unix", path); err == nil {
				t.Cleanup(func() { c.Close() })
			}
		}(s.Path)
	}

	gotR, gotW, e1, e2 := AcceptConns(r, w, 5)

	if e1 != nil || e2 != nil {
		t.Fatalf("errors: %v, %v", e1, e2)
	}
	if gotR.Connection == nil || gotW.Connection == nil {
		t.Error("a connection is missing")
	}
}

func TestAcceptConnsClosesTheConnectionThatSucceededWhenTheOtherTimesOut(t *testing.T) {
	r, w := newSockets(t)
	client, err := net.Dial("unix", r.Path)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	gotR, gotW, e1, e2 := AcceptConns(r, w, 1)

	if gotR != nil || gotW != nil {
		t.Error("no sockets should be returned on failure")
	}
	if e1 != nil || e2 == nil {
		t.Fatalf("errors = %v, %v; want only the second to fail", e1, e2)
	}
	client.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := client.Read(make([]byte, 1)); err != io.EOF {
		t.Errorf("read error = %v, want EOF because the server closed the connection", err)
	}
}

func TestAcceptConnsFailsWhenNobodyConnects(t *testing.T) {
	r, w := newSockets(t)

	_, _, e1, e2 := AcceptConns(r, w, 1)

	if e1 == nil || e2 == nil {
		t.Errorf("errors = %v, %v; want both", e1, e2)
	}
}
