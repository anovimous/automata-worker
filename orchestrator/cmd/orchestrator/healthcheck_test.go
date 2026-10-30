package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func healthzPort(t *testing.T, status int) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("the healthcheck asked for %s", r.URL.Path)
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	return port
}

func TestHealthcheck(t *testing.T) {
	client := &http.Client{Timeout: time.Second}
	ok := healthzPort(t, http.StatusOK)
	stuck := healthzPort(t, http.StatusServiceUnavailable)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := ln.Addr().String()
	ln.Close()

	tests := []struct {
		name, addr string
		want       int
	}{
		{"200", "127.0.0.1:" + ok, 0},
		{"503", "127.0.0.1:" + stuck, 1},
		{"connection refused", refused, 1},
		{"telemetry off", "off", 0},
		{"wildcard host", "0.0.0.0:" + ok, 0},
		{"empty host", ":" + ok, 0},
		{"invalid address", "9464", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := healthcheck(tt.addr, client); got != tt.want {
				t.Errorf("healthcheck(%q) = %d, want %d", tt.addr, got, tt.want)
			}
		})
	}
}
