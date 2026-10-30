package main

import (
	"fmt"
	"net"
	"net/http"

	configP "github.com/anovimous/automata-worker/orchestrator/internal/config"
)

// healthcheck is `orchestrator healthcheck`, the Docker HEALTHCHECK of the image: GET /healthz of the telemetry
// server on telemetryAddr (TELEMETRY_ADDR), 0 for a 200 and 1 otherwise. With the telemetry off there is nothing to
// check, so it is 0.
func healthcheck(telemetryAddr string, client *http.Client) int {
	addr, err := configP.ParseTelemetryAddr(telemetryAddr)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	if addr == "" {
		return 0
	}

	// a wildcard listen address is reached through the loopback
	host, port, _ := net.SplitHostPort(addr)
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}

	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Println("healthz answered", resp.Status)
		return 1
	}
	return 0
}
