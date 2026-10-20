package sdk

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"testing"
)

func finishSentBy(t *testing.T, send func(s *SDK)) finishMessage {
	t.Helper()

	sdkEnd, orchestratorEnd := net.Pipe()
	defer orchestratorEnd.Close()
	readSdkEnd, readOrchestratorEnd := net.Pipe()
	defer readOrchestratorEnd.Close()

	s := &SDK{writeConn: sdkEnd, readConn: readSdkEnd}

	got := make(chan finishMessage, 1)
	go func() {
		var msg finishMessage
		json.NewDecoder(orchestratorEnd).Decode(&msg)
		got <- msg
	}()

	send(s)

	return <-got
}

func TestCancelReportsTheCanceledState(t *testing.T) {
	msg := finishSentBy(t, func(s *SDK) { s.Cancel("job was cancelled") })

	if msg.Type != "finish" || msg.Result.FinishState != "CANCELED" || msg.Result.Message != "job was cancelled" {
		t.Errorf("unexpected message %+v", msg)
	}
}

func TestFailReportsTheFailedState(t *testing.T) {
	msg := finishSentBy(t, func(s *SDK) { s.Fail("boom") })

	if msg.Type != "finish" || msg.Result.FinishState != "FAILED" || msg.Result.Message != "boom" {
		t.Errorf("unexpected message %+v", msg)
	}
}

func TestCancelAndFailAreNoOpsWhenStandalone(t *testing.T) {
	s := &SDK{standalone: true}

	s.Cancel("x")
	s.Fail("x")
}

func logsOf(t *testing.T, run func()) string {
	t.Helper()

	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	defer slog.SetDefault(previous)

	run()

	return buf.String()
}

func brokenSDK() *SDK {
	sdkEnd, orchestratorEnd := net.Pipe()
	orchestratorEnd.Close()
	readSdkEnd, readOrchestratorEnd := net.Pipe()
	readOrchestratorEnd.Close()

	return &SDK{writeConn: sdkEnd, readConn: readSdkEnd}
}

func TestAFailedFinishReportIsLogged(t *testing.T) {
	out := logsOf(t, func() { brokenSDK().Finish("/tmp/out") })

	if !strings.Contains(out, "failed to report to the orchestrator") || !strings.Contains(out, "type=finish") {
		t.Errorf("send error not logged: %s", out)
	}
}

func TestAFailedFailAndCancelReportIsLogged(t *testing.T) {
	for name, run := range map[string]func(s *SDK){
		"fail":   func(s *SDK) { s.Fail("boom") },
		"cancel": func(s *SDK) { s.Cancel("stop") },
	} {
		out := logsOf(t, func() { run(brokenSDK()) })

		if !strings.Contains(out, "failed to report to the orchestrator") {
			t.Errorf("%s: send error not logged: %s", name, out)
		}
	}
}

func TestAFailedStateUpdateIsLogged(t *testing.T) {
	out := logsOf(t, func() { brokenSDK().UpdateState("PAUSED") })

	if !strings.Contains(out, "type=stateUpdate") {
		t.Errorf("send error not logged: %s", out)
	}
}

func TestASuccessfulFinishLogsNoSendError(t *testing.T) {
	out := logsOf(t, func() { finishSentBy(t, func(s *SDK) { s.Finish("/tmp/out") }) })

	if strings.Contains(out, "failed to report") {
		t.Errorf("unexpected error log: %s", out)
	}
}
