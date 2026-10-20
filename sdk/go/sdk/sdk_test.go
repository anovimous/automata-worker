package sdk

import (
	"encoding/json"
	"net"
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
