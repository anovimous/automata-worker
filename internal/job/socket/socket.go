package socket

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"github.com/anovimous/orchestrator/internal/job"
)

type Socket struct {
	Path       string
	Listener   net.Listener
	Connection net.Conn
}

func New(socketDir string, mode rune) (offSocket *Socket, e error) {

	var path string

	switch mode {
	case 'r':
		path = fmt.Sprintf("%s/read", socketDir)
	case 'w':
		path = fmt.Sprintf("%s/write", socketDir)
	default:
		return nil, fmt.Errorf("available modes are 'r' and 'w'")
	}

	os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("could not create socket on the path %s : %s", path, err)
	}

	return &Socket{Path: path, Listener: ln}, nil

}

func AcceptConns(s1 *Socket, s2 *Socket) (*Socket, *Socket, error, error) {
	var (
		wg           sync.WaitGroup
		conn1, conn2 net.Conn
		e1, e2       error
	)

	wg.Add(2)

	go func() {
		defer wg.Done()
		conn1, e1 = s1.Listener.Accept()
	}()

	go func() {
		defer wg.Done()
		conn2, e2 = s2.Listener.Accept()
	}()

	wg.Wait()

	if e1 != nil || e2 != nil {
		return nil, nil, e1, e2
	}

	s1.Connection = conn1
	s2.Connection = conn2

	return s1, s2, e1, e2

}

func (s *Socket) Listen(ctx context.Context) (<-chan job.JobMessage, error) {

	msgCh := make(chan job.JobMessage)

	go func() {
		defer close(msgCh)

		decoder := json.NewDecoder(s.Connection)

		for {
			// check context before each decode attempt
			select {
			case <-ctx.Done():
				return
			default:
			}

			// set a short deadline so Decode does not block forever
			s.Connection.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

			var msg job.JobMessage
			if err := decoder.Decode(&msg); err != nil {
				// if it is a timeout error, loop back and check context
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				// any other error means the connection is broken
				return
			}

			// clear the deadline after a successful read
			s.Connection.SetReadDeadline(time.Time{})

			msgCh <- msg
		}
	}()

	return msgCh, nil
}

func (s *Socket) SendState(desiredState job.JobState) error {

	payload := struct {
		State job.JobState `json:"state"`
	}{
		State: desiredState,
	}

	if err := json.NewEncoder(s.Connection).Encode(payload); err != nil {
		return fmt.Errorf("failed to send state to socket %s: %w", s.Path, err)
	}

	return nil
}
