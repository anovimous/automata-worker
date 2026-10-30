package socket

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
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

func AcceptConns(s1 *Socket, s2 *Socket, timeoutSecs int) (*Socket, *Socket, error, error) {
	return AcceptConnsContext(context.Background(), s1, s2, timeoutSecs)
}

// AcceptConnsContext is AcceptConns that also gives up when ctx is done, so that a shutdown does not wait for the
// whole timeout.
func AcceptConnsContext(ctx context.Context, s1 *Socket, s2 *Socket, timeoutSecs int) (*Socket, *Socket, error, error) {
	var (
		wg           sync.WaitGroup
		conn1, conn2 net.Conn
		e1, e2       error
	)

	deadline := time.Now().Add(time.Duration(timeoutSecs) * time.Second)

	s1.Listener.(*net.UnixListener).SetDeadline(deadline)
	s2.Listener.(*net.UnixListener).SetDeadline(deadline)

	// a done ctx moves the deadlines to now, which ends both Accepts
	stop, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		select {
		case <-ctx.Done():
			now := time.Now()
			s1.Listener.(*net.UnixListener).SetDeadline(now)
			s2.Listener.(*net.UnixListener).SetDeadline(now)
		case <-stop:
		}
	}()

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
	// the goroutine above must be gone before the reset, or it could set a deadline for the next job
	close(stop)
	<-stopped

	// reset deadlines so listeners are reusable for next job
	s1.Listener.(*net.UnixListener).SetDeadline(time.Time{})
	s2.Listener.(*net.UnixListener).SetDeadline(time.Time{})

	if e1 != nil || e2 != nil {
		// one side may have connected, it must not be left open
		for _, conn := range []net.Conn{conn1, conn2} {
			if conn != nil {
				conn.Close()
			}
		}
		return nil, nil, e1, e2
	}

	s1.Connection = conn1
	s2.Connection = conn2

	return s1, s2, e1, e2
}

func (s *Socket) Listen(ctx context.Context) <-chan job.JobMessage {
	msgCh := make(chan job.JobMessage)
	go func() {
		defer close(msgCh)
		decoder := json.NewDecoder(s.Connection)

		// Close connection when context is cancelled to unblock Decode()
		go func() {
			<-ctx.Done()
			s.Connection.Close()
		}()

		for {
			var msg job.JobMessage
			if err := decoder.Decode(&msg); err != nil {
				log.Error("ORCH socket.Listen(): error on decoding message, going to call return")
				return // connection closed or broken — exit cleanly
			}
			log.Info("ORCH socket.Listen(): message recieved of", "type", msg.Typ)
			msgCh <- msg
		}
	}()
	return msgCh
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
