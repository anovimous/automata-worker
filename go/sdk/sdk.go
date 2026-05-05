package sdk

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"sync"
	"time"
)

type SDK struct {
	jobId             int
	readSocketPath    string
	writeSocketPath   string
	heartbeatInterval int
	detailsPath       string
	dataPath          string
	standalone        bool
	tmpDir            string

	readConn  net.Conn
	writeConn net.Conn

	writeMu sync.Mutex
	stateMu sync.Mutex

	cancelled bool
	paused    bool
}

type heartbeatMessage struct {
	Type string `json:"type"`
}

type stateUpdateMessage struct {
	Type     string `json:"type"`
	NewState string `json:"newState"`
}

type finishMessage struct {
	Type   string    `json:"type"`
	Result jobResult `json:"result"`
}

type jobResult struct {
	ResultFilePath string `json:"resultPath"`
	Message        string `json:"message"`
	FinishState    string `json:"finishState"`
}

func New() (*SDK, error) {
	jobId := flag.Int("jobId", 0, "job id")
	details := flag.String("details", "", "path to job details file")
	data := flag.String("data", "", "path to data file")
	readSocket := flag.String("readSocket", "", "path to read socket")
	writeSocket := flag.String("writeSocket", "", "path to write socket")
	heartbeatInterval := flag.Int("heartbeatInterval", 5, "heartbeat interval in seconds")
	tmpDir := flag.String("tmpDir", "/var/tmp", "path to result tmp dir")
	flag.Parse()

	s := &SDK{
		jobId:             *jobId,
		detailsPath:       *details,
		dataPath:          *data,
		readSocketPath:    *readSocket,
		writeSocketPath:   *writeSocket,
		heartbeatInterval: *heartbeatInterval,
		standalone:        *readSocket == "" || *writeSocket == "",
		tmpDir:            *tmpDir,
	}

	if !s.standalone {
		readConn, err := net.Dial("unix", *readSocket)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to read socket: %w", err)
		}

		writeConn, err := net.Dial("unix", *writeSocket)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to write socket: %w", err)
		}

		s.readConn = readConn
		s.writeConn = writeConn

		go s.heartbeatLoop()
		go s.listenerLoop()
	}

	return s, nil
}

func (s *SDK) heartbeatLoop() {
	ticker := time.NewTicker(time.Duration(s.heartbeatInterval) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.sendMessage(heartbeatMessage{Type: "heartbeat"})
	}
}

func (s *SDK) listenerLoop() {
	decoder := json.NewDecoder(s.readConn)

	for {
		var incoming struct {
			State string `json:"state"`
		}

		if err := decoder.Decode(&incoming); err != nil {
			return
		}

		s.stateMu.Lock()
		switch incoming.State {
		case "cancelled":
			s.cancelled = true
		case "paused":
			s.paused = true
		case "resumed":
			s.paused = false
		}
		s.stateMu.Unlock()
	}
}

func (s *SDK) IsCancelled() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.cancelled
}

func (s *SDK) IsPaused() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.paused
}

func (s *SDK) WaitForResume() {
	for {
		s.stateMu.Lock()
		paused := s.paused
		s.stateMu.Unlock()

		if !paused {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (s *SDK) UpdateState(newState string) {
	s.sendMessage(stateUpdateMessage{
		Type:     "stateUpdate",
		NewState: newState,
	})

	if !s.standalone {
		s.readConn.Close()
		s.writeConn.Close()
	}
}

func (s *SDK) Finish(resultPath string) {
	s.sendMessage(finishMessage{
		Type: "finish",
		Result: jobResult{
			ResultFilePath: resultPath,
			FinishState:    "FINISHED",
		},
	})

	if !s.standalone {
		s.readConn.Close()
		s.writeConn.Close()
	}
}

func (s *SDK) Fail(message string) {
	s.sendMessage(finishMessage{
		Type: "finish",
		Result: jobResult{
			Message:     message,
			FinishState: "FAILED",
		},
	})

	if !s.standalone {
		s.readConn.Close()
		s.writeConn.Close()
	}
}

func (s *SDK) sendMessage(msg any) error {
	if s.standalone {
		return nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if err := json.NewEncoder(s.writeConn).Encode(msg); err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (s *SDK) DetailsPath() string {
	return s.detailsPath
}

func (s *SDK) DataPath() string {
	return s.dataPath
}

func (s *SDK) TmpDirectory() string {
	return s.tmpDir
}

func (s *SDK) JobId() int {
	return s.jobId
}
