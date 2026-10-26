package job

type Job struct {
	Id         int      `json:"jobId"`
	RoutineKey string   `json:"routineKey"`
	State      JobState `json:"state"`
}

type JobState string

const (
	StateQueued         JobState = "QUEUED"
	StateRunning        JobState = "RUNNING"
	StatePaused         JobState = "PAUSED"
	StateCanceled       JobState = "CANCELED"
	StateFinished       JobState = "FINISHED"
	StateFailed         JobState = "FAILED"
	StateSelfTerminated JobState = "SELFTERMINATED"
	// only the core sets it, for a job which no worker has been in contact about
	StateLostContact JobState = "LOSTCONTACT"
)

type JobMessage struct {
	Typ    MessageType `json:"type"`
	State  JobState    `json:"newState"`
	Result JobResult   `json:"result"`
}

type MessageType string

const (
	MessageHeartbeat    MessageType = "heartbeat"
	MessageStateUpdate  MessageType = "stateUpdate"
	MessageFinishResult MessageType = "finish"
)

type JobResult struct {
	ResultFilePath  string          `json:"resultPath"`
	Message         string          `json:"message"`
	FinishState     JobState        `json:"finishState"`
	FailureCategory FailureCategory `json:"failureCategory"`
}

// FailureCategory tells the core what kind of failure ended a job, so a reader knows what to do next.
// The values are the ones of FailureCategory in the core, which reads any other as UNKNOWN. An empty one
// is not sent (a report that is no failure carries none).
type FailureCategory string

const (
	// the job could not be set up: work directory, files download, routine mapping
	FailurePreparation FailureCategory = "PREPARATION"
	// the first poll of the core failed
	FailurePoll FailureCategory = "POLL"
	// the job details could not be parsed or turned into a command
	FailureInvalidConfig FailureCategory = "INVALID_CONFIG"
	// the tool (or its connector) could not be started
	FailureToolStart FailureCategory = "TOOL_START"
	// the tool ran and exited with an error
	FailureToolExit FailureCategory = "TOOL_EXIT"
	// the result is missing or could not be uploaded
	FailureResult FailureCategory = "RESULT"
	// nothing more is known
	FailureUnknown FailureCategory = "UNKNOWN"
)
