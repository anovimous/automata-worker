package job

type Job struct {
	Id         int      `json:"jobId"`
	RoutineKey string   `json:"routineKey"`
	State      JobState `json:"state"`
}

type JobState string

const (
	StateQueued     JobState = "QUEUED"
	StateRunning    JobState = "RUNNING"
	StatePaused     JobState = "PAUSED"
	StateCanceled   JobState = "CANCELED"
	StateFinished   JobState = "FINISHED"
	StateFailed     JobState = "FAILED"
	StateTerminated JobState = "TERMINATED"
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
	ResultFilePath string   `json:"resultPath"`
	Message        string   `json:"message"`
	FinishState    JobState `json:"finishState"`
}
