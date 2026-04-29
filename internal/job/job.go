package job

type Job struct {
	Id         int      `json:"jobId"`
	RoutineKey string   `json:"routineKey"`
	State      JobState `json:"state"`
}

type JobState string

const (
	StateRunning  JobState = "RUNNING"
	StatePaused   JobState = "PAUSED"
	StateCanceled JobState = "CANCELED"
	StateFinished JobState = "FINISHED"
	StateFailed   JobState = "FAILED"
)
