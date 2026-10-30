// Package telemetry keeps what the orchestrator is doing and serves it on GET /healthz and /status for the
// monitor (docs/telemetry-contract.md). The worker does not know the monitor: nothing here waits on it or calls it.
package telemetry

import "time"

// SchemaVersion of the status document; bump it only for an incompatible change (adding a field is compatible).
const SchemaVersion = 1

// Status is the status document. The JSON names are the contract, checked against contract/telemetry/. Optional
// objects are pointers so that they are written as null, never left out.
type Status struct {
	SchemaVersion      int            `json:"schemaVersion"`
	WorkerID           string         `json:"workerId"`
	Version            string         `json:"version"`
	Commit             string         `json:"commit"`
	GoVersion          string         `json:"goVersion"`
	PID                int            `json:"pid"`
	StartedAt          time.Time      `json:"startedAt"`
	Now                time.Time      `json:"now"`
	Phase              Phase          `json:"phase"`
	PhaseDetail        string         `json:"phaseDetail"`
	PhaseSince         time.Time      `json:"phaseSince"`
	PhaseBudgetSeconds int            `json:"phaseBudgetSeconds"`
	LastProgressAt     time.Time      `json:"lastProgressAt"`
	Draining           bool           `json:"draining"`
	ShuttingDown       bool           `json:"shuttingDown"`
	Job                *Job           `json:"job"`
	Dependencies       Dependencies   `json:"dependencies"`
	WordlistCache      *WordlistCache `json:"wordlistCache"`
	Counters           Counters       `json:"counters"`
}

// Job is the job the worker is busy with.
type Job struct {
	JobID      int           `json:"jobId"`
	RoutineKey string        `json:"routineKey"`
	Command    string        `json:"command"` // the connector, never its arguments
	State      string        `json:"state"`   // "" before RUNNING was reported, then RUNNING or PAUSED
	TakenAt    time.Time     `json:"takenAt"`
	StartedAt  *time.Time    `json:"startedAt"`
	Connector  *Connector    `json:"connector"`
	Resources  *JobResources `json:"resources"`
}

type Connector struct {
	Connected                bool       `json:"connected"`
	LastMessageAt            *time.Time `json:"lastMessageAt"`
	LastHeartbeatAt          *time.Time `json:"lastHeartbeatAt"`
	HeartbeatIntervalSeconds int        `json:"heartbeatIntervalSeconds"`
	HeartbeatTimeoutSeconds  int        `json:"heartbeatTimeoutSeconds"`
}

// JobResources is a sample of the job's cgroups. The orchestrator does not fill it yet, so it is always null.
type JobResources struct {
	SampledAt time.Time    `json:"sampledAt"`
	Job       *CgroupUsage `json:"job"`
	Tool      *CgroupUsage `json:"tool"`
}

type CgroupUsage struct {
	CPUUsageSeconds float64 `json:"cpuUsageSeconds"`
	MemoryBytes     *int64  `json:"memoryBytes"`
	MemoryPeakBytes *int64  `json:"memoryPeakBytes"`
	Pids            *int64  `json:"pids"`
}

type Dependencies struct {
	RabbitMQ      DependencyStatus `json:"rabbitmq"`
	Core          DependencyStatus `json:"core"`
	ObjectStorage DependencyStatus `json:"objectStorage"`
}

// DependencyStatus is a service as the worker sees it. Up is null for a service that is not a connection.
type DependencyStatus struct {
	Up            *bool      `json:"up"`
	LastSuccessAt *time.Time `json:"lastSuccessAt"`
	LastErrorAt   *time.Time `json:"lastErrorAt"`
	LastError     string     `json:"lastError"` // sanitized
	ErrorsTotal   int        `json:"errorsTotal"`
}

type WordlistCache struct {
	Dir         string `json:"dir"`
	Bytes       int64  `json:"bytes"`
	Files       int    `json:"files"`
	MaxBytes    int64  `json:"maxBytes"`
	PinnedFiles int    `json:"pinnedFiles"`
}

// Counters count since the process started.
type Counters struct {
	MessagesTaken   int            `json:"messagesTaken"`
	MessagesSkipped int            `json:"messagesSkipped"`
	JobsStarted     int            `json:"jobsStarted"`
	JobsByOutcome   map[string]int `json:"jobsByOutcome"`
}
