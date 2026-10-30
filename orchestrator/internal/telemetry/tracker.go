package telemetry

import (
	log "log/slog"
	"maps"
	"os"
	"sync"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

// Dependency names a service the worker talks to.
type Dependency string

const (
	DepRabbitMQ      Dependency = "rabbitmq"
	DepCore          Dependency = "core"
	DepObjectStorage Dependency = "objectStorage"
)

// OutcomeNotStarted counts a job that was taken but never started: a stale or duplicate message, or one put back on
// the queue. It is not a state of the core.
const OutcomeNotStarted job.JobState = "NOT_STARTED"

// the wordlist cache is read at most this often, walking its directory on every scrape would be wasteful
const cacheStatsMaxAge = 30 * time.Second

// BuildInfo is what the binary was built from.
type BuildInfo struct {
	Version   string
	Commit    string
	GoVersion string
}

// Tracker keeps the status of the worker. Its methods are safe for concurrent use. Touch, SetPhase and SetDetail
// prove that the main loop moves, so only the main loop calls them, never a goroutine of its own.
type Tracker struct {
	mu     sync.Mutex
	clock  func() time.Time
	status Status

	// the wordlist cache is read outside mu, one read at a time
	cacheMu     sync.Mutex
	cacheStats  func() (WordlistCache, error)
	cache       *WordlistCache
	cacheReadAt time.Time
}

// NewTracker starts in phase STARTING. cacheStats reads the wordlist cache, it may be nil.
func NewTracker(workerID string, build BuildInfo, cacheStats func() (WordlistCache, error), clock func() time.Time) *Tracker {
	t := &Tracker{clock: clock, cacheStats: cacheStats}
	now := t.now()
	t.status = Status{
		SchemaVersion: SchemaVersion,
		WorkerID:      workerID,
		Version:       build.Version,
		Commit:        build.Commit,
		GoVersion:     build.GoVersion,
		PID:           os.Getpid(),
		StartedAt:     now,
		Counters:      Counters{JobsByOutcome: map[string]int{}},
	}
	t.setPhase(PhaseStarting, "", now)
	return t
}

// SetCacheStats gives the tracker the reading of the wordlist cache, for a cache made after the tracker.
func (t *Tracker) SetCacheStats(cacheStats func() (WordlistCache, error)) {
	t.cacheMu.Lock()
	defer t.cacheMu.Unlock()
	t.cacheStats = cacheStats
	t.cache = nil
	t.cacheReadAt = time.Time{}
}

// now is the clock in UTC, as the contract writes its times.
func (t *Tracker) now() time.Time {
	return t.clock().UTC()
}

// SetPhase enters a phase; it counts as progress.
func (t *Tracker) SetPhase(p Phase, detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setPhase(p, detail, t.now())
}

func (t *Tracker) setPhase(p Phase, detail string, now time.Time) {
	t.status.Phase = p
	t.status.PhaseDetail = detail
	t.status.PhaseSince = now
	t.status.PhaseBudgetSeconds = int(PhaseBudget(p).Seconds())
	t.status.LastProgressAt = now
}

// SetDetail names the step inside the phase; it counts as progress.
func (t *Tracker) SetDetail(detail string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.PhaseDetail = detail
	t.status.LastProgressAt = t.now()
}

// Touch records that the main loop made progress.
func (t *Tracker) Touch() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.LastProgressAt = t.now()
}

// MessageTaken starts the job of a message taken from the queue.
func (t *Tracker) MessageTaken(jobId int, routineKey string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Job = &Job{JobID: jobId, RoutineKey: routineKey, TakenAt: t.now()}
	t.status.Counters.MessagesTaken++
}

// MessageSkipped ends a message that is not run: unreadable, stale or a duplicate. A job that was set is counted
// NOT_STARTED.
func (t *Tracker) MessageSkipped() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Counters.MessagesSkipped++
	if t.status.Job != nil {
		t.status.Counters.JobsByOutcome[string(OutcomeNotStarted)]++
	}
	t.status.Job = nil
}

// JobCommand is the connector the routine maps to.
func (t *Tracker) JobCommand(command string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.Job != nil {
		t.status.Job.Command = command
	}
}

// ConnectorStarted records that the connector process runs; it is not connected yet.
func (t *Tracker) ConnectorStarted(heartbeatInterval, heartbeatTimeout int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.Job != nil {
		t.status.Job.Connector = &Connector{HeartbeatIntervalSeconds: heartbeatInterval, HeartbeatTimeoutSeconds: heartbeatTimeout}
	}
}

// JobStarted records that RUNNING was reported to the core.
func (t *Tracker) JobStarted() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.Job == nil {
		return
	}
	now := t.now()
	t.status.Job.StartedAt = &now
	t.status.Job.State = string(job.StateRunning)
	t.status.Counters.JobsStarted++
}

// ConnectorConnected records that the connector connected to both sockets.
func (t *Tracker) ConnectorConnected() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.connector(); c != nil {
		c.Connected = true
	}
}

// ConnectorMessage records a message of the connector. It is called from the watcher's goroutine, so it is not
// progress of the main loop.
func (t *Tracker) ConnectorMessage(typ job.MessageType) {
	t.mu.Lock()
	defer t.mu.Unlock()
	c := t.connector()
	if c == nil {
		return
	}
	now := t.now()
	c.LastMessageAt = &now
	if typ == job.MessageHeartbeat {
		c.LastHeartbeatAt = &now
	}
}

func (t *Tracker) connector() *Connector {
	if t.status.Job == nil {
		return nil
	}
	return t.status.Job.Connector
}

// JobState records a state the connector reported (RUNNING, PAUSED).
func (t *Tracker) JobState(s job.JobState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.Job != nil {
		t.status.Job.State = string(s)
	}
}

// JobEnded counts the outcome of the job and clears it.
func (t *Tracker) JobEnded(outcome job.JobState) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Counters.JobsByOutcome[string(outcome)]++
	t.status.Job = nil
}

// DependencyResult records the result of a call to a dependency: its time, and for an error its sanitized text. A
// success with RabbitMQ also means its connection is up.
func (t *Tracker) DependencyResult(d Dependency, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	dep := t.dependency(d)
	if dep == nil {
		return
	}
	now := t.now()
	if err != nil {
		t.recordError(dep, err, now)
		return
	}
	dep.LastSuccessAt = &now
	if d == DepRabbitMQ {
		up := true
		dep.Up = &up
	}
}

// DependencyDown records that the connection to a dependency closed.
func (t *Tracker) DependencyDown(d Dependency, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	dep := t.dependency(d)
	if dep == nil {
		return
	}
	down := false
	dep.Up = &down
	if err != nil {
		t.recordError(dep, err, t.now())
	}
}

func (t *Tracker) recordError(dep *DependencyStatus, err error, now time.Time) {
	dep.LastErrorAt = &now
	dep.LastError = Sanitize(err.Error())
	dep.ErrorsTotal++
}

func (t *Tracker) dependency(d Dependency) *DependencyStatus {
	switch d {
	case DepRabbitMQ:
		return &t.status.Dependencies.RabbitMQ
	case DepCore:
		return &t.status.Dependencies.Core
	case DepObjectStorage:
		return &t.status.Dependencies.ObjectStorage
	}
	log.Warn("unknown dependency", "dependency", d)
	return nil
}

// SetDraining records that a drain was requested or undone.
func (t *Tracker) SetDraining(draining bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.Draining = draining
}

// SetShuttingDown records that a shutdown signal was received.
func (t *Tracker) SetShuttingDown() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.status.ShuttingDown = true
}

// Snapshot is the status as of now. The wordlist cache is read outside the lock, at most every 30 s.
func (t *Tracker) Snapshot() Status {
	t.mu.Lock()
	s := t.status
	s.Now = t.now()
	s.Counters.JobsByOutcome = maps.Clone(t.status.Counters.JobsByOutcome)
	if t.status.Job != nil {
		j := *t.status.Job
		if j.Connector != nil {
			c := *j.Connector
			j.Connector = &c
		}
		s.Job = &j
	}
	t.mu.Unlock()

	s.WordlistCache = t.wordlistCache(s.Now)
	return s
}

// wordlistCache is the last cache reading, read again when it is older than cacheStatsMaxAge; null when it could
// not be read.
func (t *Tracker) wordlistCache(now time.Time) *WordlistCache {
	t.cacheMu.Lock()
	defer t.cacheMu.Unlock()
	if t.cacheStats == nil {
		return nil
	}
	if t.cacheReadAt.IsZero() || now.Sub(t.cacheReadAt) >= cacheStatsMaxAge {
		t.cacheReadAt = now
		stats, err := t.cacheStats()
		if err != nil {
			log.Warn("failed to read the wordlist cache for the status", "error", err)
			t.cache = nil
		} else {
			t.cache = &stats
		}
	}
	if t.cache == nil {
		return nil
	}
	c := *t.cache
	return &c
}

// Health tells whether the main loop made progress within the budget of its phase (GET /healthz).
func (t *Tracker) Health() (ok bool, progressAge time.Duration, phase Phase) {
	t.mu.Lock()
	defer t.mu.Unlock()
	age := t.now().Sub(t.status.LastProgressAt)
	return age <= PhaseBudget(t.status.Phase), age, t.status.Phase
}
