package telemetry

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

var t0 = time.Date(2026, 10, 29, 10, 0, 0, 0, time.UTC)

// fakeClock is a clock the test moves.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

var build = BuildInfo{Version: "0.4.0", Commit: "1a4c23f", GoVersion: "go1.26.2"}

func newTestTracker() (*Tracker, *fakeClock) {
	clock := &fakeClock{now: t0}
	return NewTracker("w-host1-k3f9qa", build, nil, clock.Now), clock
}

func TestNewTrackerIsStarting(t *testing.T) {
	tr, _ := newTestTracker()
	s := tr.Snapshot()

	if s.SchemaVersion != 1 || s.WorkerID != "w-host1-k3f9qa" || s.Version != "0.4.0" || s.Commit != "1a4c23f" ||
		s.GoVersion != "go1.26.2" || s.PID != os.Getpid() {
		t.Errorf("identity = %+v", s)
	}
	if s.Phase != PhaseStarting || s.PhaseBudgetSeconds != 60 || !s.StartedAt.Equal(t0) || !s.LastProgressAt.Equal(t0) {
		t.Errorf("phase %s budget %d started %s progress %s, want STARTING, 60, %s", s.Phase, s.PhaseBudgetSeconds, s.StartedAt, s.LastProgressAt, t0)
	}
	if s.Job != nil || s.WordlistCache != nil || s.Counters.JobsByOutcome == nil {
		t.Errorf("job %v cache %v outcomes %v, want no job, no cache, an empty map", s.Job, s.WordlistCache, s.Counters.JobsByOutcome)
	}
}

func TestTimesAreInUTC(t *testing.T) {
	local := time.FixedZone("CEST", 2*60*60)
	tr := NewTracker("w", build, nil, func() time.Time { return t0.In(local) })

	if s := tr.Snapshot(); s.Now.Location() != time.UTC || s.StartedAt.Location() != time.UTC {
		t.Errorf("now %s, startedAt %s, want UTC", s.Now, s.StartedAt)
	}
}

func TestSetPhaseSetsTheBudgetAndTouches(t *testing.T) {
	tr, clock := newTestTracker()
	clock.Advance(10 * time.Second)
	tr.SetPhase(PhasePreparing, "DOWNLOADING_FILES")

	s := tr.Snapshot()
	at := t0.Add(10 * time.Second)
	if s.Phase != PhasePreparing || s.PhaseDetail != "DOWNLOADING_FILES" || s.PhaseBudgetSeconds != 1500 ||
		!s.PhaseSince.Equal(at) || !s.LastProgressAt.Equal(at) {
		t.Errorf("status = %s/%s budget %d since %s progress %s", s.Phase, s.PhaseDetail, s.PhaseBudgetSeconds, s.PhaseSince, s.LastProgressAt)
	}
}

func TestSetDetailKeepsThePhaseAndTouches(t *testing.T) {
	tr, clock := newTestTracker()
	tr.SetPhase(PhasePreparing, "")
	clock.Advance(3 * time.Second)
	tr.SetDetail("WORDLISTS")

	s := tr.Snapshot()
	if s.PhaseDetail != "WORDLISTS" || !s.PhaseSince.Equal(t0) || !s.LastProgressAt.Equal(t0.Add(3*time.Second)) {
		t.Errorf("detail %s since %s progress %s, want WORDLISTS since %s with progress 3 s later", s.PhaseDetail, s.PhaseSince, s.LastProgressAt, t0)
	}
}

func TestTouchRecordsProgress(t *testing.T) {
	tr, clock := newTestTracker()
	clock.Advance(7 * time.Second)
	tr.Touch()

	if s := tr.Snapshot(); !s.LastProgressAt.Equal(t0.Add(7*time.Second)) || !s.PhaseSince.Equal(t0) {
		t.Errorf("progress %s since %s, want progress moved, phase not", s.LastProgressAt, s.PhaseSince)
	}
}

// a full job, the way the main loop calls the tracker
func TestAFullJobSequence(t *testing.T) {
	tr, clock := newTestTracker()
	tr.SetPhase(PhaseIdle, "")
	tr.DependencyResult(DepRabbitMQ, nil)

	clock.Advance(time.Second)
	tr.SetPhase(PhasePreparing, "")
	tr.MessageTaken(4211, "fuzz")
	tr.SetDetail("DOWNLOADING_FILES")
	tr.DependencyResult(DepObjectStorage, nil)
	tr.SetDetail("INITIAL_POLL")
	tr.DependencyResult(DepCore, nil)

	clock.Advance(time.Second)
	tr.SetPhase(PhaseStartingJob, "STARTING_CONNECTOR")
	tr.JobCommand("ffuf-connector")
	tr.ConnectorStarted(5, 60)
	tr.JobStarted()
	tr.SetDetail("ACCEPTING_SOCKETS")
	tr.ConnectorConnected()
	tr.SetPhase(PhaseRunningJob, "")

	clock.Advance(5 * time.Second)
	tr.ConnectorMessage(job.MessageHeartbeat)
	tr.JobState(job.StatePaused)

	s := tr.Snapshot()
	j := s.Job
	if j == nil || j.JobID != 4211 || j.RoutineKey != "fuzz" || j.Command != "ffuf-connector" || j.State != "PAUSED" {
		t.Fatalf("job = %+v, want 4211 fuzz ffuf-connector PAUSED", j)
	}
	if !j.TakenAt.Equal(t0.Add(time.Second)) || j.StartedAt == nil || !j.StartedAt.Equal(t0.Add(2*time.Second)) {
		t.Errorf("taken %s started %v", j.TakenAt, j.StartedAt)
	}
	c := j.Connector
	hb := t0.Add(7 * time.Second)
	if c == nil || !c.Connected || c.HeartbeatIntervalSeconds != 5 || c.HeartbeatTimeoutSeconds != 60 ||
		c.LastHeartbeatAt == nil || !c.LastHeartbeatAt.Equal(hb) || !c.LastMessageAt.Equal(hb) {
		t.Errorf("connector = %+v", c)
	}
	if j.Resources != nil {
		t.Errorf("resources = %+v, want null (not sampled)", j.Resources)
	}
	if s.Phase != PhaseRunningJob || s.PhaseBudgetSeconds != 30 {
		t.Errorf("phase %s budget %d, want RUNNING_JOB, 30", s.Phase, s.PhaseBudgetSeconds)
	}

	tr.SetPhase(PhaseFinishing, "UPLOADING_RESULT")
	tr.DependencyResult(DepObjectStorage, nil)
	tr.SetDetail("REPORTING")
	tr.DependencyResult(DepCore, nil)
	tr.JobEnded(job.StateFinished)
	tr.SetPhase(PhaseIdle, "")

	s = tr.Snapshot()
	want := Counters{MessagesTaken: 1, JobsStarted: 1, JobsByOutcome: map[string]int{"FINISHED": 1}}
	if s.Job != nil || s.Counters.MessagesTaken != want.MessagesTaken || s.Counters.JobsStarted != 1 || s.Counters.JobsByOutcome["FINISHED"] != 1 {
		t.Errorf("after the job: job %v counters %+v, want %+v", s.Job, s.Counters, want)
	}
	if up := s.Dependencies.RabbitMQ.Up; up == nil || !*up {
		t.Errorf("rabbitmq up = %v, want true", up)
	}
	if s.Dependencies.Core.Up != nil || s.Dependencies.ObjectStorage.Up != nil {
		t.Error("core and object storage are not connections, their up must stay null")
	}
}

func TestJobCallsWithoutAJobDoNothing(t *testing.T) {
	tr, _ := newTestTracker()
	tr.JobCommand("ffuf-connector")
	tr.ConnectorStarted(5, 60)
	tr.JobStarted()
	tr.ConnectorConnected()
	tr.ConnectorMessage(job.MessageHeartbeat)
	tr.JobState(job.StateRunning)

	if s := tr.Snapshot(); s.Job != nil || s.Counters.JobsStarted != 0 {
		t.Errorf("job %v jobs started %d, want nothing", s.Job, s.Counters.JobsStarted)
	}
}

func TestConnectorMessageBeforeTheConnectorStartedDoesNothing(t *testing.T) {
	tr, _ := newTestTracker()
	tr.MessageTaken(1, "fuzz")
	tr.ConnectorMessage(job.MessageHeartbeat)

	if s := tr.Snapshot(); s.Job.Connector != nil {
		t.Errorf("connector = %+v, want null", s.Job.Connector)
	}
}

func TestConnectorMessageIsNotProgress(t *testing.T) {
	tr, clock := newTestTracker()
	tr.MessageTaken(1, "fuzz")
	tr.ConnectorStarted(5, 60)
	clock.Advance(10 * time.Second)
	tr.ConnectorMessage(job.MessageStateUpdate)

	s := tr.Snapshot()
	if !s.LastProgressAt.Equal(t0) {
		t.Errorf("progress = %s, want %s: a message of the connector proves nothing of the main loop", s.LastProgressAt, t0)
	}
	if c := s.Job.Connector; c.LastMessageAt == nil || c.LastHeartbeatAt != nil {
		t.Errorf("connector = %+v, want a message time and no heartbeat", c)
	}
}

func TestJobEndedCountsTheOutcomeAndClearsTheJob(t *testing.T) {
	tr, _ := newTestTracker()
	for _, outcome := range []job.JobState{job.StateFailed, job.StateFailed, job.StateSelfTerminated, OutcomeNotStarted} {
		tr.MessageTaken(1, "fuzz")
		tr.JobEnded(outcome)
	}

	s := tr.Snapshot()
	if s.Job != nil || s.Counters.JobsByOutcome["FAILED"] != 2 || s.Counters.JobsByOutcome["SELFTERMINATED"] != 1 ||
		s.Counters.JobsByOutcome["NOT_STARTED"] != 1 || s.Counters.MessagesTaken != 4 {
		t.Errorf("job %v counters %+v", s.Job, s.Counters)
	}
}

func TestMessageSkipped(t *testing.T) {
	tr, _ := newTestTracker()
	// an unreadable message has no job
	tr.MessageSkipped()
	// a stale one has
	tr.MessageTaken(1, "fuzz")
	tr.MessageSkipped()

	s := tr.Snapshot()
	if s.Job != nil || s.Counters.MessagesSkipped != 2 || s.Counters.JobsByOutcome["NOT_STARTED"] != 1 {
		t.Errorf("job %v counters %+v, want 2 skipped, 1 NOT_STARTED", s.Job, s.Counters)
	}
}

func TestDependencyResultRecordsTheSanitizedError(t *testing.T) {
	tr, clock := newTestTracker()
	tr.DependencyResult(DepCore, nil)
	clock.Advance(time.Minute)
	tr.DependencyResult(DepCore, errors.New(`Get "http://user:pw@core:8080/poll/1": connection refused`))
	tr.DependencyResult(DepCore, errors.New("central service returned status 503 for job 1"))

	c := tr.Snapshot().Dependencies.Core
	at := t0.Add(time.Minute)
	if c.LastSuccessAt == nil || !c.LastSuccessAt.Equal(t0) || c.LastErrorAt == nil || !c.LastErrorAt.Equal(at) {
		t.Errorf("core = %+v, want a success at %s and an error at %s", c, t0, at)
	}
	if c.ErrorsTotal != 2 || c.LastError != "central service returned status 503 for job 1" {
		t.Errorf("core errors %d last %q", c.ErrorsTotal, c.LastError)
	}

	tr.DependencyResult(DepObjectStorage, errors.New(`Get "http://user:pw@rustfs:9000/x": EOF`))
	if e := tr.Snapshot().Dependencies.ObjectStorage.LastError; e != `Get "http://***@rustfs:9000/x": EOF` {
		t.Errorf("lastError = %q, want the user info masked", e)
	}
}

func TestRabbitMQIsUpAfterASuccessAndDownAfterAClose(t *testing.T) {
	tr, _ := newTestTracker()
	tr.DependencyResult(DepRabbitMQ, nil)
	tr.DependencyDown(DepRabbitMQ, errors.New("Exception (320) Reason: \"CONNECTION_FORCED\""))

	r := tr.Snapshot().Dependencies.RabbitMQ
	if r.Up == nil || *r.Up || r.ErrorsTotal != 1 || r.LastErrorAt == nil {
		t.Errorf("rabbitmq = %+v, want down with the error", r)
	}

	tr.DependencyResult(DepRabbitMQ, nil)
	if r := tr.Snapshot().Dependencies.RabbitMQ; r.Up == nil || !*r.Up {
		t.Errorf("rabbitmq up = %v after a success, want true", r.Up)
	}
}

func TestDependencyDownWithoutAnError(t *testing.T) {
	tr, _ := newTestTracker()
	tr.DependencyDown(DepRabbitMQ, nil)

	if r := tr.Snapshot().Dependencies.RabbitMQ; r.Up == nil || *r.Up || r.ErrorsTotal != 0 {
		t.Errorf("rabbitmq = %+v, want down and no error", r)
	}
}

func TestUnknownDependencyIsIgnored(t *testing.T) {
	tr, _ := newTestTracker()
	tr.DependencyResult(Dependency("redis"), errors.New("x"))
	tr.DependencyDown(Dependency("redis"), errors.New("x"))

	d := tr.Snapshot().Dependencies
	if d.RabbitMQ.ErrorsTotal+d.Core.ErrorsTotal+d.ObjectStorage.ErrorsTotal != 0 {
		t.Errorf("dependencies = %+v, want nothing recorded", d)
	}
}

func TestDrainingAndShuttingDown(t *testing.T) {
	tr, _ := newTestTracker()
	tr.SetDraining(true)
	if s := tr.Snapshot(); !s.Draining || s.ShuttingDown {
		t.Errorf("draining %v shutting down %v", s.Draining, s.ShuttingDown)
	}
	tr.SetDraining(false)
	tr.SetShuttingDown()
	if s := tr.Snapshot(); s.Draining || !s.ShuttingDown {
		t.Errorf("draining %v shutting down %v", s.Draining, s.ShuttingDown)
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	tr, _ := newTestTracker()
	tr.MessageTaken(1, "fuzz")
	tr.ConnectorStarted(5, 60)
	tr.JobEnded(job.StateFailed)
	tr.MessageTaken(2, "fuzz")
	tr.ConnectorStarted(5, 60)

	s := tr.Snapshot()
	s.Job.JobID = 99
	s.Job.Connector.Connected = true
	s.Counters.JobsByOutcome["FAILED"] = 50

	again := tr.Snapshot()
	if again.Job.JobID != 2 || again.Job.Connector.Connected || again.Counters.JobsByOutcome["FAILED"] != 1 {
		t.Errorf("changing a snapshot changed the tracker: %+v %+v", again.Job, again.Counters)
	}
}

func TestSnapshotReadsTheWordlistCacheAtMostEvery30Seconds(t *testing.T) {
	clock := &fakeClock{now: t0}
	reads := 0
	stats := func() (WordlistCache, error) {
		reads++
		return WordlistCache{Dir: "/cache", Files: reads}, nil
	}
	tr := NewTracker("w", build, stats, clock.Now)

	if c := tr.Snapshot().WordlistCache; c == nil || c.Files != 1 {
		t.Fatalf("cache = %+v, want the first reading", c)
	}
	clock.Advance(29 * time.Second)
	if c := tr.Snapshot().WordlistCache; c.Files != 1 || reads != 1 {
		t.Errorf("cache %+v after %d reads, want the cached reading", c, reads)
	}
	clock.Advance(time.Second)
	if c := tr.Snapshot().WordlistCache; c.Files != 2 || reads != 2 {
		t.Errorf("cache %+v after %d reads, want a new reading at 30 s", c, reads)
	}

	tr.Snapshot().WordlistCache.Files = 77
	if c := tr.Snapshot().WordlistCache; c.Files != 2 {
		t.Errorf("changing a snapshot changed the cached reading: %+v", c)
	}
}

func TestSnapshotHasNoCacheWhenItCanNotBeRead(t *testing.T) {
	clock := &fakeClock{now: t0}
	fail := true
	stats := func() (WordlistCache, error) {
		if fail {
			return WordlistCache{}, errors.New("no such directory")
		}
		return WordlistCache{Files: 3}, nil
	}
	tr := NewTracker("w", build, stats, clock.Now)

	if c := tr.Snapshot().WordlistCache; c != nil {
		t.Errorf("cache = %+v, want null", c)
	}
	fail = false
	clock.Advance(30 * time.Second)
	if c := tr.Snapshot().WordlistCache; c == nil || c.Files != 3 {
		t.Errorf("cache = %+v, want it read again after 30 s", c)
	}
}

func TestHealth(t *testing.T) {
	tr, clock := newTestTracker()
	tr.SetPhase(PhaseIdle, "")

	clock.Advance(30 * time.Second)
	if ok, age, phase := tr.Health(); !ok || age != 30*time.Second || phase != PhaseIdle {
		t.Errorf("Health() at the budget = %v, %s, %s, want ok, 30s, IDLE", ok, age, phase)
	}
	clock.Advance(time.Second)
	if ok, age, _ := tr.Health(); ok || age != 31*time.Second {
		t.Errorf("Health() past the budget = %v, %s, want not ok, 31s", ok, age)
	}

	tr.SetPhase(PhasePreparing, "WORDLISTS")
	clock.Advance(20 * time.Minute)
	if ok, _, phase := tr.Health(); !ok || phase != PhasePreparing {
		t.Errorf("Health() during a 20 minute download = %v in %s, want ok", ok, phase)
	}
}

func TestConcurrentUse(t *testing.T) {
	clock := &fakeClock{now: t0}
	tr := NewTracker("w", build, func() (WordlistCache, error) { return WordlistCache{}, nil }, clock.Now)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 200 {
				tr.Snapshot()
				tr.Health()
				tr.ConnectorMessage(job.MessageHeartbeat)
				tr.DependencyResult(DepCore, nil)
				clock.Advance(time.Second)
			}
		})
	}
	for i := range 200 {
		tr.SetPhase(PhasePreparing, "")
		tr.MessageTaken(i, "fuzz")
		tr.ConnectorStarted(5, 60)
		tr.JobStarted()
		tr.Touch()
		tr.JobEnded(job.StateFinished)
	}
	wg.Wait()
}

func TestSetCacheStatsReadsTheNewCacheAtOnce(t *testing.T) {
	tr, clock := newTestTracker()
	if c := tr.Snapshot().WordlistCache; c != nil {
		t.Fatalf("cache = %+v before SetCacheStats, want null", c)
	}

	tr.SetCacheStats(func() (WordlistCache, error) { return WordlistCache{Files: 1}, nil })
	tr.Snapshot()
	clock.Advance(time.Second)
	tr.SetCacheStats(func() (WordlistCache, error) { return WordlistCache{Files: 2}, nil })

	if c := tr.Snapshot().WordlistCache; c == nil || c.Files != 2 {
		t.Errorf("cache = %+v, want the reading of the new function without waiting 30 s", c)
	}
}
