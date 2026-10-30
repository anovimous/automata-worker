# Telemetry contract between a worker and the monitor

This contract belongs to this repo. It is what `automata-worker-monitor` relies on to observe and control the
workers; the worker does not know the monitor exists and keeps working without it. The monitor keeps a copy of this
document (`plan/05-telemetry-contract.md` in its repo) and of the samples (`contracts/`), and parses the samples
with its own types; here `orchestrator/internal/telemetry/contract_test.go` writes the Go types and compares them
with the samples in [`contract/telemetry/`](../contract/telemetry/). A renamed field fails a test on one side. Keep
both copies equal by hand: a change to the status document, the signals, the env vars or the labels changes this
file, the samples and the monitor's copy together.

The code is `orchestrator/internal/telemetry` (status, tracker, sanitizer, server), the instrumentation of the main
loop in `orchestrator/cmd/orchestrator/orchestrator.go`, and `orchestrator healthcheck`.

It has five parts: env vars, HTTP endpoints, the status document, signals and exit codes, container labels.

## 1. Env vars read by the orchestrator (new)

| Var | Default | Meaning |
|---|---|---|
| `WORKER_ID` | the hostname (`os.Hostname()`) | Identity of the worker in status and logs. The monitor sets `w-<host>-<6 chars>`. Must match `^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`, otherwise the start fails. |
| `TELEMETRY_ADDR` | `127.0.0.1:9464` | Listen address of the telemetry server. The value `off` disables it. An address that cannot be parsed or bound stops the start (like the wordlist cache settings). |
| `LOG_FORMAT` | `text` | `text` or `json` (slog handlers). Every log record carries `workerId`. |

## 2. HTTP endpoints (`TELEMETRY_ADDR`, read-only, no auth)

All responses carry `Cache-Control: no-store`. Only `GET` (and `HEAD`) are accepted, anything else is 405.
The server uses `ReadHeaderTimeout` 5 s, `WriteTimeout` 10 s, and is shut down last during exit.

| Endpoint | Response |
|---|---|
| `GET /healthz` | `200 ok` (text) when the loop made progress within the budget of its phase; `503` with `{"reason":"STUCK","phase":"PREPARING","progressAgeSeconds":1602}` otherwise. Used by the Docker `HEALTHCHECK` (`orchestrator healthcheck`). |
| `GET /status` | `200`, `application/json`, the status document (§3). |

The endpoints are not authenticated because (a) they bind to localhost or a private Docker network, (b) they
contain no secret: no env value, no URL user info, no command line, no details-file content. Error strings are
passed through the redaction rules of §3.4 before they are stored.

## 3. Status document, schema version 1

Samples: [`contract/telemetry/worker-status.v1.running.json`](../contract/telemetry/worker-status.v1.running.json),
[`contract/telemetry/worker-status.v1.idle.json`](../contract/telemetry/worker-status.v1.idle.json).

All timestamps are RFC 3339 UTC with second precision or better. Optional objects are `null` when absent, never
omitted, so the shape is stable. A consumer must ignore unknown fields (forward compatibility) and must refuse a
`schemaVersion` it does not know with a clear error (`UNSUPPORTED_SCHEMA`).

### 3.1 Top level

| Field | Type | Meaning |
|---|---|---|
| `schemaVersion` | int | `1`. Bumped only for an incompatible change; adding a field is compatible. |
| `workerId` | string | `WORKER_ID`. |
| `version` | string | Build version (`-ldflags -X`), `dev` when not set. |
| `commit` | string | Git commit, `unknown` when not set. |
| `goVersion` | string | `runtime.Version()`. |
| `pid` | int | Orchestrator pid (inside the container). |
| `startedAt` | time | Process start. |
| `now` | time | Time the document was produced (lets the monitor compute ages without trusting its own clock against the worker's). |
| `phase` | enum | See §3.2. |
| `phaseDetail` | string | Sub-step inside a phase, e.g. `DOWNLOADING_FILES`, `INITIAL_POLL`, `WORDLISTS`, `ACCEPTING_SOCKETS`, `UPLOADING_RESULT`, `REPORTING`; empty when none. |
| `phaseSince` | time | When the current phase started. |
| `phaseBudgetSeconds` | int | How long the loop may go without progress in this phase before it is considered stuck (§3.2). |
| `lastProgressAt` | time | Last time the loop proved it was alive (§3.3). |
| `draining` | bool | A drain was requested (`SIGUSR1`) and not undone. |
| `shuttingDown` | bool | `SIGTERM`/`SIGINT` received. |
| `job` | object \| null | Current job, §3.5. `null` when `IDLE`, `DRAINED`, `STARTING`. |
| `dependencies` | object | `rabbitmq`, `core`, `objectStorage`, each a dependency object (§3.6). |
| `wordlistCache` | object \| null | `{dir, bytes, files, maxBytes, pinnedFiles}`; `null` if the cache could not be read. |
| `counters` | object | §3.7. |

### 3.2 Phases and budgets

| Phase | Entered when | Progress is touched | `phaseBudgetSeconds` |
|---|---|---|---|
| `STARTING` | process start, until the main loop starts | — | 60 |
| `IDLE` | waiting for a message (queue empty or between jobs) | every loop iteration (≤ 2 s) | 30 |
| `PREPARING` | a message was taken: unmarshal, work dir, files download, initial poll, wordlists, mapping | between every step | 1500 (the wordlist step alone may take 20 min) |
| `STARTING_JOB` | `runner.Start` and `AcceptConns` (≤ 30 s) | before and after | 60 |
| `RUNNING_JOB` | job loop | every 5 s by a ticker in the `select`, and on every message | 30 |
| `FINISHING` | result upload and final report | between steps | 900 |
| `DRAINED` | draining and idle; the process is about to exit 0 | — | 30 |
| `SHUTTING_DOWN` | SIGTERM handling (terminate, report, clean up) | between steps | 90 |

The budgets are constants of the worker (one place: `telemetry.PhaseBudget`). The monitor never hard-codes them.

### 3.3 Progress

`lastProgressAt` is updated by `Tracker.Touch()` from the goroutine that runs the main loop only, never from the
HTTP server or a timer goroutine of its own, so it proves that the loop itself is moving. The job loop gets a
`time.Ticker` (5 s) case for this. Consumers compute `progressAgeSeconds = now − lastProgressAt` using the
document's own `now`.

### 3.4 Redaction of strings

Every error string stored in the status (`lastError` fields) goes through `telemetry.Sanitize`. The rules are
applied in this order (Go `regexp` syntax, replacements as for `ReplaceAllString`):

```
1. URL user info        ([a-zA-Z][a-zA-Z0-9+.-]*://)[^/@\s]+@                                       → ${1}***@
2. Authorization        (?i)((?:proxy-)?authorization["']?\s*[:=]\s*["']?(?:(?:bearer|basic|digest|token|negotiate)\s+)?)[^\s"',;]+
                                                                                                  → ${1}***
3. Cookies              (?i)((?:set-)?cookie["']?\s*[:=]\s*)[^"'\n]+                                 → ${1}***
4. Other secret names   (?i)(x-api-key|x-auth-token|api[-_]?key|token|password|passwd|secret)(["']?\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;'"}\]]+)
                                                                                                  → ${1}${2}***
5. Cut to 300 characters (rune-safe), with … appended when cut.
```

| Input | Output |
|---|---|
| `dial amqp://automata:s3cret@rabbitmq:5672/` | `dial amqp://***@rabbitmq:5672/` |
| `Authorization: Bearer eyJhbGci.x.y` | `Authorization: Bearer ***` (the scheme stays readable, the credential goes) |
| `authorization=Basic dXNlcjpwYXNz rest` | `authorization=Basic *** rest` |
| `Cookie: session=abc; theme=dark` | `Cookie: ***` (every cookie of the header) |
| `TOKEN=abc PASSWORD=hunter2` | `TOKEN=*** PASSWORD=***` |
| `{"user":"a","password":"x y"}` | `{"user":"a","password":***}` |

Applying the rules to a string they already produced gives the same string, so the monitor can redact again what
the worker redacted. The monitor's `internal/redact` uses rules 1–4 (no cut) plus the command-line forms
`-H '<secret header>: …'` and `-b '…'` of the connectors' logs.

### 3.5 `job`

| Field | Type | Meaning |
|---|---|---|
| `jobId` | int | From the queue message. |
| `routineKey` | string | From the queue message. |
| `command` | string | Connector from the mapping (`ffuf-connector`), empty before the lookup. Never the arguments. |
| `state` | enum \| `""` | As the worker knows it: `""` before `RUNNING` was reported, then `RUNNING` / `PAUSED` from the connector's state updates. |
| `takenAt` | time | When the message was taken. |
| `startedAt` | time \| null | When `RUNNING` was reported. |
| `connector` | object \| null | `{connected, lastMessageAt, lastHeartbeatAt, heartbeatIntervalSeconds, heartbeatTimeoutSeconds}`; `null` before the connector was started. |
| `resources` | object \| null | Job cgroup sample, read at request time: `{sampledAt, job: {cpuUsageSeconds, memoryBytes, memoryPeakBytes, pids}, tool: {...}}`. A value that is not available (controller not enabled, cgroup gone) is `null`, not 0. |

### 3.6 Dependency object

`{ "up": bool | null, "lastSuccessAt": time | null, "lastErrorAt": time | null, "lastError": string, "errorsTotal": int }`

| Dependency | `up` | Fed by |
|---|---|---|
| `rabbitmq` | `false` once the AMQP connection or channel reported a close (`NotifyClose`), `true` after a successful `Get` | `consumer.Next`, close notifications |
| `core` | `null` (it is not a connection); consumers use the times | every poll (`SinglePoll` and the job poller) and every `Inform` |
| `objectStorage` | `null` | downloads (files, wordlists) and the result upload |

The monitor shows these values. It never contacts any of these services itself (C1).

### 3.7 `counters`

Since process start: `messagesTaken`, `messagesSkipped` (stale/duplicate, unreadable), `jobsStarted`, and
`jobsByOutcome` with keys among `FINISHED, FAILED, CANCELED, SELFTERMINATED, LOSTCONTACT, NOT_STARTED` (a job that
failed before `RUNNING` is counted under its reported state; `NOT_STARTED` = skipped or requeued on shutdown).

Implementation status: the orchestrator does not sample the job's cgroups yet, so `job.resources` is always
`null`.

## 4. No metrics endpoint

There is deliberately no `/metrics` (no Prometheus in this system). Everything the monitor shows comes from
`/status` plus what the worker machine controller reads from Docker; history is kept by the workers control plane.

## 5. Signals and exit codes

Implementation status: only the exit codes are implemented (a lost queue exits with 1). The orchestrator does not
handle the signals yet: `SIGTERM` and `SIGINT` still end it at once (Go's default action, the job is left to
`LOSTCONTACT`), and `SIGUSR1` and `SIGUSR2` are ignored, so a drain request does nothing.

`tini` forwards signals to the orchestrator only. The connector and the tool are never signalled directly by
Docker; the orchestrator deals with them.

| Signal | Effect |
|---|---|
| `SIGUSR1` | **Drain**: `draining=true`. No new message is taken. The current job (if any) continues to its normal end. When idle: phase `DRAINED`, clean exit, code 0. Repeated `SIGUSR1` is a no-op. |
| `SIGUSR2` | **Undrain**: `draining=false` if the process has not started exiting. No-op otherwise. |
| `SIGTERM`, `SIGINT` | **Shut down now**. Idle/drained → exit 0. Preparing (message not acked) → `Nack(requeue=true)`, no report to the core, exit 0. Starting or running a job → terminate the job cgroup (SIGTERM, SIGKILL after 5 s), report `SELFTERMINATED` with message `the worker was shut down` and category `UNKNOWN` (retry policy: 3 attempts, 1 s base, 5 s max, 5 s per request), destroy cgroup, remove work dir, exit 0. Finishing (the connector already sent `finish`; upload or final report in progress) → the step continues (it holds the job's real outcome), then exit 0; Docker's stop timeout bounds it. A second `SIGTERM` during shutdown → exit 1 immediately. |
| `SIGKILL` | Not catchable. The job stays `RUNNING`/`PAUSED` in the core until `LOSTCONTACT`. |

| Exit code | Meaning |
|---|---|
| 0 | Clean exit after drain or a shutdown signal. A restart policy `on-failure` does not restart it. |
| 1 | Fatal error: bad config, lost RabbitMQ connection, a socket could not be created, second SIGTERM. Restarted by `on-failure`. |

Docker's stop timeout for a worker must be ≥ 45 s (5 s terminate + up to ~20 s report + cleanup + margin). The
monitor uses 60 s.

## 6. Container labels

Set by the monitor on containers it creates. A container started by hand (compose) becomes visible to the monitor
when it has at least `automata.worker.id`; it is then shown as **external**.

| Label | Example | Meaning |
|---|---|---|
| `automata.worker.id` | `w-host1-k3f9qa` | Required. Equal to `WORKER_ID`. |
| `automata.worker.managed-by` | `automata-worker-monitor` | Absent on external workers. |
| `automata.worker.template` | `default` | Template name at creation. |
| `automata.worker.network-mode` | `bridge` \| `host` | How the worker machine controller reaches the telemetry. |
| `automata.worker.telemetry-port` | `9464` | Port of `TELEMETRY_ADDR`. Absent → no telemetry (container-level monitoring only). |
| `automata.worker.created-at` | RFC 3339 | |

How the worker machine controller builds the telemetry URL: `host` mode → `http://127.0.0.1:<port>`; bridge → `http://<container IP on
its first network>:<port>` (from `docker inspect`). The worker machine controller runs with host networking, so both are reachable.
