# Automata Worker

The execution side of **Automata**, a framework for automating security testing of HTTP APIs. The Automata core stores the attack surface and creates, schedules and supervises *jobs*, but never runs a tool. This repo holds the Go code that takes a queued job, runs the tool for it (**ffuf**, **masser**) under resource control, and reports the outcome back to the core.

Status: working end to end on a developer machine, one job at a time per orchestrator process. It has **no authentication** towards the core and is not meant to be exposed to a network yet.

---

## Repositories

| Repo | Role | Stack |
|---|---|---|
| `automata` (`code/automata`) | Core service: REST API, domain, job engine, scheduler | Spring Boot 3.5, Java 21 |
| `automata-worker` (this one) | Orchestrator that consumes queued jobs, plus the tool connectors | Go 1.26 |
| `automata-frontend`, `automata-mcp`, Burp extension | UI, AI agent gateway, traffic ingestion | see the core README |

Core and worker only talk through a documented contract (`docs/worker-contract.md` in the core repo). `contract/samples` here is a copy of the JSON samples the core's tests write, and the tests of this repo parse them with the worker's own types, so a drift on either side breaks a test.

---

## Architecture

```
 user / UI ──► Automata core ──publish──► RabbitMQ ──► orchestrator ──spawns──► connector ──► tool (ffuf / masser)
                │    ▲                                   │  ▲                       │
                │    └── HTTP poll / update ─────────────┘  └── unix sockets ───────┘
                └── details + data files ──► RustFS (S3) ◄── result file ───────────┘
```

| Dir | Module | What it is |
|---|---|---|
| `orchestrator` | `.../orchestrator` | Long-running daemon: consumes jobs, downloads job files and wordlists, starts a connector, relays state between core and connector, uploads the result |
| `connector` | `.../connector` | One small binary per tool (`cmd/connectors/ffuf`, `masser`). `internal/parser` reads the details file, `internal/runner` runs the tool in a cgroup and handles pause/cancel |
| `sdk/go` | `.../sdk/go` | Library every connector uses (`sdk.New`, `IsCancelled`, `IsPaused`, `UpdateState`, `Finish`, `Fail`): talks to the orchestrator over two unix sockets and sends heartbeats; runs standalone without socket flags |
| `cgroup` | `.../cgroup` | cgroup v2 helper (create, add pid, signal, terminate, kill, destroy) |
| `contract` | | Samples of the core/worker payloads, checked by tests |

- **RabbitMQ**: the orchestrator reads `jobs.http.queue` for `{jobId, routineKey}` messages, competing with every other orchestrator.
- **RustFS (S3)**: reads `details/{id}.json` and `data/{id}.jsonl` from the jobs bucket, wordlists from the `wordlists` bucket, writes `{id}.result` to the results bucket.
- **Core worker API**: polls the requested state (pause, resume, cancel) and reports every state change.
- **Isolation**: each job runs in its own cgroup `automata/job-<id>`, the tool in a nested `tool` cgroup that can be frozen, signalled or killed as a unit.

---

## How a job runs

| Step | What happens |
|---|---|
| 1. Take | Takes one message from the queue, only when idle. Downloads the details and data files into `TEMP_DIR`; failure reports `FAILED`. |
| 2. Check | Polls the core once: only a job still `QUEUED` starts. `CANCELED` is reported back; anything else is a stale or duplicate message and is skipped silently. |
| 3. Wordlists | Each wordlist of the details file comes from the local cache or is downloaded (verified against sha256 and size, 3 attempts, 20 min limit). The orchestrator's copy of the details file is rewritten with local `wordlistsPaths`. Failure reports `FAILED` / `PREPARATION`. |
| 4. Start | Maps the `routineKey` to a connector (`fuzz` → `ffuf-connector`, `masser` → `masser-connector`), creates the cgroup, starts the binary and reports `RUNNING`. |
| 5. Connect | The connector must connect to both unix sockets (`/read`, `/write` in `WORKER_SOCKET_DIR`) within 30 s. Messages are JSON lines: `heartbeat`, `stateUpdate`, `finish` from the connector; `{state}` back. |
| 6. Supervise | Core poll answers are forwarded to the connector. `LOSTCONTACT` terminates the tool without a report. No message within `WORKER_HEARTBEAT_TIMEOUT_SECONDS` terminates the cgroup and reports `SELFTERMINATED`. |
| 7. Finish | On `finish` the result file is uploaded (only for `FINISHED`, with retries) and the final state reported; a missing file or failed upload is `FAILED`. Reports are retried on network errors, timeouts and 5xx. |
| 8. Clean up | Cancels the context, closes the sockets, destroys the cgroup. |

Inside a connector, the runner checks the SDK state every 500 ms: **cancel** → SIGTERM then kill, **pause** → SIGSTOP the tool cgroup and report `PAUSED`, **resume** → SIGCONT and report `RUNNING`. Tool exit 0 → `Finish(outputPath)`, otherwise `Fail(category, message)`.

### Distributed by design

- **Competing consumers**: start as many orchestrators as you need, on any machine that can reach the queue, storage and core. Each handles one job at a time.
- **Nothing shared**: orchestrators never talk to each other. Each keeps its own wordlist cache (`WORDLIST_CACHE_DIR/<sha256>`, least recently used removed above `WORDLIST_CACHE_MAX_BYTES`, files of the running job never removed). Use one cache directory per process.
- **Failures are visible**: a job ends as `FAILED` with a category (`PREPARATION`, `POLL`, `INVALID_CONFIG`, `TOOL_START`, `TOOL_EXIT`, `RESULT`, `UNKNOWN`) and a message, or `SELFTERMINATED`; if the worker dies, the core marks the job `LOSTCONTACT` after 30 minutes of silence.

---

## Contract with the core

Implicit (Java records on one side, Go structs on the other): **change both sides together**, update `docs/worker-contract.md` in the core, copy the samples (see `contract/README.md`) and fix the failing test.

- **Queue**: `jobs.http.queue` (`x-max-priority` 2), routing key `jobs.http.ready` on `automataDirectExchange`; message `{jobId, routineKey}`.
- **Objects**: `details/{id}.json` (`jobId, routineKey, rate, verbosity, auth, targetType, genericConfig, customConfig, wordlists`), `data/{id}.jsonl` (one request or host per line), `{id}.result`, `wordlists/{wordlistId}`. The `auth` block holds the tenant's credentials, and the connectors never see `wordlists`.
- **HTTP**: `GET /api/external/jobs/http/poll/{jobId}` → `{requestedState}`; `POST /api/external/jobs/http/update/{jobId}` with `{jobId, newState, message, failureCategory}`.
- **States**: `QUEUED, RUNNING, PAUSED, CANCELED, FINISHED, FAILED, SELFTERMINATED, LOSTCONTACT`. The core's transition table decides what a worker may report; pause, cancel and resume are accepted only when a user requested them.
- **Routine keys**: the core's `Routine.key` must exist in `orchestrator/internal/mapper/routine-worker-mapping.json`, or the job fails with "failed to find mapping for routineKey".
- **`customConfig`**: ffuf `{rawRequestBase64, flags:[{key,value}]}`, masser `{flags}`; flag keys are checked against an allow-list in each connector's `builder.go`.

---

## Adding a connector for a new tool

1. `connector/cmd/connectors/<tool>/` with a `main.go` (`sdk.New` → `parser.Parse` → `BuildCommand` → `runner.Run`, failing via `s.Fail`) and a `builder.go` (details → command line, output path under `tmpDir`, flag allow-list).
2. Add the build line and the binary to the `Dockerfile`.
3. Add the `key` → binary entry to `routine-worker-mapping.json`.
4. Create the matching `Routine` (same `key`) in the core.

---

## Getting started

Prerequisites: Go 1.26, Linux with cgroup v2 and permission to create cgroups under `/sys/fs/cgroup/automata/`, `ffuf` and `masser` on `PATH`, and the core's RabbitMQ and RustFS reachable.

```bash
cp .env.example orchestrator/.env          # then fill it in; never commit .env
cd orchestrator && go build -o /dev/null ./cmd/orchestrator
cd connector    && go build ./cmd/connectors/ffuf ./cmd/connectors/masser
go vet ./... && go test ./...              # inside each module directory
docker compose build && docker compose up -d
```

The container runs `privileged` with host networking so it can manage cgroups. The compose file sets the wordlist variables; when running outside Docker, set them yourself.

| Variable | Purpose |
|---|---|
| `RABBITMQ_URL`, `RABBITMQ_QUEUE` | Queue to consume (`jobs.http.queue`) |
| `RUSTFS_ENDPOINT`, `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY` | Object storage |
| `RUSTFS_JOBS_BUCKET`, `RUSTFS_RESULTS_BUCKET`, `RUSTFS_WORDLISTS_BUCKET` | `job-configuration`, `job-results`, `wordlists` |
| `WORDLIST_CACHE_DIR` (absolute), `WORDLIST_CACHE_MAX_BYTES` | Wordlist cache, default 1.5 GB; a wrong value stops the start |
| `CENTRAL_BASE_URL`, `CENTRAL_POLL_INTERVAL_SECONDS`, `CENTRAL_STATE_ENDPOINT`, `CENTRAL_OUTCOME_ENDPOINT` | Core worker API |
| `WORKER_HEARTBEAT_TIMEOUT_SECONDS`, `WORKER_HEARTBEAT_INTERVAL_SECONDS`, `WORKER_SOCKET_DIR`, `TEMP_DIR` | Job supervision and scratch space |

Missing numeric variables silently become 0.

---

## Known limits

- The `masser` build context in `docker-compose.yml` is an external path, so a fresh clone cannot build the image yet. Once masser is published and tagged, it will be installed with a pinned `go install ...@<tag>` instead.
- No authentication towards the core's worker API, and some ffuf flags are still dangerous; see the review in the core repo (`.claude/components-code-review.md`).
- The ffuf connector takes its target only from `rawRequestBase64` and rejects a job with more than one wordlist.
- A user cancel during the wordlist download is only seen by the first poll after the start.
