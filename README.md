# Automata Worker

The execution side of **Automata**, a framework for automating security testing of HTTP APIs. The Automata core stores the attack surface and creates, schedules and supervises *jobs*, but never runs a tool. This repo holds the Go code that takes a queued job, runs the tool for it (**ffuf**, **masser**) under resource control, and reports the outcome back to the core.

Status: working end to end on a developer machine, one job at a time per orchestrator process. It has **no authentication** towards the core and is not meant to be exposed to a network yet.

---

## Repositories

| Repo | Role | Stack |
|---|---|---|
| `automata` | Monorepo of the rest of Automata: `core/` (REST API, domain, job engine, scheduler), `ui/` (frontend), `mcp/` (MCP server for the AI agent). Its `docker-compose.yml` runs the core with PostgreSQL, RabbitMQ and RustFS | Spring Boot 3.5 / Java 21, React + TypeScript, Python |
| `automata-worker` (this one) | Orchestrator that consumes queued jobs, plus the tool connectors | Go 1.26 |

Core and worker only talk through a documented contract (`core/docs/worker-contract.md` in the `automata` repo). `contract/samples` here is a copy of the JSON samples the core's tests write, and the tests of this repo parse them with the worker's own types, so a drift on either side breaks a test.

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

Implicit (Java records on one side, Go structs on the other): **change both sides together**, update `core/docs/worker-contract.md` in the `automata` repo, copy the samples (see `contract/README.md`) and fix the failing test.

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

## Deployment

A worker is deployed with Docker Compose from the root of this repo. This describes one worker on one machine; it runs one job at a time.

### Prerequisites

- Linux with **cgroup v2**. The worker creates a cgroup per job under `/sys/fs/cgroup/automata/`, so the container runs `privileged`.
- Docker and Docker Compose.
- A running Automata stack (`docker compose up` in the `automata` repo), with RabbitMQ (`5672`), RustFS (`9000`) and the core (`8080`) reachable from this machine. The stack creates the queue and the `job-configuration`, `job-results` and `wordlists` buckets.

### 1. Configure

```bash
cp .env.example orchestrator/.env      # never commit .env
```

Fill it in. The values below are for a worker on the same machine as the stack; the passwords are the ones in the `automata` repo's `.env`, and the user is `automata` for both RabbitMQ and RustFS.

| Variable | Value |
|---|---|
| `RABBITMQ_URL` | `amqp://automata:<RABBITMQ_PASSWORD>@localhost:5672/` |
| `RABBITMQ_QUEUE` | `jobs.http.queue` |
| `RUSTFS_ENDPOINT` | `localhost:9000` (host and port, no scheme; plain HTTP) |
| `RUSTFS_ACCESS_KEY`, `RUSTFS_SECRET_KEY` | `automata`, `<RUSTFS_PASSWORD>` |
| `RUSTFS_JOBS_BUCKET`, `RUSTFS_RESULTS_BUCKET` | `job-configuration`, `job-results` |
| `CENTRAL_BASE_URL` | `http://localhost:8080` |
| `CENTRAL_POLL_INTERVAL_SECONDS` | `5` |
| `CENTRAL_STATE_ENDPOINT`, `CENTRAL_OUTCOME_ENDPOINT` | `/api/external/jobs/http/poll`, `/api/external/jobs/http/update` |
| `WORKER_HEARTBEAT_TIMEOUT_SECONDS`, `WORKER_HEARTBEAT_INTERVAL_SECONDS` | `30`, `10` (the interval must be well below the timeout) |
| `WORKER_SOCKET_DIR`, `TEMP_DIR` | `/var/tmp` |

Missing numeric variables silently become 0, so leave none empty. The wordlist variables (`RUSTFS_WORDLISTS_BUCKET=wordlists`, `WORDLIST_CACHE_DIR`, an absolute path, and `WORDLIST_CACHE_MAX_BYTES`, default 1.5 GB) are set by `docker-compose.yml`; a wrong value stops the start.

### 2. Run with Docker Compose

```bash
docker compose build                 # orchestrator, both connectors, ffuf and masser in one image
docker compose up -d
docker compose logs -f orchestrator
docker compose down                  # stop the worker
```

The container runs `privileged` (cgroups) with host networking (so `localhost` reaches the stack), mounts `/var/tmp` for the job files and sockets, and keeps the wordlist cache in the `wordlist_cache` volume across restarts.

### 3. Check

Queue a `fuzz` job from the UI. The worker log shows it being taken, and the job goes `QUEUED` → `RUNNING` → `FINISHED`, with its result in the `job-results` bucket.

---

## Development

```bash
go vet ./... && go test ./...        # inside each module directory; there is no root module
```

---

## Known limits

- No authentication towards the core's worker API, and some ffuf flags are still dangerous; see the review in the `automata` repo (`core/.claude/components-code-review.md`).
- The ffuf connector takes its target only from `rawRequestBase64` and rejects a job with more than one wordlist.
- A user cancel during the wordlist download is only seen by the first poll after the start.
