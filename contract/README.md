# Contract samples

`samples/` is a copy of `src/test/resources/contract` of the core repo (`/home/haidar/dev/automata/code/automata`), except `details-file-resolved.json`, which is the worker's own. The core's tests write the copied files with its own code; the tests of this repo parse them with the worker's own types (`connector/internal/parser`, both connector builders, `orchestrator/internal/queue`, `orchestrator/internal/wordlist`, `central/poller`, `central/informer`, `job`).

The contract itself, including the state tables, is `docs/worker-contract.md` in the core repo. When a payload changes, change it in the core, copy the samples here (`cp -r <core>/src/test/resources/contract/. contract/samples/`) and fix whatever test fails.

`details-file-resolved.json` is `details-file.json` as the connectors get it: the orchestrator replaces `wordlists` (what the core writes) by `wordlistsPaths` (the local paths of the cached files). A test of `orchestrator/internal/wordlist` checks that rewriting `details-file.json` gives exactly this file, and the parser and connector tests read it. When `details-file.json` changes, change this file with it.

`telemetry/` belongs to another contract, the one between a worker and the monitor (`automata-worker-monitor`), written in `docs/telemetry-contract.md`. Unlike `samples/`, these files are the worker's own: `orchestrator/internal/telemetry/contract_test.go` writes the status types and compares them with them, and the monitor keeps an equal copy in its `contracts/` (check with `diff -r`). A change to the status document changes this directory, the doc and the monitor's copy together.
