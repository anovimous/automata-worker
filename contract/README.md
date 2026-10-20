# Contract samples

`samples/` is a copy of `src/test/resources/contract` of the core repo (`/home/haidar/dev/automata/code/automata`). The core's tests write these files with its own code; the tests of this repo parse them with the worker's own types (`connector/internal/parser`, both connector builders, `orchestrator/internal/queue`, `central/poller`, `central/informer`, `job`).

The contract itself, including the state tables, is `docs/worker-contract.md` in the core repo. When a payload changes, change it in the core, copy the samples here (`cp -r <core>/src/test/resources/contract/. contract/samples/`) and fix whatever test fails.
