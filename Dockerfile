# STAGE 1: BUILDER
FROM golang:1.26 AS builder

# what the orchestrator reports as its version and commit (internal/buildinfo)
ARG VERSION=dev
ARG COMMIT=unknown

WORKDIR /build

# Copy the workspace file and all modules
COPY go.work ./
COPY sdk/ ./sdk/
COPY cgroup/ ./cgroup/
COPY orchestrator/ ./orchestrator/
COPY connector/ ./connector/
# Copy custom tools
COPY --from=masser ./ ./tools/masser

# Build orchestrator
WORKDIR /build/orchestrator
RUN go build -ldflags "-X github.com/anovimous/automata-worker/orchestrator/internal/buildinfo.Version=${VERSION} \
  -X github.com/anovimous/automata-worker/orchestrator/internal/buildinfo.Commit=${COMMIT}" \
  -o /bin/orchestrator ./cmd/orchestrator

# Build connectors
WORKDIR /build/connector
RUN go build -o /bin/ffuf-connector ./cmd/connectors/ffuf
RUN go build -o /bin/masser-connector ./cmd/connectors/masser

# Build custom tools (not part of the workspace)
WORKDIR /build/tools/masser
RUN GOWORK=off go build -o /bin/masser .

# STAGE 2: RUNNER
FROM ubuntu:24.04

ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.source=https://github.com/anovimous/automata-worker

# Install external tools
RUN apt-get update && apt-get install -y ffuf tini

# Copy orchestrator to runner
COPY --from=builder /bin/orchestrator /usr/local/bin/orchestrator
# Copy connectors to runner
COPY --from=builder /bin/ffuf-connector /usr/local/bin/ffuf-connector
COPY --from=builder /bin/masser-connector /usr/local/bin/masser-connector
# Copy tools to runner
COPY --from=builder /bin/masser /usr/local/bin/masser

# GET /healthz of the telemetry server (TELEMETRY_ADDR); Docker runs it without the entrypoint
HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=3 CMD ["/usr/local/bin/orchestrator", "healthcheck"]

ENTRYPOINT ["/usr/bin/tini", "--", "orchestrator"]