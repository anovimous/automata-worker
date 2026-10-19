# STAGE 1: BUILDER
FROM golang:1.26 AS builder

WORKDIR /build

# Copy all modules
COPY sdk/ ./sdk/
COPY cgroup/ ./cgroup/
COPY orchestrator/ ./orchestrator/
COPY connector/ ./connector/
# Copy custom tools
COPY --from=masser ./ ./tools/masser

# Build orchestrator
WORKDIR /build/orchestrator
RUN go mod edit -replace github.com/anovimous/cgroup=../cgroup
RUN go build -o /bin/orchestrator ./cmd/orchestrator

# Build connectors
WORKDIR /build/connector
RUN go mod edit -replace github.com/anovimous/automatasdk=../sdk/go
RUN go mod edit -replace github.com/anovimous/cgroup=../cgroup
RUN go build -o /bin/ffuf-connector ./cmd/connectors/ffuf
RUN go build -o /bin/masser-connector ./cmd/connectors/masser

# Build custom tools
WORKDIR /build/tools/masser
RUN go build -o /bin/masser .

# STAGE 2: RUNNER
FROM ubuntu:24.04

# Install external tools
RUN apt-get update && apt-get install -y ffuf tini

# Copy orchestrator to runner
COPY --from=builder /bin/orchestrator /usr/local/bin/orchestrator
# Copy connectors to runner
COPY --from=builder /bin/ffuf-connector /usr/local/bin/ffuf-connector
COPY --from=builder /bin/masser-connector /usr/local/bin/masser-connector
# Copy tools to runner
COPY --from=builder /bin/masser /usr/local/bin/masser


ENTRYPOINT ["/usr/bin/tini", "--", "orchestrator"]