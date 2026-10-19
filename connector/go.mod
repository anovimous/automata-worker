module github.com/anovimous/automata-worker/connector

require github.com/anovimous/automata-worker/sdk/go v0.0.0
require github.com/anovimous/automata-worker/cgroup v0.0.0

require github.com/google/uuid v1.6.0

replace github.com/anovimous/automata-worker/sdk/go => ../sdk/go
replace github.com/anovimous/automata-worker/cgroup => ../cgroup

go 1.26.2
