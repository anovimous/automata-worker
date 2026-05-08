module github.com/anovimous/sdkconnector

require github.com/anovimous/automatasdk v0.0.0
require github.com/anovimous/cgroup v0.0.0

require github.com/google/uuid v1.6.0

replace github.com/anovimous/automatasdk => /home/haidar/dev/automata-components/sdk/go
replace github.com/anovimous/cgroup => /home/haidar/dev/automata-components/cgroup

go 1.26.2
